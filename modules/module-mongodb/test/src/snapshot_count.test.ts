import { estimateFilteredSnapshotCount } from '@module/replication/estimateFilteredSnapshotCount.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { createLogger } from '@powersync/lib-services-framework';
import { afterEach, describe, expect, test, vi } from 'vitest';

const FILTER = { $expr: { $eq: ['$label', 'live'] } };
const LOGGER = createLogger({ silent: true });

function fixture() {
  const client = new mongo.MongoClient('mongodb://localhost:27017');
  const collection = client.db('snapshot_count_unit').collection('documents');
  const controller = new AbortController();
  const count = vi.spyOn(collection, 'countDocuments').mockResolvedValue(42);
  const metadata = vi.spyOn(collection, 'estimatedDocumentCount').mockResolvedValue(100_000);
  const cursor = collection.aggregate();
  const next = vi.spyOn(cursor, 'next').mockResolvedValue({ sampled: [{ count: 1_000 }], matches: [{ count: 250 }] });
  const close = vi.spyOn(cursor, 'close').mockResolvedValue();
  const aggregate = vi.spyOn(collection, 'aggregate').mockReturnValue(cursor);
  const estimate = () =>
    estimateFilteredSnapshotCount({ collection, filter: FILTER, signal: controller.signal, logger: LOGGER });
  return { controller, count, metadata, next, close, aggregate, estimate };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('filtered snapshot count', () => {
  test('uses a bounded exact count with the snapshot collation', async () => {
    const context = fixture();
    expect(await context.estimate()).toBe(42);
    expect(context.count).toHaveBeenCalledWith(FILTER, {
      collation: { locale: 'simple' },
      readConcern: 'majority',
      timeoutMS: 10_000,
      signal: context.controller.signal
    });
    expect(context.metadata).not.toHaveBeenCalled();
    expect(context.aggregate).not.toHaveBeenCalled();
  });

  test('preserves an exact zero without sampling', async () => {
    const context = fixture();
    context.count.mockResolvedValue(0);
    expect(await context.estimate()).toBe(0);
    expect(context.metadata).not.toHaveBeenCalled();
  });

  test('samples before filtering and scales by collection metadata after a count timeout', async () => {
    const context = fixture();
    context.count.mockRejectedValue(new mongo.MongoOperationTimeoutError('count timed out'));
    expect(await context.estimate()).toBe(25_000);
    expect(context.aggregate).toHaveBeenCalledWith(
      [
        { $sample: { size: 1_000 } },
        { $facet: { sampled: [{ $count: 'count' }], matches: [{ $match: FILTER }, { $count: 'count' }] } }
      ],
      expect.objectContaining({
        collation: { locale: 'simple' },
        readConcern: 'majority',
        signal: context.controller.signal,
        allowDiskUse: false
      })
    );
    const timeoutMS = context.aggregate.mock.calls[0][1]!.timeoutMS!;
    expect(timeoutMS).toBeGreaterThan(0);
    expect(timeoutMS).toBeLessThanOrEqual(10_000);
    expect(context.close).toHaveBeenCalled();
  });

  test.each([
    [10, 1],
    [100_000_000, 10_000]
  ])('bounds the sample size for a collection of %s documents', async (total, sampleSize) => {
    const context = fixture();
    context.count.mockRejectedValue(new Error('count failed'));
    context.metadata.mockResolvedValue(total);
    await context.estimate();
    expect(context.aggregate.mock.calls[0][0]![0]).toEqual({ $sample: { size: sampleSize } });
  });

  test('uses the actual number sampled when metadata or collection size has changed', async () => {
    const context = fixture();
    context.count.mockRejectedValue(new Error('count failed'));
    context.next.mockResolvedValue({ sampled: [{ count: 80 }], matches: [{ count: 20 }] });
    expect(await context.estimate()).toBe(25_000);
  });

  test.each([null, { sampled: [], matches: [] }, { sampled: [{ count: 1_000 }], matches: [] }])(
    'leaves the total unknown for an empty sample or no matches: %j',
    async (sample) => {
      const context = fixture();
      context.count.mockRejectedValue(new Error('count failed'));
      context.next.mockResolvedValue(sample);
      expect(await context.estimate()).toBe(-1);
      expect(context.close).toHaveBeenCalled();
    }
  );

  test.each(['metadata', 'sample', 'empty metadata', 'budget exhausted'])(
    'leaves the total unknown after %s',
    async (failure) => {
      const context = fixture();
      context.count.mockRejectedValue(new Error('count failed'));
      if (failure == 'metadata') {
        context.metadata.mockRejectedValue(new Error('metadata failed'));
      } else if (failure == 'sample') {
        context.next.mockRejectedValue(new mongo.MongoOperationTimeoutError('sample timed out'));
      } else if (failure == 'empty metadata') {
        context.metadata.mockResolvedValue(0);
      } else {
        const { performance } = await import('node:perf_hooks');
        vi.spyOn(performance, 'now').mockReturnValueOnce(0).mockReturnValue(10_001);
      }
      expect(await context.estimate()).toBe(-1);
      if (failure == 'sample') {
        expect(context.close).toHaveBeenCalled();
      } else {
        expect(context.aggregate).not.toHaveBeenCalled();
      }
    }
  );

  test.each(['before count', 'during count', 'during sample'])('propagates cancellation %s', async (when) => {
    const context = fixture();
    const reason = new Error('replication aborted');
    if (when == 'before count') {
      context.controller.abort(reason);
    } else if (when == 'during count') {
      context.count.mockImplementation(async () => {
        context.controller.abort(reason);
        throw reason;
      });
    } else {
      context.count.mockRejectedValue(new Error('count failed'));
      context.next.mockImplementation(async () => {
        context.controller.abort(reason);
        throw reason;
      });
    }
    await expect(context.estimate()).rejects.toBe(reason);
    if (when != 'during sample') {
      expect(context.metadata).not.toHaveBeenCalled();
    }
  });
});
