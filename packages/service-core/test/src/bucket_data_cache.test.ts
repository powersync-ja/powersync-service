import { BucketDataCache, FetchData, FetchBucketDataArgs } from '@/storage/BucketDataCache.js';
import { SyncBucketDataChunk } from '@/storage/SyncRulesBucketStorage.js';
import { InternalOpId } from '@/util/util-index.js';
import { chunk } from 'lodash';
import { describe, expect, it, vi } from 'vitest';

function createTestChunk(bucket: string, after: InternalOpId, hasMore: boolean = false): SyncBucketDataChunk {
  return {
    chunkData: {
      bucket,
      data: [],
      has_more: hasMore,
      after: after.toString(),
      next_after: (after + 1n).toString()
    },
    targetOp: after + 1n
  };
}

function createMockFetchData(chunks: SyncBucketDataChunk[]): FetchData {
  return vi.fn(async (batch: FetchBucketDataArgs[]) => {
    let results = new Map<string, SyncBucketDataChunk>();
    for (let chunk of chunks) {
      results.set(`${chunk.chunkData.bucket}:${chunk.chunkData.after}`, chunk);
    }
    return batch.map((request) => {
      const key = `${request.bucket}:${request.start}`;
      return (
        results.get(key) ?? {
          chunkData: {
            bucket: request.bucket,
            data: [],
            has_more: false,
            after: request.start.toString(),
            next_after: (request.end + 1n).toString()
          },
          targetOp: null
        }
      );
    });
  });
}

describe('BucketDataCache', () => {
  it('should fetch data from one bucket when not cached', async () => {
    const mockChunk = createTestChunk('bucket1', 100n);
    const fetchData = createMockFetchData([mockChunk]);
    const cache = new BucketDataCache({ fetchData });

    const dataBuckets = new Map([['bucket1', 100n]]);
    const results = [];

    for await (const chunk of cache.getBucketData(200n, dataBuckets)) {
      results.push(chunk);
    }

    expect(results).toEqual([mockChunk]);
    expect(fetchData).toHaveBeenCalledOnce();
    expect(fetchData).toHaveBeenCalledWith([{ bucket: 'bucket1', start: 100n, end: 200n }]);
  });

  it('should get multiple chunks from one bucket when not cached', async () => {
    const chunks = [
      {
        chunkData: {
          bucket: 'bucket1',
          data: [],
          has_more: true,
          after: '100',
          next_after: '200'
        },
        targetOp: null
      },
      {
        chunkData: {
          bucket: 'bucket1',
          data: [],
          has_more: false,
          after: '200',
          next_after: '300'
        },
        targetOp: null
      }
    ];
    const fetchData = createMockFetchData(chunks);
    const cache = new BucketDataCache({ fetchData });

    const dataBuckets = new Map([['bucket1', 100n]]);
    const results = [];

    for await (const chunk of cache.getBucketData(300n, dataBuckets)) {
      results.push(chunk);
    }

    expect(results).toEqual(chunks);
  });

  it('should deduplicate concurrent requests', async () => {
    let resolvePromise: (() => void) | null = null;
    const fetchPromise = new Promise<void>((resolve) => {
      resolvePromise = resolve;
    });

    const mockChunk = createTestChunk('bucket1', 100n);
    const fetchData = vi.fn(async (batch: FetchBucketDataArgs[]) => {
      await fetchPromise;
      return batch.map(() => mockChunk);
    });

    const cache = new BucketDataCache({ fetchData });

    const dataBuckets1 = new Map([['bucket1', 100n]]);
    const dataBuckets2 = new Map([['bucket1', 100n]]);

    const iterator1 = cache.getBucketData(200n, dataBuckets1);
    const iterator2 = cache.getBucketData(200n, dataBuckets2);

    const promise1 = iterator1.next();
    const promise2 = iterator2.next();

    resolvePromise!();

    const [result1, result2] = await Promise.all([promise1, promise2]);

    expect(result1.value).toEqual(mockChunk);
    expect(result2.value).toEqual(mockChunk);
    expect(fetchData).toHaveBeenCalledOnce();
  });

  it('should use cached data for subsequent requests', async () => {
    const mockChunk = createTestChunk('bucket1', 100n);
    const fetchData = createMockFetchData([mockChunk]);
    const cache = new BucketDataCache({ fetchData });

    const dataBuckets1 = new Map([['bucket1', 100n]]);
    const results1 = [];
    for await (const chunk of cache.getBucketData(200n, dataBuckets1)) {
      results1.push(chunk);
    }

    const dataBuckets2 = new Map([['bucket1', 100n]]);
    const results2 = [];
    for await (const chunk of cache.getBucketData(200n, dataBuckets2)) {
      results2.push(chunk);
    }

    expect(results1).toEqual([mockChunk]);
    expect(results2).toEqual([mockChunk]);
    expect(fetchData).toHaveBeenCalledOnce();
  });

  it('should handle multiple buckets in single request', async () => {
    const chunk1 = createTestChunk('bucket1', 100n);
    const chunk2 = createTestChunk('bucket2', 150n);
    const fetchData = createMockFetchData([chunk1, chunk2]);
    const cache = new BucketDataCache({ fetchData });

    const dataBuckets = new Map([
      ['bucket1', 100n],
      ['bucket2', 150n]
    ]);
    const results = [];

    for await (const chunk of cache.getBucketData(200n, dataBuckets)) {
      results.push(chunk);
    }

    expect(results).toHaveLength(2);
    expect(results).toContainEqual(chunk1);
    expect(results).toContainEqual(chunk2);
    expect(fetchData).toHaveBeenCalledOnce();
  });

  it('should handle bucket with has_more=true', async () => {
    const chunk1 = createTestChunk('bucket1', 100n, true);
    const chunk2 = createTestChunk('bucket1', 101n, false);

    const fetchData = vi.fn(async (batch: FetchBucketDataArgs[]) => {
      const request = batch[0];
      if (request.start === 100n) {
        return [chunk1];
      } else if (request.start === 101n) {
        return [chunk2];
      }
      return [];
    });

    const cache = new BucketDataCache({ fetchData });
    const dataBuckets = new Map([['bucket1', 100n]]);
    const results = [];

    for await (const chunk of cache.getBucketData(200n, dataBuckets)) {
      results.push(chunk);
    }

    expect(results).toEqual([chunk1, chunk2]);
    expect(fetchData).toHaveBeenCalledTimes(2);
    expect(fetchData).toHaveBeenNthCalledWith(1, [{ bucket: 'bucket1', start: 100n, end: 200n }]);
    expect(fetchData).toHaveBeenNthCalledWith(2, [{ bucket: 'bucket1', start: 101n, end: 200n }]);
  });

  it('should handle empty bucket list', async () => {
    const fetchData = vi.fn();
    const cache = new BucketDataCache({ fetchData });

    const dataBuckets = new Map<string, InternalOpId>();
    const results = [];

    for await (const chunk of cache.getBucketData(200n, dataBuckets)) {
      results.push(chunk);
    }

    expect(results).toEqual([]);
    expect(fetchData).not.toHaveBeenCalled();
  });

  it.skip('should respect custom maxSize option', async () => {
    const fetchData = vi.fn(async (batch: FetchBucketDataArgs[]) => {
      return batch.map((request) => createTestChunk(request.bucket, request.start));
    });

    const cache = new BucketDataCache({ fetchData, maxSize: 1 });

    const dataBuckets1 = new Map([['bucket1', 100n]]);
    const results1 = [];
    for await (const chunk of cache.getBucketData(200n, dataBuckets1)) {
      results1.push(chunk);
    }

    const dataBuckets2 = new Map([['bucket2', 150n]]);
    const results2 = [];
    for await (const chunk of cache.getBucketData(200n, dataBuckets2)) {
      results2.push(chunk);
    }

    const dataBuckets3 = new Map([['bucket1', 100n]]);
    const results3 = [];
    for await (const chunk of cache.getBucketData(200n, dataBuckets3)) {
      results3.push(chunk);
    }

    expect(results1).toHaveLength(1);
    expect(results2).toHaveLength(1);
    expect(results3).toHaveLength(1);
    expect(fetchData).toHaveBeenCalledTimes(3);
  });

  it('should handle fetch errors gracefully', async () => {
    const fetchData = vi.fn(async () => {
      throw new Error('Fetch failed');
    });

    const cache = new BucketDataCache({ fetchData });
    const dataBuckets = new Map([['bucket1', 100n]]);

    await expect(async () => {
      for await (const chunk of cache.getBucketData(200n, dataBuckets)) {
        // Should not reach here
      }
    }).rejects.toThrow('Fetch failed');
  });
});
