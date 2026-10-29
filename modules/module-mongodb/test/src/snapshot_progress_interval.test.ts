import { MongoSnapshotter } from '@module/replication/MongoSnapshotter.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { performance } from 'node:perf_hooks';
import { expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const CONFIG = `
config:
  edition: 3
streams:
  items:
    query: SELECT _id AS id, description FROM test_data
`;

async function open(snapshotProgressIntervalMs?: number) {
  const context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, { storageVersion: 4 });
  await context.updateSyncRules(CONFIG);
  await context.db
    .collection<{ _id: string; description: string }>('test_data')
    .insertMany(['A', 'B', 'C', 'D'].map((id) => ({ _id: id, description: id })));
  const abort = new AbortController();
  const snapshotter = new MongoSnapshotter({
    storage: context.storage!,
    connections: context.connectionManager,
    metrics: METRICS_HELPER.metricsEngine,
    abortSignal: abort.signal,
    checkpointStreamId: new mongo.ObjectId(),
    snapshotChunkLength: 1,
    snapshotProgressIntervalMs
  });
  return { context, abort, snapshotter };
}

test.skipIf(!env.TEST_MONGO_STORAGE).each([
  { interval: undefined, times: [0, 9999, 10000, 10001], counts: [3, 4] },
  { interval: 50, times: [0, 49, 50, 51], counts: [3, 4] },
  { interval: 0, times: [0, 0, 0, 0], counts: [1, 2, 3, 4] },
  { interval: 60000, times: [0, 1, 2, 3], counts: [4] }
])('snapshot progress interval $interval', async ({ interval, times, counts }) => {
  const c = await open(interval);
  await using context = c.context;
  let now = 0;
  using _clock = vi.spyOn(performance, 'now').mockImplementation(() => now);
  const observed: number[] = [];
  const order: string[] = [];
  let rows = 0;
  const create = context.storage!.createWriter.bind(context.storage!);
  using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
    const writer = await create(...args);
    const save = writer.save.bind(writer);
    vi.spyOn(writer, 'save').mockImplementation(async (record) => {
      const result = await save(record);
      now = times[rows++];
      return result;
    });
    const flush = writer.flush.bind(writer);
    vi.spyOn(writer, 'flush').mockImplementation(async (...args) => {
      const result = await flush(...args);
      order.push('flush');
      return result;
    });
    const update = writer.updateTableProgress.bind(writer);
    vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (table, status) => {
      if (status.replicatedCount != null) {
        expect(order.at(-1)).toBe('flush');
        expect(bson.deserialize(status.lastKey!)._id).toBe(['A', 'B', 'C', 'D'][status.replicatedCount - 1]);
        observed.push(status.replicatedCount);
        order.push('progress');
      }
      return update(table, status);
    });
    return writer;
  });
  await c.snapshotter.setupCheckpointsCollection();
  await c.snapshotter.queueSnapshotTables(null);
  const running = c.snapshotter.replicationLoop().catch((error: unknown) => error);
  try {
    await c.snapshotter.waitForInitialSnapshot();
    expect(rows).toBe(4);
    expect(observed).toEqual(counts);
  } finally {
    c.abort.abort();
    await running;
  }
});

test.skipIf(!env.TEST_MONGO_STORAGE)('graceful abort persists the last complete page before the interval', async () => {
  const c = await open(60000);
  await using context = c.context;
  let rows = 0;
  let progressTable: storage.SourceTable | undefined;
  const create = context.storage!.createWriter.bind(context.storage!);
  using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
    const writer = await create(...args);
    const save = writer.save.bind(writer);
    vi.spyOn(writer, 'save').mockImplementation(async (record) => {
      const result = await save(record);
      if (++rows === 2) {
        c.abort.abort();
      }
      return result;
    });
    const update = writer.updateTableProgress.bind(writer);
    vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (...args) => {
      const result = await update(...args);
      if (args[1].replicatedCount != null) {
        progressTable = result;
      }
      return result;
    });
    return writer;
  });
  await c.snapshotter.setupCheckpointsCollection();
  await c.snapshotter.queueSnapshotTables(null);
  await expect(c.snapshotter.replicationLoop()).rejects.toThrow('Aborted initial replication');
  expect(rows).toBe(2);
  expect(progressTable!.snapshotStatus!.replicatedCount).toBe(2);
  expect(progressTable!.snapshotComplete).toBe(false);
});

test.skipIf(!env.TEST_MONGO_STORAGE)('a failed flush never persists the pending cursor', async () => {
  const c = await open(0);
  await using context = c.context;
  const observed: number[] = [];
  const create = context.storage!.createWriter.bind(context.storage!);
  using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
    const writer = await create(...args);
    vi.spyOn(writer, 'flush').mockRejectedValueOnce(new Error('failed snapshot flush'));
    const update = writer.updateTableProgress.bind(writer);
    vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (...args) => {
      if (args[1].replicatedCount != null) {
        observed.push(args[1].replicatedCount);
      }
      return update(...args);
    });
    return writer;
  });
  await c.snapshotter.setupCheckpointsCollection();
  await c.snapshotter.queueSnapshotTables(null);
  await expect(c.snapshotter.replicationLoop()).rejects.toThrow('failed snapshot flush');
  expect(observed).toEqual([]);
});
