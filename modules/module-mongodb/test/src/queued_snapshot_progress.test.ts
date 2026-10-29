import { MongoSnapshotter } from '@module/replication/MongoSnapshotter.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import { setTimeout as delay } from 'node:timers/promises';
import { expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

test.skipIf(!env.TEST_MONGO_STORAGE)(
  'snapshot admits a later page before earlier receipt delivery',
  async () => {
    METRICS_HELPER.resetMetrics();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const secondPage = Promise.withResolvers<void>();
    let flushed = 0;
    await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion: 4
    });
    await context.updateSyncRules(`
config:
  edition: 3
streams:
  items:
    query: SELECT _id AS id, description FROM test_data
`);
    await context.db.collection<{ _id: string; description: string }>('test_data').insertMany([
      { _id: 'A', description: 'first' },
      { _id: 'B', description: 'second' },
      { _id: 'C', description: 'third' }
    ]);
    const storage = context.storage!;
    const create = storage.createWriter.bind(storage);
    using _create = vi.spyOn(storage, 'createWriter').mockImplementation(async (...args) => {
      const writer = await create(...args);
      const queue = writer.queueTableProgress;
      if (queue != null) {
        vi.spyOn(writer, 'queueTableProgress').mockImplementation(async (table, progress) => {
          const receipt = await queue.call(writer, table, progress);
          if (progress.replicatedCount === 2) {
            secondPage.resolve();
          }
          return receipt;
        });
      }
      return writer;
    });
    const abort = new AbortController();
    // Exercise the snapshotter directly: concurrent streaming metadata barriers
    // deliberately drain storage and would obscure page-overlap scheduling.
    const snapshotter = new MongoSnapshotter({
      storage,
      connections: context.connectionManager,
      metrics: METRICS_HELPER.metricsEngine,
      abortSignal: abort.signal,
      checkpointStreamId: new mongo.ObjectId(),
      snapshotChunkLength: 1,
      storageHooks: {
        afterBatchFlush: async (batch) => {
          if (batch.last_flushed_op == null || batch.last_flushed_op === 0n) {
            return;
          }
          if (++flushed === 1) {
            entered.resolve();
            await release.promise;
          }
        }
      }
    });
    await snapshotter.setupCheckpointsCollection();
    await snapshotter.queueSnapshotTables(null);
    const running = snapshotter.replicationLoop().then(
      () => undefined,
      (error: unknown) => error
    );
    const snapshot = snapshotter.waitForInitialSnapshot();
    try {
      await Promise.race([
        entered.promise,
        delay(5000).then(() => {
          throw new Error('No data flush reached hook');
        })
      ]);
      // This proves page application/admission, beyond the existing source prefetch.
      await Promise.race([
        secondPage.promise,
        delay(5000).then(() => {
          throw new Error('Second page not admitted while first receipt held');
        })
      ]);
      expect((await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0).toBe(0);
    } finally {
      release.resolve();
      try {
        await snapshot;
      } finally {
        abort.abort();
        await running;
      }
    }
    expect(await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)).toBe(3);
  },
  20_000
);
