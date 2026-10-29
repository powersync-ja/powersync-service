import { METRICS_HELPER } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import { expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

test.skipIf(!env.TEST_MONGO_STORAGE).each(['key scan', 'cursor'])(
  'snapshot queues page progress while publication is pending (%s)',
  async (mode) => {
    await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion: 4,
      walStreamOptions: { snapshotChunkLength: 1 }
    });
    await context.updateSyncRules(`
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM test_data
`);
    // Composite identities use SimpleSnapshotQuery / FETCH; a single integer
    // identity uses resumable key scans. Neither may issue concurrent source queries.
    await context.pool.query(
      mode === 'key scan'
        ? 'CREATE TABLE test_data(id integer primary key, other integer, description text)'
        : 'CREATE TABLE test_data(id integer, other integer, description text, PRIMARY KEY(id, other))'
    );
    await context.pool.query("INSERT INTO test_data VALUES (1, 1, 'first'), (2, 2, 'second'), (3, 3, 'third')");
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const secondPage = Promise.withResolvers<void>();
    let flushed = 0;
    const storage = context.storage!;
    const create = storage.createWriter.bind(storage);
    using _create = vi.spyOn(storage, 'createWriter').mockImplementation(async (options) => {
      const writer = await create({
        ...options,
        hooks: {
          ...options.hooks,
          afterBatchFlush: async (batch) => {
            await options.hooks?.afterBatchFlush?.(batch);
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
    const baseline = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const snapshot = context.replicateSnapshot();
    try {
      await entered.promise;
      await secondPage.promise;
      expect((await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0).toBe(baseline);
    } finally {
      release.resolve();
      await snapshot;
    }
    expect(await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)).toBe(baseline + 3);
    expect((await context.getCheckpoint()).lsn).not.toBeNull();
  },
  20_000
);
