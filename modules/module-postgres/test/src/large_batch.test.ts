import { MONGO_STORAGE_FACTORY, StorageFactory } from '@core-tests/util.js';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { TEST_CONNECTION_OPTIONS } from './util.js';
import { walStreamTest } from './wal_stream_utils.js';
import { populateData } from 'dist/populate_test_data.js';

describe('batch replication tests - mongodb', function () {
  // These are slow but consistent tests.
  // Not run on every test run, but we do run on CI, or when manually debugging issues.
  if (env.CI || env.SLOW_TESTS) {
    defineBatchTests(MONGO_STORAGE_FACTORY);
  } else {
    // Need something in this file.
    test('no-op', () => {});
  }
});

const BASIC_SYNC_RULES = `bucket_definitions:
  global:
    data:
      - SELECT id, description, other FROM "test_data"`;

function defineBatchTests(factory: StorageFactory) {
  test(
    'update large record',
    walStreamTest(factory, async (context) => {
      // This test generates a large transaction in MongoDB, despite the replicated data
      // not being that large.
      // If we don't limit transaction size, we could run into this error:
      // > -31800: transaction is too large and will not fit in the storage engine cache
      await context.updateSyncRules(BASIC_SYNC_RULES);
      const { pool } = context;

      await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

      await context.replicateSnapshot();

      let operation_count = await populateData({
        num_transactions: 1,
        per_transaction: 80,
        size: 4_000_000,
        connection: TEST_CONNECTION_OPTIONS
      });

      const start = Date.now();

      context.startStreaming();

      const checkpoint = await context.getCheckpoint({ timeout: 100_000 });
      const duration = Date.now() - start;
      const used = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
      const checksum = await context.storage!.getChecksums(checkpoint, ['global[]']);
      expect(checksum.get('global[]')!.count).toEqual(operation_count);
      const perSecond = Math.round((operation_count / duration) * 1000);
      console.log(`${operation_count} ops in ${duration}ms ${perSecond} ops/s. ${used}MB heap`);
    }),
    { timeout: 120_000 }
  );

  test(
    'initial replication performance',
    walStreamTest(factory, async (context) => {
      // Manual test to check initial replication performance and memory usage
      await context.updateSyncRules(BASIC_SYNC_RULES);
      const { pool } = context;

      await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

      // Some stats (varies a lot):
      // Old 'postgres' driver, using cursor(2)
      // 15 ops in 19559ms 1 ops/s. 354MB RSS, 115MB heap, 137MB external
      // 25 ops in 42984ms 1 ops/s. 377MB RSS, 129MB heap, 137MB external
      // 35 ops in 41337ms 1 ops/s. 365MB RSS, 115MB heap, 137MB external

      // streaming with pgwire
      // 15 ops in 26423ms 1 ops/s. 379MB RSS, 128MB heap, 182MB external, 165MB ArrayBuffers
      // 35 ops in 78897ms 0 ops/s. 539MB RSS, 52MB heap, 87MB external, 83MB ArrayBuffers

      let operation_count = await populateData({
        num_transactions: 1,
        per_transaction: 35,
        size: 14_000_000,
        connection: TEST_CONNECTION_OPTIONS
      });

      global.gc?.();

      // Note that we could already have high memory usage at this point
      printMemoryUsage();

      let interval = setInterval(() => {
        printMemoryUsage();
      }, 2000);
      try {
        const start = Date.now();

        await context.replicateSnapshot();
        await context.storage!.autoActivate();
        context.startStreaming();

        const checkpoint = await context.getCheckpoint({ timeout: 100_000 });
        const duration = Date.now() - start;
        const checksum = await context.storage!.getChecksums(checkpoint, ['global[]']);
        expect(checksum.get('global[]')!.count).toEqual(operation_count);
        const perSecond = Math.round((operation_count / duration) * 1000);
        console.log(`${operation_count} ops in ${duration}ms ${perSecond} ops/s.`);
        printMemoryUsage();
      } finally {
        clearInterval(interval);
      }
    }),
    { timeout: 120_000 }
  );

  test(
    'large number of operations',
    walStreamTest(factory, async (context) => {
      // This just tests performance of a large number of operations inside a transaction.
      await context.updateSyncRules(BASIC_SYNC_RULES);
      const { pool } = context;

      await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

      await context.replicateSnapshot();

      const numTransactions = 20;
      const perTransaction = 1500;
      let operationCount = 0;

      const description = 'description';

      for (let i = 0; i < numTransactions; i++) {
        const prefix = `test${i}K`;

        await pool.query(
          {
            statement: `INSERT INTO test_data(id, description, other) SELECT $1 || i, $2 || i, 'foo' FROM generate_series(1, $3) i`,
            params: [
              { type: 'varchar', value: prefix },
              { type: 'varchar', value: description },
              { type: 'int4', value: perTransaction }
            ]
          },
          {
            statement: `UPDATE test_data SET other = other || '#' WHERE id LIKE $1 || '%'`,
            params: [{ type: 'varchar', value: prefix }]
          }
        );
        operationCount += perTransaction * 2;
      }

      const start = Date.now();

      context.startStreaming();

      const checkpoint = await context.getCheckpoint({ timeout: 50_000 });
      const duration = Date.now() - start;
      const used = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
      const checksum = await context.storage!.getChecksums(checkpoint, ['global[]']);
      expect(checksum.get('global[]')!.count).toEqual(operationCount);
      const perSecond = Math.round((operationCount / duration) * 1000);
      // This number depends on the test machine, so we keep the test significantly
      // lower than expected numbers.
      expect(perSecond).toBeGreaterThan(1000);
      console.log(`${operationCount} ops in ${duration}ms ${perSecond} ops/s. ${used}MB heap`);

      // Truncating is fast (~10k ops/second).
      // We'd need a really large set of data to actually run into limits when truncating,
      // but we just test with the data we have here.
      const truncateStart = Date.now();
      await pool.query(`TRUNCATE test_data`);

      const checkpoint2 = await context.getCheckpoint({ timeout: 20_000 });
      const truncateDuration = Date.now() - truncateStart;

      const checksum2 = await context.storage!.getChecksums(checkpoint2, ['global[]']);
      const truncateCount = checksum2.get('global[]')!.count - checksum.get('global[]')!.count;
      expect(truncateCount).toEqual(numTransactions * perTransaction);
      const truncatePerSecond = Math.round((truncateCount / truncateDuration) * 1000);
      console.log(`Truncated ${truncateCount} ops in ${truncateDuration}ms ${truncatePerSecond} ops/s. ${used}MB heap`);
    }),
    { timeout: 90_000 }
  );

  function printMemoryUsage() {
    const memoryUsage = process.memoryUsage();

    const rss = Math.round(memoryUsage.rss / 1024 / 1024);
    const heap = Math.round(memoryUsage.heapUsed / 1024 / 1024);
    const external = Math.round(memoryUsage.external / 1024 / 1024);
    const arrayBuffers = Math.round(memoryUsage.arrayBuffers / 1024 / 1024);
    console.log(`${rss}MB RSS, ${heap}MB heap, ${external}MB external, ${arrayBuffers}MB ArrayBuffers`);
  }
}
