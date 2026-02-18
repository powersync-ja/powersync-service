import { describe, expect, test } from 'vitest';
import { populateData } from '../../dist/utils/populate_test_data.js';
import { env } from './env.js';
import { describeWithStorage, StorageVersionTestContext, TEST_CONNECTION_OPTIONS } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

describe.skipIf(!(env.CI || env.SLOW_TESTS))('batch replication', function () {
  describeWithStorage({ timeout: 240_000 }, defineBatchTests);
});

const BASIC_SYNC_RULES = `bucket_definitions:
  global:
    data:
      - SELECT id, description, other FROM "test_data"`;

function defineBatchTests({ factory, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof WalStreamTestContext.open>[1]) => {
    return WalStreamTestContext.open(factory, { ...options, storageVersion });
  };

  test('update large record', async () => {
    await using context = await openContext();
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

    const checksum = await context.getChecksums(['global[]'], { timeout: 100_000 });
    const duration = Date.now() - start;
    const used = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    expect(checksum.get('global[]')!.count).toEqual(operation_count);
    const perSecond = Math.round((operation_count / duration) * 1000);
    console.log(`${operation_count} ops in ${duration}ms ${perSecond} ops/s. ${used}MB heap`);
  });

  test('initial replication performance', async () => {
    await using context = await openContext();
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

      const checksum = await context.getChecksums(['global[]'], { timeout: 100_000 });
      const duration = Date.now() - start;
      expect(checksum.get('global[]')!.count).toEqual(operation_count);
      const perSecond = Math.round((operation_count / duration) * 1000);
      console.log(`${operation_count} ops in ${duration}ms ${perSecond} ops/s.`);
      printMemoryUsage();
    } finally {
      clearInterval(interval);
    }
  });

  test('large number of operations', async () => {
    await using context = await openContext();
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

    const checksum = await context.getChecksums(['global[]']);
    const duration = Date.now() - start;
    const used = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
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

    const checksum2 = await context.getChecksums(['global[]'], { timeout: 20_000 });
    const truncateDuration = Date.now() - truncateStart;
    const truncateCount = checksum2.get('global[]')!.count - checksum.get('global[]')!.count;
    expect(truncateCount).toEqual(numTransactions * perTransaction);
    const truncatePerSecond = Math.round((truncateCount / truncateDuration) * 1000);
    console.log(`Truncated ${truncateCount} ops in ${truncateDuration}ms ${truncatePerSecond} ops/s. ${used}MB heap`);
  });

  test('large number of bucket_data docs', async () => {
    // This tests that we don't run into this error:
    //   MongoBulkWriteError: BSONObj size: 16814023 (0x1008FC7) is invalid. Size must be between 0 and 16793600(16MB) First element: insert: "bucket_data"
    // The test is quite sensitive to internals, since we need to
    // generate an internal batch that is just below 16MB.
    //
    // For the test to work, we need a:
    // 1. Large number of documents in the batch.
    // 2. More bucket_data documents than current_data documents,
    //    otherwise other batch limiting thresholds are hit.
    // 3. A large document to make sure we get to just below the 16MB
    //    limit.
    // 4. Another document to make sure the internal batching overflows
    //    to a second batch.

    await using context = await openContext();
    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      # Sync 4x so we get more bucket_data documents
      - SELECT * FROM test_data
      - SELECT * FROM test_data
      - SELECT * FROM test_data
      - SELECT * FROM test_data
      `);
    const { pool } = context;

    await pool.query(`CREATE TABLE test_data(id serial primary key, description text)`);

    const numDocs = 499;
    let description = '';
    while (description.length < 2650) {
      description += '.';
    }

    await pool.query({
      statement: `INSERT INTO test_data(description) SELECT $2 FROM generate_series(1, $1) i`,
      params: [
        { type: 'int4', value: numDocs },
        { type: 'varchar', value: description }
      ]
    });

    let largeDescription = '';

    while (largeDescription.length < 2_768_000) {
      largeDescription += '.';
    }
    await pool.query({
      statement: 'INSERT INTO test_data(description) VALUES($1)',
      params: [{ type: 'varchar', value: largeDescription }]
    });
    await pool.query({
      statement: 'INSERT INTO test_data(description) VALUES($1)',
      params: [{ type: 'varchar', value: 'testingthis' }]
    });
    await context.replicateSnapshot();

    const checksum = await context.getChecksums(['global[]'], { timeout: 50_000 });
    expect(checksum.get('global[]')!.count).toEqual((numDocs + 2) * 4);
  });

  function printMemoryUsage() {
    const memoryUsage = process.memoryUsage();

    const rss = Math.round(memoryUsage.rss / 1024 / 1024);
    const heap = Math.round(memoryUsage.heapUsed / 1024 / 1024);
    const external = Math.round(memoryUsage.external / 1024 / 1024);
    const arrayBuffers = Math.round(memoryUsage.arrayBuffers / 1024 / 1024);
    console.log(`${rss}MB RSS, ${heap}MB heap, ${external}MB external, ${arrayBuffers}MB ArrayBuffers`);
  }
}
