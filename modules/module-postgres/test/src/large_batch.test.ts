import { storage } from '@powersync/service-core';
import * as timers from 'timers/promises';
import { describe, expect, test } from 'vitest';
import { populateData } from '../../dist/utils/populate_test_data.js';
import { env } from './env.js';
import {
  describeWithStorage,
  INITIALIZED_MONGO_STORAGE_FACTORY,
  INITIALIZED_POSTGRES_STORAGE_FACTORY,
  TEST_CONNECTION_OPTIONS
} from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';

describe.skipIf(!(env.CI || env.SLOW_TESTS))('batch replication', function () {
  describeWithStorage({ timeout: 240_000 }, function (factory) {
    defineBatchTests(factory);
  });
});

const BASIC_SYNC_RULES = `bucket_definitions:
  global:
    data:
      - SELECT id, description, other FROM "test_data"`;

function defineBatchTests(factory: storage.TestStorageFactory) {
  test('update large record', async () => {
    await using context = await WalStreamTestContext.open(factory);
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
  });

  test('initial replication performance', async () => {
    await using context = await WalStreamTestContext.open(factory);
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
  });

  test('large number of operations', async () => {
    await using context = await WalStreamTestContext.open(factory);
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

    await using context = await WalStreamTestContext.open(factory);
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

    context.startStreaming();

    const checkpoint = await context.getCheckpoint({ timeout: 50_000 });
    const checksum = await context.storage!.getChecksums(checkpoint, ['global[]']);
    expect(checksum.get('global[]')!.count).toEqual((numDocs + 2) * 4);
  });

  test('resuming initial replication (1)', async () => {
    // Stop early - likely to not include deleted row in first replication attempt.
    await testResumingReplication(2000);
  });
  test('resuming initial replication (2)', async () => {
    // Stop late - likely to include deleted row in first replication attempt.
    await testResumingReplication(8000);
  });

  async function testResumingReplication(stopAfter: number) {
    // This tests interrupting and then resuming initial replication.
    // We interrupt replication after test_data1 has fully replicated, and
    // test_data2 has partially replicated.
    // This test relies on interval behavior that is not 100% deterministic:
    // 1. We attempt to abort initial replication once a certain number of
    //    rows have been replicated, but this is not exact. Our only requirement
    //    is that we have not fully replicated test_data2 yet.
    // 2. Order of replication is not deterministic, so which specific rows
    //    have been / have not been replicated at that point is not deterministic.
    //    We do allow for some variation in the test results to account for this.

    await using context = await WalStreamTestContext.open(factory);

    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data1
      - SELECT * FROM test_data2`);
    const { pool } = context;

    await pool.query(`CREATE TABLE test_data1(id serial primary key, description text)`);
    await pool.query(`CREATE TABLE test_data2(id serial primary key, description text)`);

    await pool.query(
      {
        statement: `INSERT INTO test_data1(description) SELECT 'foo' FROM generate_series(1, 1000) i`
      },
      {
        statement: `INSERT INTO test_data2( description) SELECT 'foo' FROM generate_series(1, 10000) i`
      }
    );

    const p = context.replicateSnapshot();

    let done = false;

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    try {
      (async () => {
        while (!done) {
          const count =
            ((await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0) - startRowCount;

          if (count >= stopAfter) {
            break;
          }
          await timers.setTimeout(1);
        }
        // This interrupts initial replication
        await context.dispose();
      })();
      // This confirms that initial replication was interrupted
      await expect(p).rejects.toThrowError();
      done = true;
    } finally {
      done = true;
    }

    // Bypass the usual "clear db on factory open" step.
    await using context2 = await WalStreamTestContext.open(factory, { doNotClear: true });

    // This delete should be using one of the ids already replicated
    const {
      rows: [[id1]]
    } = await context2.pool.query(`DELETE FROM test_data2 WHERE id = (SELECT id FROM test_data2 LIMIT 1) RETURNING id`);
    // This update should also be using one of the ids already replicated
    const {
      rows: [[id2]]
    } = await context2.pool.query(
      `UPDATE test_data2 SET description = 'update1' WHERE id = (SELECT id FROM test_data2 LIMIT 1) RETURNING id`
    );
    const {
      rows: [[id3]]
    } = await context2.pool.query(`INSERT INTO test_data2(description) SELECT 'insert1' RETURNING id`);

    await context2.loadNextSyncRules();
    await context2.replicateSnapshot();

    context2.startStreaming();
    const data = await context2.getBucketData('global[]', undefined, {});

    const deletedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === String(id1));
    const updatedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === String(id2));
    const insertedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === String(id3));

    if (deletedRowOps.length != 0) {
      // The deleted row was part of the first replication batch,
      // so it is removed by streaming replication.
      expect(deletedRowOps.length).toEqual(2);
      expect(deletedRowOps[1].op).toEqual('REMOVE');
    } else {
      // The deleted row was not part of the first replication batch,
      // so it's not in the resulting ops at all.
    }

    expect(updatedRowOps.length).toEqual(2);
    // description for the first op could be 'foo' or 'update1'.
    // We only test the final version.
    expect(JSON.parse(updatedRowOps[1].data as string).description).toEqual('update1');

    expect(insertedRowOps.length).toEqual(2);
    expect(JSON.parse(insertedRowOps[0].data as string).description).toEqual('insert1');
    expect(JSON.parse(insertedRowOps[1].data as string).description).toEqual('insert1');

    // 1000 of test_data1 during first replication attempt.
    // N >= 1000 of test_data2 during first replication attempt.
    // 10000 - N - 1 + 1 of test_data2 during second replication attempt.
    // An additional update during streaming replication (2x total for this row).
    // An additional insert during streaming replication (2x total for this row).
    // If the deleted row was part of the first replication batch, it's removed by streaming replication.
    // This adds 2 ops.
    // We expect this to be 11002 for stopAfter: 2000, and 11004 for stopAfter: 8000.
    // However, this is not deterministic.
    expect(data.length).toEqual(11002 + deletedRowOps.length);
  }

  function printMemoryUsage() {
    const memoryUsage = process.memoryUsage();

    const rss = Math.round(memoryUsage.rss / 1024 / 1024);
    const heap = Math.round(memoryUsage.heapUsed / 1024 / 1024);
    const external = Math.round(memoryUsage.external / 1024 / 1024);
    const arrayBuffers = Math.round(memoryUsage.arrayBuffers / 1024 / 1024);
    console.log(`${rss}MB RSS, ${heap}MB heap, ${external}MB external, ${arrayBuffers}MB ArrayBuffers`);
  }
}
