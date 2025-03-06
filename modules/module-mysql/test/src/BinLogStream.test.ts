import { storage } from '@powersync/service-core';
import { METRICS_HELPER, putOp, removeOp } from '@powersync/service-core-tests';
import { v4 as uuid } from 'uuid';
import { describe, expect, test } from 'vitest';
import { BinlogStreamTestContext } from './BinlogStreamUtils.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, INITIALIZED_POSTGRES_STORAGE_FACTORY } from './util.js';
import { ReplicationMetric } from '@powersync/service-types';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

describe.skipIf(!env.TEST_MONGO_STORAGE)(' Binlog stream - mongodb', { timeout: 20_000 }, function () {
  defineBinlogStreamTests(INITIALIZED_MONGO_STORAGE_FACTORY);
});

describe.skipIf(!env.TEST_POSTGRES_STORAGE)(' Binlog stream - postgres', { timeout: 20_000 }, function () {
  defineBinlogStreamTests(INITIALIZED_POSTGRES_STORAGE_FACTORY);
});

function defineBinlogStreamTests(factory: storage.TestStorageFactory) {
  test('Replicate basic values', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description, num FROM "test_data"`);

    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT, num BIGINT)`);

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const startTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;

    context.startStreaming();
    const testId = uuid();
    await connectionManager.query(
      `INSERT INTO test_data(id, description, num) VALUES('${testId}', 'test1', 1152921504606846976)`
    );
    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1', num: 1152921504606846976n })]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const endTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(1);
    expect(endTxCount - startTxCount).toEqual(1);
  });

  test('replicating case sensitive table', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT id, description FROM "test_DATA"
      `);

    await connectionManager.query(`CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY, description text)`);

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const startTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;

    context.startStreaming();

    const testId = uuid();
    await connectionManager.query(`INSERT INTO test_DATA(id, description) VALUES('${testId}','test1')`);

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_DATA', { id: testId, description: 'test1' })]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const endTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(1);
    expect(endTxCount - startTxCount).toEqual(1);
  });

  // TODO: Not supported yet
  // test('replicating TRUNCATE', async () => {
  //   await using context = await BinlogStreamTestContext.create(factory);
  //   const { connectionManager } = context;
  //   const syncRuleContent = `
  // bucket_definitions:
  //   global:
  //     data:
  //       - SELECT id, description FROM "test_data"
  //   by_test_data:
  //     parameters: SELECT id FROM test_data WHERE id = token_parameters.user_id
  //     data: []
  // `;
  //   await context.updateSyncRules(syncRuleContent);
  //   await connectionManager.query(`DROP TABLE IF EXISTS test_data`);
  //   await connectionManager.query(
  //     `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`
  //   );

  //   await context.replicateSnapshot();
  //   context.startStreaming();

  //   const [{ test_id }] = pgwireRows(
  //     await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
  //   );
  //   await connectionManager.query(`TRUNCATE test_data`);

  //   const data = await context.getBucketData('global[]');

  //   expect(data).toMatchObject([
  //     putOp('test_data', { id: test_id, description: 'test1' }),
  //     removeOp('test_data', test_id)
  //   ]);
  // });

  test('replicating changing primary key', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description text)`);

    await context.replicateSnapshot();
    context.startStreaming();

    const testId1 = uuid();
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('${testId1}','test1')`);

    const testId2 = uuid();
    await connectionManager.query(
      `UPDATE test_data SET id = '${testId2}', description = 'test2a' WHERE id = '${testId1}'`
    );

    // This update may fail replicating with:
    // Error: Update on missing record public.test_data:074a601e-fc78-4c33-a15d-f89fdd4af31d :: {"g":1,"t":"651e9fbe9fec6155895057ec","k":"1a0b34da-fb8c-5e6f-8421-d7a3c5d4df4f"}
    await connectionManager.query(`UPDATE test_data SET description = 'test2b' WHERE id = '${testId2}'`);

    // Re-use old id again
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('${testId1}', 'test1b')`);
    await connectionManager.query(`UPDATE test_data SET description = 'test1c' WHERE id = '${testId1}'`);

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      // Initial insert
      putOp('test_data', { id: testId1, description: 'test1' }),
      // Update id, then description
      removeOp('test_data', testId1),
      putOp('test_data', { id: testId2, description: 'test2a' }),
      putOp('test_data', { id: testId2, description: 'test2b' }),
      // Re-use old id
      putOp('test_data', { id: testId1, description: 'test1b' }),
      putOp('test_data', { id: testId1, description: 'test1c' })
    ]);
  });

  test('initial sync', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description text)`);

    const testId = uuid();
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('${testId}','test1')`);

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;

    await context.replicateSnapshot();

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1' })]);
    expect(endRowCount - startRowCount).toEqual(1);
  });

  test('snapshot with date values', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT * FROM "test_data"
      `);

    await connectionManager.query(
      `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT, date DATE, datetime DATETIME, timestamp TIMESTAMP)`
    );

    const testId = uuid();
    await connectionManager.query(`
        INSERT INTO test_data(id, description, date, datetime, timestamp) VALUES('${testId}','testDates', '2023-03-06', '2023-03-06 15:47', '2023-03-06 15:47')
      `);

    await context.replicateSnapshot();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      putOp('test_data', {
        id: testId,
        description: 'testDates',
        date: `2023-03-06`,
        datetime: '2023-03-06T15:47:00.000Z',
        timestamp: '2023-03-06T15:47:00.000Z'
      })
    ]);
  });

  test('replication with date values', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT * FROM "test_data"
      `);

    await connectionManager.query(
      `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT, date DATE, datetime DATETIME NULL, timestamp TIMESTAMP NULL)`
    );

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const startTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;

    context.startStreaming();

    const testId = uuid();
    await connectionManager.query(`
        INSERT INTO test_data(id, description, date, datetime, timestamp) VALUES('${testId}','testDates', '2023-03-06', '2023-03-06 15:47', '2023-03-06 15:47')
      `);
    await connectionManager.query(`UPDATE test_data SET description = ? WHERE id = ?`, ['testUpdated', testId]);

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      putOp('test_data', {
        id: testId,
        description: 'testDates',
        date: `2023-03-06`,
        datetime: '2023-03-06T15:47:00.000Z',
        timestamp: '2023-03-06T15:47:00.000Z'
      }),
      putOp('test_data', {
        id: testId,
        description: 'testUpdated',
        date: `2023-03-06`,
        datetime: '2023-03-06T15:47:00.000Z',
        timestamp: '2023-03-06T15:47:00.000Z'
      })
    ]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const endTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(2);
    expect(endTxCount - startTxCount).toEqual(2);
  });

  test('table not in sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await connectionManager.query(`CREATE TABLE test_donotsync (id CHAR(36) PRIMARY KEY, description text)`);

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const startTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;

    context.startStreaming();

    await connectionManager.query(`INSERT INTO test_donotsync(id, description) VALUES('${uuid()}','test1')`);
    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED_TOTAL)) ?? 0;
    const endTxCount =
      (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL)) ?? 0;

    // There was a transaction, but we should not replicate any actual data
    expect(endRowCount - startRowCount).toEqual(0);
    expect(endTxCount - startTxCount).toEqual(1);
  });

  test('Resume replication', async () => {
    const testId1 = uuid();
    const testId2 = uuid();
    {
      await using context = await BinlogStreamTestContext.open(factory);
      const { connectionManager } = context;
      await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description, num FROM "test_data"`);

      await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT, num BIGINT)`);

      await context.replicateSnapshot();
      context.startStreaming();
      await connectionManager.query(
        `INSERT INTO test_data(id, description, num) VALUES('${testId1}', 'test1', 1152921504606846976)`
      );
      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        putOp('test_data', { id: testId1, description: 'test1', num: 1152921504606846976n })
      ]);
    }
    {
      await using context = await BinlogStreamTestContext.open(factory, { doNotClear: true });
      const { connectionManager } = context;
      await context.loadActiveSyncRules();
      // Does not actually do a snapshot again - just does the required intialization.
      await context.replicateSnapshot();
      context.startStreaming();
      await connectionManager.query(`INSERT INTO test_data(id, description, num) VALUES('${testId2}', 'test2', 0)`);
      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        putOp('test_data', { id: testId1, description: 'test1', num: 1152921504606846976n }),
        putOp('test_data', { id: testId2, description: 'test2', num: 0n })
      ]);
    }
  });
}
