import { getLatestLSN } from '@module/utils/mssql.js';
import { storage } from '@powersync/service-core';
import { METRICS_HELPER, putOp, removeOp } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import sql from 'mssql';
import { describe, expect, test } from 'vitest';
import { CDCStreamTestContext } from './CDCStreamTestContext.js';
import {
  createTestTable,
  describeWithStorage,
  enableCDCForTable,
  insertTestData,
  waitForPendingCDCChanges
} from './util.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

describe('CDCStream tests', () => {
  describeWithStorage({ timeout: 20_000 }, defineCDCStreamTests);
});

function defineCDCStreamTests(config: storage.TestStorageConfig) {
  const { factory } = config;

  test('Initial snapshot sync', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTable(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData = await insertTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);
    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;

    await context.replicateSnapshot();
    await context.startStreaming();

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData)]);
    expect(endRowCount - startRowCount).toEqual(1);
  });

  test('Initial snapshot of a composite primary key table larger than the snapshot batch size', async () => {
    // A composite primary key cannot use BatchedSnapshotQuery, so the table is read in a single pass.
    await using context = await CDCStreamTestContext.open(factory, { cdcStreamOptions: { snapshotBatchSize: 100 } });
    const { connectionManager } = context;
    await context.updateSyncRules(`
config:
  edition: 3
streams:
  global:
    auto_subscribe: true
    query: SELECT id_user AS id, id_user, id_visit FROM test_composite
`);

    await connectionManager.query(`
      CREATE TABLE test_composite (
        id_user INT NOT NULL,
        id_visit INT NOT NULL,
        CONSTRAINT PK_test_composite PRIMARY KEY (id_user, id_visit)
      )`);
    await enableCDCForTable({ connectionManager, table: 'test_composite' });
    const beforeLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`
      WITH n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 250)
      INSERT INTO test_composite (id_user, id_visit) SELECT i, i % 10 FROM n OPTION (MAXRECURSION 250)`);
    await waitForPendingCDCChanges(beforeLSN, connectionManager);
    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;

    await context.replicateSnapshot();
    await context.startStreaming();

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(250);
    expect(await context.getBucketData('global|0[]')).toHaveLength(250);
  });

  test('Replicate basic values', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTable(connectionManager, 'test_data');
    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    await context.startStreaming();

    const testData = await insertTestData(connectionManager, 'test_data');

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_data', testData)]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(1);
    expect(endTxCount - startTxCount).toEqual(1);
  });

  test('Replicate row updates', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTable(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData = await insertTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);
    await context.replicateSnapshot();

    await context.startStreaming();

    const updatedTestData = { ...testData };
    updatedTestData.description = 'updated';
    await connectionManager.query(`UPDATE test_data SET description = @description WHERE id = @id`, [
      { name: 'description', type: sql.NVarChar(sql.MAX), value: updatedTestData.description },
      { name: 'id', type: sql.UniqueIdentifier, value: updatedTestData.id }
    ]);

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData), putOp('test_data', updatedTestData)]);
  });

  test('Replicate row deletions', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTable(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData = await insertTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);
    await context.replicateSnapshot();

    await context.startStreaming();

    await connectionManager.query(`DELETE FROM test_data WHERE id = @id`, [
      { name: 'id', type: sql.UniqueIdentifier, value: testData.id }
    ]);

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData), removeOp('test_data', testData.id)]);
  });

  test('Replicate multiple tables in sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    // Table wildcards are not supported - each table is listed explicitly.
    await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description FROM "test_data_1"
        - SELECT id, description FROM "test_data_2"`);

    await createTestTable(connectionManager, 'test_data_1');
    await createTestTable(connectionManager, 'test_data_2');

    const testData11 = await insertTestData(connectionManager, 'test_data_1');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData21 = await insertTestData(connectionManager, 'test_data_2');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    const testData12 = await insertTestData(connectionManager, 'test_data_1');
    const testData22 = await insertTestData(connectionManager, 'test_data_2');

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data_1', testData11),
      putOp('test_data_2', testData21),
      putOp('test_data_1', testData12),
      putOp('test_data_2', testData22)
    ]);
  });

  test('Replication for tables not in the sync config are ignored', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    // test_data is in the sync config and must exist for replication to start; test_donotsync is not.
    await createTestTable(connectionManager, 'test_data');
    await createTestTable(connectionManager, 'test_donotsync');

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    await context.startStreaming();

    await insertTestData(connectionManager, 'test_donotsync');
    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    // There was a transaction, but it is not counted since it is not for a table in the sync config
    expect(endRowCount - startRowCount).toEqual(0);
    expect(endTxCount - startTxCount).toEqual(0);
  });

  test('Replicate case sensitive table', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT id, description FROM "test_DATA"
      `);

    await createTestTable(connectionManager, 'test_DATA');

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    await context.startStreaming();

    const testData = await insertTestData(connectionManager, 'test_DATA');
    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_DATA', testData)]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(1);
    expect(endTxCount - startTxCount).toBeGreaterThanOrEqual(1);
  });
}
