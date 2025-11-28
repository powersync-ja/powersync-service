import { describe, expect, test } from 'vitest';
import { METRICS_HELPER, putOp, removeOp } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import { createTestTable, describeWithStorage, insertTestData, waitForPendingCDCChanges } from './util.js';
import { storage } from '@powersync/service-core';
import { CDCStreamTestContext } from './CDCStreamTestContext.js';
import { getLatestReplicatedLSN } from '@module/utils/mssql.js';
import sql from 'mssql';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

describe('CDCStream tests', () => {
  describeWithStorage({ timeout: 20_000 }, defineCDCStreamTests);
});

function defineCDCStreamTests(factory: storage.TestStorageFactory) {
  test('Initial snapshot sync', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTable(connectionManager, 'test_data');
    const beforeLSN = await getLatestReplicatedLSN(connectionManager);
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
    const beforeLSN = await getLatestReplicatedLSN(connectionManager);
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
    const beforeLSN = await getLatestReplicatedLSN(connectionManager);
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

  test('Replicate matched wild card tables in sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description FROM "test_data_%"`);

    await createTestTable(connectionManager, 'test_data_1');
    await createTestTable(connectionManager, 'test_data_2');

    const beforeLSN = await getLatestReplicatedLSN(connectionManager);
    const testData11 = await insertTestData(connectionManager, 'test_data_1');
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

  test('Replication for tables not in the sync rules are ignored', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

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

    // There was a transaction, but it is not counted since it is not for a table in the sync rules
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
