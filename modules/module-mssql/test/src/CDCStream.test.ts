import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';
import { getLatestLSN } from '@module/utils/mssql.js';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { METRICS_HELPER, putOp, removeOp } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import sql from 'mssql';
import { describe, expect, test, vi } from 'vitest';
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

const COMPOSITE_KEY_SYNC_CONFIG = `
config:
  edition: 3
streams:
  global:
    auto_subscribe: true
    query: SELECT id_user AS id, id_user, id_visit FROM test_composite
`;

async function getRowsReplicated() {
  return (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
}

async function getTransactionsReplicated() {
  return (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
}

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
    const startRowCount = await getRowsReplicated();

    await context.replicateSnapshot();
    await context.startStreaming();

    const endRowCount = await getRowsReplicated();
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

    const startRowCount = await getRowsReplicated();
    const startTxCount = await getTransactionsReplicated();

    await context.startStreaming();

    const testData = await insertTestData(connectionManager, 'test_data');

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_data', testData)]);
    const endRowCount = await getRowsReplicated();
    const endTxCount = await getTransactionsReplicated();
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

    const startRowCount = await getRowsReplicated();
    const startTxCount = await getTransactionsReplicated();

    await context.startStreaming();

    await insertTestData(connectionManager, 'test_donotsync');
    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
    const endRowCount = await getRowsReplicated();
    const endTxCount = await getTransactionsReplicated();

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

    const startRowCount = await getRowsReplicated();
    const startTxCount = await getTransactionsReplicated();

    await context.startStreaming();

    const testData = await insertTestData(connectionManager, 'test_DATA');
    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_DATA', testData)]);
    const endRowCount = await getRowsReplicated();
    const endTxCount = await getTransactionsReplicated();
    expect(endRowCount - startRowCount).toEqual(1);
    expect(endTxCount - startTxCount).toBeGreaterThanOrEqual(1);
  });

  // 250 rows end with a partial batch, 300 rows end with an empty batch.
  test.each([250, 300])(
    'Initial snapshot for a SimpleSnapshotQuery table with %i rows, larger than the snapshot batch size.',
    async (rowCount) => {
      await using context = await CDCStreamTestContext.open(factory, { cdcStreamOptions: { snapshotBatchSize: 100 } });
      const { connectionManager } = context;
      await context.updateSyncRules(COMPOSITE_KEY_SYNC_CONFIG);

      await createCompositeKeyTable(connectionManager);
      await insertCompositeKeyTableRows(connectionManager, rowCount);
      const startRowCount = await getRowsReplicated();

      await context.replicateSnapshot();
      await context.startStreaming();

      const endRowCount = await getRowsReplicated();
      expect(endRowCount - startRowCount).toEqual(rowCount);
      expect(await context.getBucketData('global|0[]')).toHaveLength(rowCount);
    }
  );

  test('Interrupting initial snapshot for a SimpleSnapshotQuery table between batches', async () => {
    // The snapshot query is still in progress when the snapshot is interrupted between batches.
    // It must be cancelled before the snapshot transaction can be rolled back.
    await using context = await CDCStreamTestContext.open(factory, { cdcStreamOptions: { snapshotBatchSize: 100 } });
    const { connectionManager } = context;
    await context.updateSyncRules(COMPOSITE_KEY_SYNC_CONFIG);
    await createCompositeKeyTable(connectionManager);
    await insertCompositeKeyTableRows(connectionManager, 5000);
    const startRowCount = await getRowsReplicated();

    const snapshotError = context.replicateSnapshot().then(
      () => null,
      (e) => e
    );
    await vi.waitFor(async () => expect((await getRowsReplicated()) - startRowCount).toBeGreaterThanOrEqual(200), {
      timeout: 10_000,
      interval: 1
    });
    await context.dispose();

    expect(await snapshotError).toBeInstanceOf(ReplicationAbortedError);
  });
}

/**
 * A composite primary key cannot use BatchedSnapshotQuery, so this table is snapshotted with SimpleSnapshotQuery.
 */
async function createCompositeKeyTable(connectionManager: MSSQLConnectionManager) {
  await connectionManager.query(`
    CREATE TABLE test_composite (
      id_user INT NOT NULL,
      id_visit INT NOT NULL,
      CONSTRAINT PK_test_composite PRIMARY KEY (id_user, id_visit)
    )`);
  await enableCDCForTable({ connectionManager, table: 'test_composite' });
}

async function insertCompositeKeyTableRows(connectionManager: MSSQLConnectionManager, rowCount: number) {
  const beforeLSN = await getLatestLSN(connectionManager);
  await connectionManager.query(`
    WITH n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < ${rowCount})
    INSERT INTO test_composite (id_user, id_visit) SELECT i, i % 10 FROM n OPTION (MAXRECURSION ${rowCount})`);
  await waitForPendingCDCChanges(beforeLSN, connectionManager);
}
