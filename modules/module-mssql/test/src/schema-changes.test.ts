import { compareIds, putOp, removeOp } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { storage } from '@powersync/service-core';

import { CDCStreamTestContext } from './CDCStreamTestContext.js';
import {
  createTestTable,
  createTestTableWithBasicId,
  describeWithStorage,
  dropTestTable,
  INITIALIZED_MONGO_STORAGE_FACTORY,
  insertBasicIdTestData,
  insertTestData,
  waitForPendingCDCChanges
} from './util.js';
import { enableCDCForTable, escapeIdentifier, getLatestLSN, toQualifiedTableName } from '@module/utils/mssql.js';
import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';

// describe('MSSQL Schema Changes Tests', () => {
//   describeWithStorage({ timeout: 20_000 }, defineSchemaChangesTests);
// });

defineSchemaChangesTests(INITIALIZED_MONGO_STORAGE_FACTORY);

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

function defineSchemaChangesTests(factory: storage.TestStorageFactory) {
  test('Re-create table', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTable(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();
    const testData2 = await insertTestData(connectionManager, 'test_data');

    await dropTestTable(connectionManager, 'test_data');
    console.log('Dropped table');

    await createTestTable(connectionManager, 'test_data');
    console.log('Created table second time');

    const testData3 = await insertTestData(connectionManager, 'test_data');

    let data = await context.getBucketData('global[]');

    expect(data.slice(0, 2)).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
    expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
      removeOp('test_data', testData1.id.toString()),
      removeOp('test_data', testData2.id.toString())
    ]);
    expect(data.slice(4)).toMatchObject([
      putOp('test_data', testData3), // Snapshot insert
      putOp('test_data', testData3) // Insert from CDC stream
    ]);
  });

  //   test('Create table: New table is in the sync rules', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await createTestTable(connectionManager, 'test_data');
  //     const beforeInsertLSN = await getLatestLSN(connectionManager);
  //     await insertRow(connectionManager, 'test_data', ROW_T1);
  //     await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

  //     const data = await context.getBucketData('global[]');

  //     expect(data).toMatchObject([PUT_T1, PUT_T1]);
  //   });

  //   test('Create table: New table is not in the sync rules', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await createTestTable(connectionManager, 'test_data_ignored');
  //     const beforeInsertLSN = await getLatestLSN(connectionManager);
  //     await insertRow(connectionManager, 'test_data_ignored', ROW_T1);
  //     await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

  //     const data = await context.getBucketData('global[]');

  //     expect(data).toMatchObject([]);
  //   });

  //   test('Rename table: Table not in the sync rules to one in the sync rules', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     await createTestTable(connectionManager, 'test_data_old');
  //     await insertRow(connectionManager, 'test_data_old', ROW_T1);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await renameTable(connectionManager, 'test_data_old', 'test_data');

  //     const beforeInsertLSN = await getLatestLSN(connectionManager);
  //     await insertRow(connectionManager, 'test_data', ROW_T2);
  //     await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

  //     const data = await context.getBucketData('global[]');

  //     expect(data).toMatchObject([
  //       // Snapshot insert
  //       PUT_T1,
  //       PUT_T2,
  //       // Replicated insert
  //       PUT_T2
  //     ]);
  //   });

  //   test('Rename table: Table in the sync rules to another table in the sync rules', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);

  //     await context.updateSyncRules(`
  // bucket_definitions:
  //   global:
  //     data:
  //       - SELECT id, description FROM "test_data%"
  // `);

  //     const { connectionManager } = context;
  //     await createTestTable(connectionManager, 'test_data1');
  //     await insertRow(connectionManager, 'test_data1', ROW_T1);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await renameTable(connectionManager, 'test_data1', 'test_data2');
  //     const beforeInsertLSN = await getLatestLSN(connectionManager);
  //     await insertRow(connectionManager, 'test_data2', ROW_T2);
  //     await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

  //     const data = await context.getBucketData('global[]');

  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial replication
  //       putOp('test_data1', ROW_T1),
  //       // Initial truncate
  //       removeOp('test_data1', ID_T1)
  //     ]);

  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
  //       // Snapshot insert
  //       putOp('test_data2', ROW_T1),
  //       putOp('test_data2', ROW_T2)
  //     ]);

  //     expect(data.slice(4)).toMatchObject([
  //       // Replicated insert
  //       putOp('test_data2', ROW_T2)
  //     ]);
  //   });

  //   test('Rename table: Table in the sync rules to not in the sync rules', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     const { connectionManager } = context;
  //     await createTestTable(connectionManager, 'test_data');
  //     await insertRow(connectionManager, 'test_data', ROW_T1);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await renameTable(connectionManager, 'test_data', 'test_data_not_in_sync_rules');
  //     const beforeInsertLSN = await getLatestLSN(connectionManager);
  //     await insertRow(connectionManager, 'test_data_not_in_sync_rules', ROW_T2);
  //     await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

  //     const data = await context.getBucketData('global[]');

  //     expect(data).toMatchObject([
  //       // Initial replication
  //       PUT_T1,
  //       // Truncate
  //       REMOVE_T1
  //     ]);
  //   });

  //   test('New capture instance resyncs the table', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     const { connectionManager } = context;
  //     await createTestTable(connectionManager, 'test_data');
  //     await insertRow(connectionManager, 'test_data', ROW_T1);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await connectionManager.query(
  //       `ALTER TABLE ${qualifiedTableName(connectionManager, 'test_data')} ADD new_column VARCHAR(MAX)`
  //     );

  //     await disableCDCForTable(connectionManager, 'test_data');
  //     await enableCDCForTable({ connectionManager, table: 'test_data' });

  //     const beforeInsertLSN = await getLatestLSN(connectionManager);
  //     await insertRow(connectionManager, 'test_data', ROW_T2);
  //     await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

  //     const data = await context.getBucketData('global[]');

  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       // Truncate
  //       REMOVE_T1
  //     ]);

  //     expect(data.slice(2)).toMatchObject([
  //       // Snapshot inserts
  //       PUT_T1,
  //       PUT_T2,
  //       // Replicated insert
  //       PUT_T2
  //     ]);
  //   });

  //   test('Drop a table in the sync rules', async () => {
  //     await using context = await CDCStreamTestContext.open(factory);
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     const { connectionManager } = context;
  //     await createTestTable(connectionManager, 'test_data');
  //     await insertRow(connectionManager, 'test_data', ROW_T1);
  //     await insertRow(connectionManager, 'test_data', ROW_T2);

  //     await context.replicateSnapshot();
  //     await context.startStreaming();

  //     await resetTestTable(connectionManager, 'test_data');

  //     const data = await context.getBucketData('global[]');

  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       PUT_T2
  //     ]);

  //     expect(data.slice(2).sort(compareIds)).toMatchObject([
  //       // Drop
  //       REMOVE_T1,
  //       REMOVE_T2
  //     ]);
  //   });
}

async function renameTable(connectionManager: MSSQLConnectionManager, fromTable: string, toTable: string) {
  await connectionManager.query(`EXEC sp_rename '${connectionManager.schema}.${fromTable}', '${toTable}'`);
}

async function disableCDCForTable(connectionManager: MSSQLConnectionManager, tableName: string) {
  await connectionManager.execute('sys.sp_cdc_disable_table', [
    { name: 'source_schema', value: connectionManager.schema },
    { name: 'source_name', value: tableName },
    { name: 'capture_instance', value: 'all' }
  ]);
}
