import { putOp, removeOp } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { storage } from '@powersync/service-core';
import sql from 'mssql';

import { CDCStreamTestContext } from './CDCStreamTestContext.js';
import {
  createTestTableWithBasicId,
  describeWithStorage,
  disableCDCForTable,
  dropTestTable,
  enableCDCForTable,
  insertBasicIdTestData,
  renameTable,
  waitForPendingCDCChanges
} from './util.js';
import { getLatestLSN, toQualifiedTableName } from '@module/utils/mssql.js';
import { SchemaChangeType } from '@module/replication/CDCPoller.js';
import { logger } from '@powersync/lib-services-framework';

describe('MSSQL Schema Changes Tests', () => {
  describeWithStorage({ timeout: 60_000 }, defineSchemaChangesTests);
});

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

function defineSchemaChangesTests(factory: storage.TestStorageFactory) {
  test('Create table: New table in the sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();
    await context.startStreaming();

    await createTestTableWithBasicId(connectionManager, 'test_data');
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');

    const data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Create table: New table created while PowerSync is stopped', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description FROM "test_data%"
  `);

    await createTestTableWithBasicId(connectionManager, 'test_data1');
    const testData = await insertBasicIdTestData(connectionManager, 'test_data1');

    await context.replicateSnapshot();
    await context.startStreaming();

    await context.dispose();

    await createTestTableWithBasicId(connectionManager, 'test_data2');

    await using newContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
    await newContext.loadActiveSyncRules();

    await newContext.replicateSnapshot();
    await newContext.startStreaming();

    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data2');
    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data2');

    const finalState = await newContext.getFinalBucketState('global[]');
    expect(finalState).toMatchObject([
      putOp('test_data1', testData),
      putOp('test_data2', testData1),
      putOp('test_data2', testData2)
    ]);
  });

  test('Create table: New table not in the sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();
    await context.startStreaming();

    await createTestTableWithBasicId(connectionManager, 'test_data_ignored');
    await insertBasicIdTestData(connectionManager, 'test_data_ignored');

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([]);
  });

  test('Drop table: Table in the sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await createTestTableWithBasicId(connectionManager, 'test_data');
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
    await dropTestTable(connectionManager, 'test_data');

    data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([]);
  });

  test('Re-create table', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTableWithBasicId(connectionManager, 'test_data');

    await context.replicateSnapshot();
    await context.startStreaming();

    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1)]);

    let schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await dropTestTable(connectionManager, 'test_data');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.TABLE_DROP);

    await createTestTableWithBasicId(connectionManager, 'test_data');

    const testData = await insertBasicIdTestData(connectionManager, 'test_data');

    data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData)]);
  });

  test('Rename table: Table not in the sync rules to one in the sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTableWithBasicId(connectionManager, 'test_data_old');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data_old');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await renameTable(connectionManager, 'test_data_old', 'test_data');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.TABLE_CREATE);

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');
    const data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Rename table: Table in the sync rules to another table in the sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;

    await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description FROM "test_data%"
  `);

    await createTestTableWithBasicId(connectionManager, 'test_data1');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data1');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await renameTable(connectionManager, 'test_data1', 'test_data2');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.TABLE_RENAME);

    const data = await context.getBucketData('global[]');
    expect(data.slice(0, 2)).toMatchObject([
      // Initial replication
      putOp('test_data1', testData1),
      // Initial truncate
      removeOp('test_data1', testData1.id)
    ]);

    const finalState = await context.getFinalBucketState('global[]');
    expect(finalState).toMatchObject([putOp('test_data2', testData1)]);
  });

  test('Rename table: Table renamed while PowerSync is stopped', async () => {
    let context = await CDCStreamTestContext.open(factory);
    let { connectionManager } = context;

    await context.updateSyncRules(`
  bucket_definitions:
    global:
      data:
        - SELECT id, description FROM "test_data%"
  `);

    await createTestTableWithBasicId(connectionManager, 'test_data1');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data1');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data1', testData1)]);

    await context.dispose();
    await renameTable(connectionManager, 'test_data1', 'test_data2');

    await using newContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
    await newContext.loadActiveSyncRules();

    await newContext.replicateSnapshot();
    await newContext.startStreaming();

    const finalState = await newContext.getFinalBucketState('global[]');
    expect(finalState).toMatchObject([putOp('test_data2', testData1)]);
  });

  test('Rename table: Table in the sync rules to not in the sync rules', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await createTestTableWithBasicId(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData = await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData)]);

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await renameTable(connectionManager, 'test_data', 'test_data_ignored');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.TABLE_RENAME);

    data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      // Initial replication
      putOp('test_data', testData),
      // Truncate
      removeOp('test_data', testData.id)
    ]);
  });

  test('New capture instance created for replicating table triggers re-snapshot', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await createTestTableWithBasicId(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    await enableCDCForTable({ connectionManager, table: 'test_data', captureInstance: 'capture_instance_new' });

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');

    const data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Capture instance created for a sync rule table without a capture instance', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data', false);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');

    await context.replicateSnapshot();
    await context.startStreaming();

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await enableCDCForTable({ connectionManager, table: 'test_data' });
    await expectedSchemaChange(schemaSpy, SchemaChangeType.NEW_CAPTURE_INSTANCE);

    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1)]);

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');

    data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Capture instance removed for an actively replicating table', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data');
    let beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');
    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await disableCDCForTable(connectionManager, 'test_data');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.MISSING_CAPTURE_INSTANCE);

    data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Capture instance removed, and then re-added', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data');

    await context.replicateSnapshot();
    await context.startStreaming();

    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');
    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);

    let schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await disableCDCForTable(connectionManager, 'test_data');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.MISSING_CAPTURE_INSTANCE);

    schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await enableCDCForTable({ connectionManager, table: 'test_data' });
    await expectedSchemaChange(schemaSpy, SchemaChangeType.NEW_CAPTURE_INSTANCE);

    const testData3 = await insertBasicIdTestData(connectionManager, 'test_data');
    const testData4 = await insertBasicIdTestData(connectionManager, 'test_data');

    const finalState = await context.getFinalBucketState('global[]');
    expect(finalState).toMatchObject([
      putOp('test_data', testData1),
      putOp('test_data', testData2),
      putOp('test_data', testData3),
      putOp('test_data', testData4)
    ]);
  });

  test('Column schema changes continue replication, but with warning.', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();
    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await connectionManager.query(`ALTER TABLE test_data ADD new_column INT`);
    await expectedSchemaChange(schemaSpy, SchemaChangeType.TABLE_COLUMN_CHANGES);

    const { recordset: result } = await connectionManager.query(
      `
      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_data')} (description, new_column) 
      OUTPUT INSERTED.id, INSERTED.description
      VALUES (@description, @new_column)
      `,
      [
        { name: 'description', type: sql.NVarChar(sql.MAX), value: 'new_column_description' },
        { name: 'new_column', type: sql.Int, value: 1 }
      ]
    );

    const testData2 = { id: result[0].id, description: result[0].description };

    const data = await context.getBucketData('global[]');
    // Capture instances do not reflect most schema changes until the capture instance is re-created
    // So testData2 will be replicated but without the new column
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);

    expect(
      context.cdcStream.tableCache
        .getAll()
        .every((t) => t.captureInstance && t.captureInstance.pendingSchemaChanges.length > 0)
    ).toBe(true);
  });
}

async function expectedSchemaChange(spy: any, type: SchemaChangeType) {
  logger.info(`Test Assertion: Waiting for schema change: ${type}`);
  await vi.waitFor(() => expect(spy).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ type })), {
    timeout: 20000
  });

  const promises = spy.mock.results.filter((r: any) => r.type === 'return').map((r: any) => r.value);

  await Promise.all(promises.map((p: Promise<unknown>) => expect(p).resolves.toBeUndefined()));
  logger.info(`Test Assertion: Received expected schema change: ${type}`);
}
