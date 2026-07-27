import { storage } from '@powersync/service-core';
import { putOp, removeOp } from '@powersync/service-core-tests';
import sql from 'mssql';
import { describe, expect, test, vi } from 'vitest';

import { SchemaChangeType } from '@module/replication/CDCPoller.js';
import { getLatestLSN, toQualifiedTableName } from '@module/utils/mssql.js';
import { logger } from '@powersync/lib-services-framework';
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

describe('MSSQL Schema Changes Tests', () => {
  describeWithStorage({ timeout: 60_000 }, defineSchemaChangesTests);
});

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

const WILDCARD_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT * FROM "test_data"
`;

function defineSchemaChangesTests(config: storage.TestStorageConfig) {
  const { factory } = config;

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
    await using context = await CDCStreamTestContext.open(factory, {
      cdcStreamOptions: { schemaCheckIntervalMs: 5000 }
    });
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

  /**
   * Start replication pinned to the original capture instance, then add a source column and create
   * a new capture instance that includes it. The running stream must keep polling the original
   * instance, so a new row is replicated without the newly captured column.
   */
  test('New capture instance with changed schema keeps the existing pinned capture instance', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(WILDCARD_SYNC_RULES);

    const { connectionManager } = context;
    await createTestTableWithBasicId(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    const tableBefore = context.cdcStream.tableCache.getAll()[0];
    expect(tableBefore.isCaptureInstancePinned(), 'The initial source-table binding should be pinned').toBe(true);
    const pinnedCaptureObjectId = tableBefore.pinnedCaptureObjectId;
    expect(
      tableBefore.captureInstance?.objectId,
      'The active capture instance should match the persisted capture-instance pin'
    ).toBe(pinnedCaptureObjectId);
    const sourceTableIds = tableBefore.sourceTables.map((table) => table.id.toString());

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await connectionManager.query(
      `ALTER TABLE ${toQualifiedTableName(connectionManager.schema, 'test_data')} ADD new_column INT`
    );
    await enableCDCForTable({ connectionManager, table: 'test_data', captureInstance: 'capture_instance_new' });
    await expectedSchemaChange(schemaSpy, SchemaChangeType.NEW_CAPTURE_INSTANCE);

    const tableAfter = context.cdcStream.tableCache.get(tableBefore.objectId)!;
    expect(
      tableAfter.pinnedCaptureObjectId,
      'Detecting a newer capture instance should not change the persisted pin'
    ).toBe(pinnedCaptureObjectId);
    expect(
      tableAfter.captureInstance?.objectId,
      'The running stream should continue polling its pinned capture instance'
    ).toBe(pinnedCaptureObjectId);
    expect(
      tableAfter.sourceTables.map((table) => table.id.toString()),
      'Detecting a newer capture instance should not replace the PowerSync SourceTable records'
    ).toEqual(sourceTableIds);

    const { recordset: result } = await connectionManager.query(
      `
      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_data')} (description, new_column)
      OUTPUT INSERTED.id, INSERTED.description, INSERTED.new_column
      VALUES (@description, @new_column)
      `,
      [
        { name: 'description', type: sql.NVarChar(sql.MAX), value: 'new_capture_column_description' },
        { name: 'new_column', type: sql.Int, value: 1 }
      ]
    );
    const testData2 = result[0];

    const data = await context.getFinalBucketState('global[]');
    const replicatedTestData2 = data.find((operation) => operation.object_id === String(testData2.id));
    expect(replicatedTestData2, 'The row should be available at the next checkpoint').toBeDefined();
    expect(
      JSON.parse(replicatedTestData2!.data!),
      'The pinned stream should replicate the row using the original capture schema'
    ).toEqual({ id: testData2.id, description: testData2.description });
  });

  /**
   * Create a replacement capture instance immediately after a schema check. The replacement omits
   * `description`, which the original instance still captures. Let the running stream process a
   * later transaction before the next schema check so its persisted LSN advances beyond the new
   * instance's minimum LSN without detecting that instance. After restart, the row's description
   * proves whether PowerSync restored the original capture instance or inferred the replacement.
   */
  test('Restart restores the existing pin after advancing past an undetected capture instance', async () => {
    let initialBinding: {
      sourceTableObjectId: number;
      pinnedCaptureObjectId: number | null;
      sourceTableIds: string[];
    };
    {
      await using context = await CDCStreamTestContext.open(factory, {
        cdcStreamOptions: { schemaCheckIntervalMs: 60_000 }
      });
      await context.updateSyncRules(WILDCARD_SYNC_RULES);
      const { connectionManager } = context;

      await createTestTableWithBasicId(connectionManager, 'test_data');
      await context.replicateSnapshot();
      await context.startStreaming();
      await context.getCheckpoint();

      const tableBefore = context.cdcStream.tableCache.getAll()[0];
      expect(tableBefore.isCaptureInstancePinned(), 'The initial source-table binding should be pinned').toBe(true);
      initialBinding = {
        sourceTableObjectId: tableBefore.objectId,
        pinnedCaptureObjectId: tableBefore.pinnedCaptureObjectId,
        sourceTableIds: tableBefore.sourceTables.map((table) => table.id.toString())
      };

      const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
      await connectionManager.query(
        `ALTER TABLE ${toQualifiedTableName(connectionManager.schema, 'test_data')} ADD new_column INT`
      );
      await enableCDCForTable({
        connectionManager,
        table: 'test_data',
        captureInstance: 'capture_instance_new',
        // Give the replacement instance a distinct schema: it captures the new column but not description.
        capturedColumns: ['id', 'new_column']
      });

      const { recordset: result } = await connectionManager.query(
        `
        INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_data')} (description, new_column)
        OUTPUT INSERTED.id, INSERTED.description, INSERTED.new_column
        VALUES (@description, @new_column)
        `,
        [
          { name: 'description', type: sql.NVarChar(sql.MAX), value: 'before_restart' },
          { name: 'new_column', type: sql.Int, value: 1 }
        ]
      );
      const rowBeforeRestart = result[0];

      const data = await context.getFinalBucketState('global[]');
      const replicatedRow = data.find((operation) => operation.object_id === String(rowBeforeRestart.id));
      expect(
        replicatedRow,
        'A transaction after the new capture instance was created should reach a PowerSync checkpoint'
      ).toBeDefined();
      expect(
        JSON.parse(replicatedRow!.data!),
        'Data captured only by the original instance should be visible before PowerSync stops'
      ).toEqual({ id: rowBeforeRestart.id, description: rowBeforeRestart.description });
      expect(
        schemaSpy.mock.calls.some(([, change]) => change.type === SchemaChangeType.NEW_CAPTURE_INSTANCE),
        'The replacement capture instance should still be undetected when PowerSync stops'
      ).toBe(false);
    }

    {
      await using replicationContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
      await replicationContext.loadActiveSyncRules();
      await replicationContext.replicateSnapshot();

      const tableAfter = replicationContext.cdcStream.tableCache.get(initialBinding.sourceTableObjectId)!;
      expect(
        tableAfter.pinnedCaptureObjectId,
        'Restarting should restore the persisted pin even though the resume LSN passed the newer capture instance'
      ).toBe(initialBinding.pinnedCaptureObjectId);
      expect(
        tableAfter.captureInstance?.objectId,
        'The restarted stream should poll the original capture instance rather than infer the newest one from LSNs'
      ).toBe(initialBinding.pinnedCaptureObjectId);
      expect(
        tableAfter.sourceTables.map((table) => table.id.toString()),
        'Restarting should reuse the existing PowerSync SourceTable records without re-snapshotting'
      ).toEqual(initialBinding.sourceTableIds);

      await replicationContext.startStreaming();
      const { connectionManager } = replicationContext;
      const { recordset: result } = await connectionManager.query(
        `
        INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_data')} (description, new_column)
        OUTPUT INSERTED.id, INSERTED.description, INSERTED.new_column
        VALUES (@description, @new_column)
        `,
        [
          { name: 'description', type: sql.NVarChar(sql.MAX), value: 'after_restart' },
          { name: 'new_column', type: sql.Int, value: 2 }
        ]
      );
      const rowAfterRestart = result[0];

      const finalState = await replicationContext.getFinalBucketState('global[]');
      const replicatedRow = finalState.find((operation) => operation.object_id === String(rowAfterRestart.id));
      expect(replicatedRow, 'The original capture instance should continue replicating after restart').toBeDefined();
      expect(
        JSON.parse(replicatedRow!.data!),
        'Data captured only by the original instance should remain visible after restart'
      ).toEqual({ id: rowAfterRestart.id, description: rowAfterRestart.description });
    }
  });

  /**
   * Persist a capture-instance pin, stop PowerSync, then use a separate context to add a source
   * column and insert a row before creating the new capture instance. A third context restarts
   * replication and must recover that change from the original capture instance.
   */
  test('New capture instance with changed schema created while PowerSync is stopped restores the existing pin', async () => {
    let initialBinding: {
      sourceTableObjectId: number;
      pinnedCaptureObjectId: number | null;
      sourceTableIds: string[];
    };
    {
      await using context = await CDCStreamTestContext.open(factory);
      await context.updateSyncRules(WILDCARD_SYNC_RULES);
      const { connectionManager } = context;

      await createTestTableWithBasicId(connectionManager, 'test_data');
      const beforeLSN = await getLatestLSN(connectionManager);
      const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
      await waitForPendingCDCChanges(beforeLSN, connectionManager);

      await context.replicateSnapshot();
      await context.startStreaming();

      const data = await context.getBucketData('global[]');
      expect(data, 'The initial row should be available before PowerSync stops').toMatchObject([
        putOp('test_data', testData1)
      ]);

      const tableBefore = context.cdcStream.tableCache.getAll()[0];
      expect(tableBefore.isCaptureInstancePinned(), 'The initial source-table binding should be pinned').toBe(true);
      initialBinding = {
        sourceTableObjectId: tableBefore.objectId,
        pinnedCaptureObjectId: tableBefore.pinnedCaptureObjectId,
        sourceTableIds: tableBefore.sourceTables.map((table) => table.id.toString())
      };
    }

    let testData2: any;
    {
      await using changeContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
      const changeConnectionManager = changeContext.connectionManager;
      await changeConnectionManager.query(
        `ALTER TABLE ${toQualifiedTableName(changeConnectionManager.schema, 'test_data')} ADD new_column INT`
      );
      const beforeChangedRowLSN = await getLatestLSN(changeConnectionManager);
      const { recordset: result } = await changeConnectionManager.query(
        `
      INSERT INTO ${toQualifiedTableName(changeConnectionManager.schema, 'test_data')} (description, new_column)
      OUTPUT INSERTED.id, INSERTED.description, INSERTED.new_column
      VALUES (@description, @new_column)
      `,
        [
          { name: 'description', type: sql.NVarChar(sql.MAX), value: 'new_capture_column_while_stopped' },
          { name: 'new_column', type: sql.Int, value: 1 }
        ]
      );
      testData2 = result[0];
      await waitForPendingCDCChanges(beforeChangedRowLSN, changeConnectionManager);
      await enableCDCForTable({
        connectionManager: changeConnectionManager,
        table: 'test_data',
        captureInstance: 'capture_instance_new'
      });
    }

    {
      await using replicationContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
      await replicationContext.loadActiveSyncRules();
      await replicationContext.replicateSnapshot();

      const tableAfter = replicationContext.cdcStream.tableCache.get(initialBinding.sourceTableObjectId)!;
      expect(
        tableAfter.pinnedCaptureObjectId,
        'Restarting the replication stream should restore the persisted capture-instance pin'
      ).toBe(initialBinding.pinnedCaptureObjectId);
      expect(
        tableAfter.captureInstance?.objectId,
        'The restarted stream should poll the persisted capture instance rather than the newest instance'
      ).toBe(initialBinding.pinnedCaptureObjectId);
      expect(
        tableAfter.sourceTables.map((table) => table.id.toString()),
        'Restarting with a newer capture instance available should reuse the PowerSync SourceTable records'
      ).toEqual(initialBinding.sourceTableIds);

      await replicationContext.startStreaming();

      const finalState = await replicationContext.getFinalBucketState('global[]');
      const replicatedTestData2 = finalState.find((operation) => operation.object_id === String(testData2.id));
      expect(replicatedTestData2, 'The row should be available at the next checkpoint after restart').toBeDefined();
      expect(
        JSON.parse(replicatedTestData2!.data!),
        'The restored pin should replicate the row using the original capture schema'
      ).toEqual({ id: testData2.id, description: testData2.description });
    }
  });

  test('Capture instance created for a sync rule table without a capture instance', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data', false);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');

    await context.replicateSnapshot();
    await context.startStreaming();

    await enableCDCForTable({ connectionManager, table: 'test_data' });

    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1)]);

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');

    data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Capture instance removed for an actively replicating table', async () => {
    await using context = await CDCStreamTestContext.open(factory, {
      cdcStreamOptions: { schemaCheckIntervalMs: 5000 }
    });
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

  test('A replacement for a removed pinned capture instance is not silently adopted', async () => {
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

    const table = context.cdcStream.tableCache.getAll()[0];
    const pinnedCaptureObjectId = table.pinnedCaptureObjectId;
    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await disableCDCForTable(connectionManager, 'test_data');
    await expectedSchemaChange(schemaSpy, SchemaChangeType.MISSING_CAPTURE_INSTANCE);

    schemaSpy.mockClear();
    await enableCDCForTable({ connectionManager, table: 'test_data' });
    await expectedSchemaChange(schemaSpy, SchemaChangeType.MISSING_CAPTURE_INSTANCE);
    expect(
      table.pinnedCaptureObjectId,
      'Enabling a replacement capture instance should not change the persisted pin'
    ).toBe(pinnedCaptureObjectId);
    expect(
      table.captureInstance,
      'The replacement capture instance should not be adopted by the running stream'
    ).toBeNull();

    const testData3 = await insertBasicIdTestData(connectionManager, 'test_data');
    const testData4 = await insertBasicIdTestData(connectionManager, 'test_data');

    const finalState = await context.getFinalBucketState('global[]');
    expect(
      finalState.find((operation) => operation.object_id === String(testData3.id)),
      'Rows from the replacement capture instance should not be replicated'
    ).toBeUndefined();
    expect(
      finalState.find((operation) => operation.object_id === String(testData4.id)),
      'Rows from the replacement capture instance should not be replicated'
    ).toBeUndefined();
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
  try {
    await vi.waitFor(() => expect(spy).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ type })), {
      timeout: 55000
    });
  } catch (error) {
    // The error message thrown here is extremely verbose and not particularly helpful
    throw new Error(`Test Assertion: Timeout waiting for schema change: ${type}`);
  }
  const promises = spy.mock.results.filter((r: any) => r.type === 'return').map((r: any) => r.value);

  await Promise.all(promises.map((p: Promise<unknown>) => expect(p).resolves.toBeUndefined()));
  logger.info(`Test Assertion: Received expected schema change: ${type}`);
}
