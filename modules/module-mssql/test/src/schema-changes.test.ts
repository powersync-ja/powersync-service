import { SourceEntityDescriptor, storage } from '@powersync/service-core';
import { putOp, test_utils } from '@powersync/service-core-tests';
import sql from 'mssql';
import { describe, expect, test, vi } from 'vitest';

import { LSN } from '@module/common/LSN.js';
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

  test('Create table: a sync rules table that does not exist stops replication', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    // test_data is never created.
    await expect(context.replicateSnapshot()).rejects.toThrow(/does not exist/);
  });

  test('Create table: a table created after deploy is not replicated', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await createTestTableWithBasicId(connectionManager, 'test_data');
    await context.replicateSnapshot();
    await context.startStreaming();

    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    expect(await context.getBucketData('global[]')).toMatchObject([putOp('test_data', testData1)]);

    // The replicated table set is fixed at deploy, so this never enters scope. It also does not
    // disturb the running stream.
    await createTestTableWithBasicId(connectionManager, 'test_data_new');
    await insertBasicIdTestData(connectionManager, 'test_data_new');

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');
    const data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
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

  /**
   * Schema checks run before polling within a cycle, so continuing after a drop would commit past
   * changes for the dropped table that were never read. The job stops instead, and the already
   * replicated data is retained for a redeploy to clean up.
   */
  test('Drop table: replication stops and the replicated data is retained', async () => {
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

    expect(await context.getBucketData('global[]')).toMatchObject([
      putOp('test_data', testData1),
      putOp('test_data', testData2)
    ]);

    await dropTestTable(connectionManager, 'test_data');

    await expect(context.streamingPromise, 'Dropping a replicated table should stop the job').rejects.toThrow(
      /has been dropped from the source/
    );

    expect(
      await context.getBucketData('global[]'),
      'Dropping the source table should not delete data already replicated to clients'
    ).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  test('Rename table: replication stops with a warning and the replicated data is retained', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await createTestTableWithBasicId(connectionManager, 'test_data');
    const beforeLSN = await getLatestLSN(connectionManager);
    const testData = await insertBasicIdTestData(connectionManager, 'test_data');
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    await context.replicateSnapshot();
    await context.startStreaming();

    expect(await context.getBucketData('global[]')).toMatchObject([putOp('test_data', testData)]);

    await renameTable(connectionManager, 'test_data', 'test_data_renamed');

    await expect(context.streamingPromise, 'Renaming a replicated table should stop the job').rejects.toThrow(
      /has been renamed/
    );

    expect(
      await context.getBucketData('global[]'),
      'Renaming the source table should not delete data already replicated to clients'
    ).toMatchObject([putOp('test_data', testData)]);
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
    expect(tableBefore.pinnedCaptureObjectId, 'The initial source-table binding should be pinned').not.toBeNull();
    const pinnedCaptureObjectId = tableBefore.pinnedCaptureObjectId;
    expect(
      tableBefore.captureInstance?.objectId,
      'The active capture instance should match the persisted capture-instance pin'
    ).toBe(pinnedCaptureObjectId);
    const sourceTableIds = tableBefore.sourceTables.map((table) => table.id.toString());

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    const warnSpy = vi.spyOn(logger, 'warn');
    await connectionManager.query(
      `ALTER TABLE ${toQualifiedTableName(connectionManager.schema, 'test_data')} ADD new_column INT`
    );
    await enableCDCForTable({ connectionManager, table: 'test_data', captureInstance: 'capture_instance_new' });
    await expectedSchemaChange(schemaSpy, SchemaChangeType.NEW_CAPTURE_INSTANCE);
    await expectedWarning(warnSpy, /newer CDC capture instance/);

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
      expect(tableBefore.pinnedCaptureObjectId, 'The initial source-table binding should be pinned').not.toBeNull();
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
      expect(tableBefore.pinnedCaptureObjectId, 'The initial source-table binding should be pinned').not.toBeNull();
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

  /**
   * Bindings created before capture-instance pinning have no source metadata. They must be pinned in
   * place on the next job - to the instance the pre-pinning streaming logic would have selected -
   * without replacing the SourceTable records or re-snapshotting.
   *
   * The metadata is stripped through the storage API rather than per-backend SQL, so this covers
   * every storage backend the suite runs against.
   */
  test('A legacy binding without source metadata is pinned on the next job', async () => {
    let sourceDescriptor: SourceEntityDescriptor;
    let initialBinding: {
      sourceTableObjectId: number;
      pinnedCaptureObjectId: number | null;
      sourceTableIds: string[];
    };

    {
      await using context = await CDCStreamTestContext.open(factory);
      await context.updateSyncRules(BASIC_SYNC_RULES);
      const { connectionManager } = context;

      await createTestTableWithBasicId(connectionManager, 'test_data');
      const beforeLSN = await getLatestLSN(connectionManager);
      const testData = await insertBasicIdTestData(connectionManager, 'test_data');
      await waitForPendingCDCChanges(beforeLSN, connectionManager);

      await context.replicateSnapshot();
      await context.startStreaming();

      expect(await context.getBucketData('global[]')).toMatchObject([putOp('test_data', testData)]);

      const table = context.cdcStream.tableCache.getAll()[0];
      expect(table.pinnedCaptureObjectId, 'The binding should start out pinned').not.toBeNull();
      sourceDescriptor = table.ref;
      initialBinding = {
        sourceTableObjectId: table.objectId,
        pinnedCaptureObjectId: table.pinnedCaptureObjectId,
        sourceTableIds: table.sourceTables.map((sourceTable) => sourceTable.id.toString())
      };
    }

    {
      // Strip the persisted pin, reproducing a record written before pinning existed. Done in its
      // own context so it does not contend with a running stream for the storage batch.
      await using context = await CDCStreamTestContext.open(factory, { doNotClear: true });
      await context.loadActiveSyncRules();

      await using writer = await context.storage!.createWriter({
        ...test_utils.BATCH_OPTIONS,
        zeroLSN: LSN.ZERO,
        defaultSchema: context.connectionManager.schema,
        storeCurrentData: false
      });
      const resolved = await writer.resolveTables({
        connection_id: 1,
        source: sourceDescriptor,
        reconcileSourceTables: ({ candidates }) => ({
          compatibleTables: candidates.map((candidate) => candidate.withSourceMetadata(undefined)),
          incompatibleTables: [],
          newTableValues: {}
        })
      });

      expect(
        resolved.tables.map((table) => table.id.toString()),
        'Stripping the metadata should reuse the existing records'
      ).toEqual(initialBinding.sourceTableIds);
      expect(
        resolved.tables.map((table) => table.sourceMetadata),
        'The persisted pin should have been cleared'
      ).toEqual(resolved.tables.map(() => undefined));
    }

    {
      await using context = await CDCStreamTestContext.open(factory, { doNotClear: true });
      await context.loadActiveSyncRules();
      await context.replicateSnapshot();

      const table = context.cdcStream.tableCache.get(initialBinding.sourceTableObjectId)!;
      expect(table.pinnedCaptureObjectId, 'The legacy binding should be backfilled with a pin').toBe(
        initialBinding.pinnedCaptureObjectId
      );
      expect(table.captureInstance?.objectId, 'The stream should poll the capture instance it was just pinned to').toBe(
        initialBinding.pinnedCaptureObjectId
      );
      expect(
        table.sourceTables.map((sourceTable) => sourceTable.id.toString()),
        'Backfilling should update the existing records in place, not re-snapshot'
      ).toEqual(initialBinding.sourceTableIds);
    }
  });

  /**
   * Replication is ordered, so the job must not advance past a sync-config table it cannot
   * replicate - a checkpoint that silently omits the table would be worse than not progressing.
   * The job fails until CDC is enabled, then replicates normally.
   */
  test('A sync rule table without a capture instance blocks the job until CDC is enabled', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data', false);
    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');

    await expect(
      context.replicateSnapshot(),
      'Replication should not start while a sync config table has no capture instance'
    ).rejects.toThrow(/CDC is not enabled/);

    await enableCDCForTable({ connectionManager, table: 'test_data' });

    await context.replicateSnapshot();
    await context.startStreaming();

    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');

    const data = await context.getFinalBucketState('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);
  });

  /**
   * A pinned capture instance disappearing with no replacement leaves nothing to poll and nothing
   * to re-snapshot onto. Silently skipping the table would keep serving its now-stale records, so
   * the whole replication job fails instead.
   */
  test('Removing the last capture instance for a replicating table fails the job', async () => {
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
    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);

    await disableCDCForTable(connectionManager, 'test_data');

    await expect(
      context.streamingPromise,
      'Losing the last capture instance should stop the whole replication job'
    ).rejects.toThrow(/no replacement is available/);
  });

  /**
   * Wildcards would let a table enter scope while a stream is running, which cannot be detected
   * safely - so they are rejected outright rather than supported partially.
   */
  test('Table wildcards in the sync configuration are rejected', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data%"
`);
    const { connectionManager } = context;
    await createTestTableWithBasicId(connectionManager, 'test_data');

    await expect(
      context.replicateSnapshot(),
      'A table wildcard should stop replication, even where it matches an existing CDC-enabled table'
    ).rejects.toThrow(/wildcards/);
  });

  /**
   * A replacement capture instance may capture a different schema, so it is never adopted in place
   * even though one is available - that would silently change what is replicated. The job stops and
   * a new sync deploy is required.
   */
  test('A replacement for a dropped pinned capture instance is not adopted, and stops the job', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;

    await createTestTableWithBasicId(connectionManager, 'test_data');

    await context.replicateSnapshot();
    await context.startStreaming();

    const testData1 = await insertBasicIdTestData(connectionManager, 'test_data');
    const testData2 = await insertBasicIdTestData(connectionManager, 'test_data');
    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', testData1), putOp('test_data', testData2)]);

    const table = context.cdcStream.tableCache.getAll()[0];
    const originalCaptureInstanceName = table.captureInstance!.name;

    // Leave a replacement in place, so the failure is specifically about not adopting it rather
    // than about having nothing to poll.
    await enableCDCForTable({ connectionManager, table: 'test_data', captureInstance: 'capture_instance_new' });
    await disableCDCForTable(connectionManager, 'test_data', originalCaptureInstanceName);

    await expect(
      context.streamingPromise,
      'An available replacement should not keep the job alive - adopting it needs a new deploy'
    ).rejects.toThrow(/no longer available/);
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
    const warnSpy = vi.spyOn(logger, 'warn');
    await connectionManager.query(`ALTER TABLE test_data ADD new_column INT`);
    await expectedSchemaChange(schemaSpy, SchemaChangeType.TABLE_COLUMN_CHANGES);
    await expectedWarning(warnSpy, /Schema drift detected/);

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

/**
 * Wait for a warning matching `pattern`. Most schema changes are warn-only now, so this is the
 * assertion for "we told the user, and did nothing else".
 */
async function expectedWarning(warnSpy: any, pattern: RegExp) {
  try {
    await vi.waitFor(
      () =>
        expect(warnSpy.mock.calls.some(([message]: any[]) => typeof message == 'string' && pattern.test(message))).toBe(
          true
        ),
      { timeout: 55000 }
    );
  } catch (error) {
    throw new Error(`Test Assertion: Timeout waiting for a warning matching ${pattern}`);
  }
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
