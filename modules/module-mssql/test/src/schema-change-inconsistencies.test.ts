import { SchemaChangeType } from '@module/replication/CDCPoller.js';
import { getLatestLSN, toQualifiedTableName } from '@module/utils/mssql.js';
import { OplogEntry, storage } from '@powersync/service-core';
import { putOp } from '@powersync/service-core-tests';
import sql from 'mssql';
import { describe, expect, test, vi } from 'vitest';

import { CDCStreamTestContext } from './CDCStreamTestContext.js';
import {
  createTestTableWithBasicId,
  describeWithStorage,
  disableCDCForTable,
  dropTestTable,
  enableCDCForTable,
  insertBasicIdTestData,
  renameTable,
  TestData,
  waitForPendingCDCChanges
} from './util.js';

const STAR_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT * FROM "test_data"
`;

describe('MSSQL old schema-change inconsistency reproductions', () => {
  describeWithStorage({ timeout: 120_000 }, defineSchemaChangeInconsistencyTests);
});

function defineSchemaChangeInconsistencyTests(config: storage.TestStorageConfig) {
  const { factory } = config;

  test('delayed table admission exposes related rows before the new table is detected', async () => {
    await using context = await CDCStreamTestContext.open(factory, {
      cdcStreamOptions: { schemaCheckIntervalMs: 60_000 }
    });
    const { connectionManager } = context;

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_parent"
      - SELECT id, description FROM "test_late_child"
`);

    await createTestTableWithBasicId(connectionManager, 'test_parent');
    await context.replicateSnapshot();
    await context.startStreaming();

    // The first schema check has already completed. The new exact table will not be admitted until
    // the next metadata poll, but changes for test_parent continue to produce checkpoints.
    await createTestTableWithBasicId(connectionManager, 'test_late_child');
    const { recordsets } = await connectionManager.query(
      `
      SET XACT_ABORT ON;
      BEGIN TRANSACTION;

      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_late_child')} (description)
      OUTPUT INSERTED.id, INSERTED.description
      VALUES (@child_description);

      DECLARE @child_id INT = SCOPE_IDENTITY();

      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_parent')} (description)
      OUTPUT INSERTED.id, INSERTED.description
      VALUES (CONVERT(NVARCHAR(MAX), @child_id));

      COMMIT TRANSACTION;
      `,
      [{ name: 'child_description', type: sql.NVarChar(sql.MAX), value: 'late_child' }]
    );
    const resultSets = Object.values(recordsets);
    const child = resultSets[0][0];
    const parent = resultSets[1][0];

    const checkpointState = await context.getFinalBucketState('global[]');

    // The child and its referring parent committed in one source transaction. A checkpoint which
    // exposes the parent should therefore expose the child as well. The valid assertion would be:
    // expect(findRow(checkpointState, 'test_late_child', child.id)).toMatchObject(
    //   putOp('test_late_child', child)
    // );
    // It fails on old main because test_late_child has not been detected, while replication of the
    // same transaction through the already-known test_parent table is allowed to create a checkpoint.
    expect(findRow(checkpointState, 'test_parent', parent.id)).toMatchObject(putOp('test_parent', parent));
    expect(findRow(checkpointState, 'test_late_child', child.id)).toBeUndefined(); // this should not pass
  });

  test('restart can switch capture instances without restoring newly captured column values', async () => {
    let rowsWithNewColumn: Array<{ id: number; description: string; new_column: number }>;

    {
      await using context = await CDCStreamTestContext.open(factory, {
        cdcStreamOptions: { schemaCheckIntervalMs: 60_000 }
      });
      const { connectionManager } = context;
      await context.updateSyncRules(STAR_SYNC_RULES);

      await createTestTableWithBasicId(connectionManager, 'test_data');
      await insertBasicIdTestData(connectionManager, 'test_data');
      await context.replicateSnapshot();
      await context.startStreaming();
      await context.getFinalBucketState('global[]');

      await connectionManager.query(
        `ALTER TABLE ${toQualifiedTableName(connectionManager.schema, 'test_data')} ADD new_column INT`
      );
      await enableCDCForTable({
        connectionManager,
        table: 'test_data',
        captureInstance: 'capture_instance_new',
        capturedColumns: ['id', 'description', 'new_column']
      });

      const { recordset } = await connectionManager.query(
        `
        INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_data')} (description, new_column)
        OUTPUT INSERTED.id, INSERTED.description, INSERTED.new_column
        VALUES
          (@description1, @new_column1),
          (@description2, @new_column2)
        `,
        [
          { name: 'description1', type: sql.NVarChar(sql.MAX), value: 'before_restart_1' },
          { name: 'new_column1', type: sql.Int, value: 101 },
          { name: 'description2', type: sql.NVarChar(sql.MAX), value: 'before_restart_2' },
          { name: 'new_column2', type: sql.Int, value: 102 }
        ]
      );
      rowsWithNewColumn = recordset;

      const stateBeforeRestart = await context.getFinalBucketState('global[]');
      for (const row of rowsWithNewColumn) {
        expect(readRow(stateBeforeRestart, 'test_data', row.id)).toEqual({
          id: row.id,
          description: row.description
        });
      }
    }

    await using restartedContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
    await restartedContext.loadActiveSyncRules();
    await restartedContext.replicateSnapshot();
    await restartedContext.startStreaming();

    const { recordset: afterRestartRows } = await restartedContext.connectionManager.query(
      `
      INSERT INTO ${toQualifiedTableName(restartedContext.connectionManager.schema, 'test_data')}
        (description, new_column)
      OUTPUT INSERTED.id, INSERTED.description, INSERTED.new_column
      VALUES (@description, @new_column)
      `,
      [
        { name: 'description', type: sql.NVarChar(sql.MAX), value: 'after_restart' },
        { name: 'new_column', type: sql.Int, value: 103 }
      ]
    );
    const afterRestartRow = afterRestartRows[0];
    const stateAfterRestart = await restartedContext.getFinalBucketState('global[]');

    // This row was inserted after restart and includes new_column in the checkpoint, proving that
    // streaming has switched to the replacement capture instance before inspecting the older rows.
    expect(readRow(stateAfterRestart, 'test_data', afterRestartRow.id)).toEqual(afterRestartRow);

    for (const row of rowsWithNewColumn) {
      // These rows were committed after the replacement capture instance started capturing
      // new_column. Once that instance is adopted, a valid state must include the captured value,
      // either by replaying its history or by re-snapshotting the table. The valid assertion would be:
      // expect(readRow(stateAfterRestart, 'test_data', row.id)).toEqual(row);
      // It fails on old main because the persisted resume LSN is newer than the replacement
      // instance's minimum LSN, so startup switches instances without re-snapshotting rows that the
      // original capture shape had already stored without new_column.
      expect(readRow(stateAfterRestart, 'test_data', row.id)).toEqual({
        id: row.id,
        description: row.description
      });
    }
  });

  test.each(['drop', 'rename'] as const)(
    '%s ahead of replication lag exposes an item without its parent',
    async (schemaChange) => {
      await using context = await CDCStreamTestContext.open(factory, {
        cdcStreamOptions: {
          additionalConfig: {
            pollingBatchSize: 1,
            pollingIntervalMs: 100,
            trustServerCertificate: true
          },
          schemaCheckIntervalMs: 0
        }
      });
      const { connectionManager } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_parent"
      - SELECT id, parent_id, description FROM "test_item"
`);

      await createTestTableWithBasicId(connectionManager, 'test_parent');
      await connectionManager.query(`
      CREATE TABLE ${toQualifiedTableName(connectionManager.schema, 'test_item')} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        parent_id INT NOT NULL,
        description VARCHAR(MAX)
      )
    `);
      await enableCDCForTable({ connectionManager, table: 'test_item' });

      const { recordset: parentRows } = await connectionManager.query(
        `
      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_parent')} (description)
      OUTPUT INSERTED.id, INSERTED.description
      VALUES (@description)
      `,
        [{ name: 'description', type: sql.NVarChar(sql.MAX), value: 'related parent value' }]
      );
      const parent = parentRows[0];
      await context.replicateSnapshot();

      // Install gates before streamChanges() creates its long-lived writer. The first gate lets the
      // test execute the DDL after a known amount of lag has committed; the second preserves the
      // inconsistent checkpoint produced after that DDL is detected.
      const commitGates = pauseSourceChangeCommitsAroundSchemaChange(context, { pauseBeforeSchemaChange: true });

      // Create all replication lag before streaming starts. At the source these transactions are
      // ordered and consistent: add an item referring to the parent, update it while preserving that
      // reference, then delete it before the parent table is dropped or renamed.
      let beforeChange = await getLatestLSN(connectionManager);
      const { recordset } = await connectionManager.query(
        `
      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_item')} (parent_id, description)
      OUTPUT INSERTED.id, INSERTED.parent_id, INSERTED.description
      VALUES (@parent_id, @description)
      `,
        [
          { name: 'parent_id', type: sql.Int, value: parent.id },
          { name: 'description', type: sql.NVarChar(sql.MAX), value: 'item created before schema change' }
        ]
      );
      const item = recordset[0];
      await waitForPendingCDCChanges(beforeChange, connectionManager);

      beforeChange = await getLatestLSN(connectionManager);
      await connectionManager.query(
        `UPDATE ${toQualifiedTableName(connectionManager.schema, 'test_item')}
       SET description = @description WHERE id = @item_id`,
        [
          { name: 'description', type: sql.NVarChar(sql.MAX), value: 'item updated before schema change' },
          { name: 'item_id', type: sql.Int, value: item.id }
        ]
      );
      const updatedItem = { ...item, description: 'item updated before schema change' };
      await waitForPendingCDCChanges(beforeChange, connectionManager);

      // This final lagging transaction repairs the relationship before the source drops the parent.
      // Once all queued changes are replicated, the client state can converge to a consistent result.
      beforeChange = await getLatestLSN(connectionManager);
      await connectionManager.query(
        `DELETE FROM ${toQualifiedTableName(connectionManager.schema, 'test_item')} WHERE id = @item_id`,
        [{ name: 'item_id', type: sql.Int, value: item.id }]
      );
      await waitForPendingCDCChanges(beforeChange, connectionManager);

      // Do not await startup yet: the first lagging commit is also what marks replication as started,
      // and the before-schema gate deliberately pauses inside that commit after it reaches storage.
      const streamingStarted = context.startStreaming();

      // Wait until the item insert is committed, but keep its update and delete in the lag. At this
      // point the replicated checkpoint is still valid because both the item and parent are present.
      await commitGates.beforeSchemaChange.paused;
      try {
        const checkpoint = await context.storage!.getCheckpoint();
        expect(checkpoint).not.toBeNull();
        const stateBeforeDrop = await context.getFinalBucketStateAtCheckpoint('global[]', checkpoint!);
        const itemBeforeDrop = readRow(stateBeforeDrop, 'test_item', item.id);
        expect(itemBeforeDrop).toEqual(item);
        expect(readRow(stateBeforeDrop, 'test_parent', itemBeforeDrop.parent_id).description).toBe(parent.description);

        // Execute the later DDL while streaming is paused at the earlier item-insert position. The
        // poller's next schema check will detect it before reading the item update.
        if (schemaChange === 'drop') {
          await dropTestTable(connectionManager, 'test_parent');
        } else {
          await renameTable(connectionManager, 'test_parent', 'test_parent_ignored');
        }
      } finally {
        commitGates.beforeSchemaChange.release();
      }
      await streamingStarted;

      // The next lagging source-row commit contains the item update. Schema handling has already
      // removed the parent, while the queued item delete is still unprocessed.
      await commitGates.afterSchemaChange.paused;
      try {
        const checkpoint = await context.storage!.getCheckpoint();
        expect(checkpoint).not.toBeNull();
        const inconsistentState = await context.getFinalBucketStateAtCheckpoint('global[]', checkpoint!);

        // At the item-update source position, the parent still existed. A valid checkpoint containing
        // the referring item must resolve parent_id to the parent's related string value. The valid
        // assertion would be:
        // const replicatedItem = readRow(inconsistentState, 'test_item', item.id);
        // expect(readRow(inconsistentState, 'test_parent', replicatedItem.parent_id).description).toBe(
        //   'related parent value'
        // );
        // It fails on old main because schema handling applies the later table removal immediately,
        // then publishes the older lagging item update before reaching the queued item delete.
        const replicatedItem = readRow(inconsistentState, 'test_item', item.id);
        expect(replicatedItem).toEqual(updatedItem); // The surviving item still carries its parent_id relationship.
        expect(findRow(inconsistentState, 'test_parent', replicatedItem.parent_id)).toBeUndefined(); // Failure: parent_id resolves to no replicated parent row.
      } finally {
        commitGates.afterSchemaChange.release();
      }

      const finalState = await context.getFinalBucketState('global[]');
      expect(findRow(finalState, 'test_parent', parent.id)).toBeUndefined();
      expect(findRow(finalState, 'test_item', item.id)).toBeUndefined();
    }
  );

  test('dropping a capture instance loses unread parent changes while related items advance', async () => {
    await using context = await CDCStreamTestContext.open(factory, {
      cdcStreamOptions: {
        additionalConfig: {
          pollingBatchSize: 1,
          pollingIntervalMs: 100,
          trustServerCertificate: true
        },
        schemaCheckIntervalMs: 0
      }
    });
    const { connectionManager } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_parent"
      - SELECT id, parent_id, expected_parent_description, status FROM "test_item"
`);

    await createTestTableWithBasicId(connectionManager, 'test_parent');
    await connectionManager.query(`
      CREATE TABLE ${toQualifiedTableName(connectionManager.schema, 'test_item')} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        parent_id INT NOT NULL,
        expected_parent_description VARCHAR(MAX),
        status VARCHAR(MAX)
      )
    `);
    await enableCDCForTable({ connectionManager, table: 'test_item' });

    const { recordset: parentRows } = await connectionManager.query(
      `
      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_parent')} (description)
      OUTPUT INSERTED.id, INSERTED.description
      VALUES (@description)
      `,
      [{ name: 'description', type: sql.NVarChar(sql.MAX), value: 'parent value before transition' }]
    );
    const parent = parentRows[0];

    const { recordset: itemRows } = await connectionManager.query(
      `
      INSERT INTO ${toQualifiedTableName(connectionManager.schema, 'test_item')}
        (parent_id, expected_parent_description, status)
      OUTPUT INSERTED.id, INSERTED.parent_id, INSERTED.expected_parent_description, INSERTED.status
      VALUES (@parent_id, @expected_parent_description, @status)
      `,
      [
        { name: 'parent_id', type: sql.Int, value: parent.id },
        {
          name: 'expected_parent_description',
          type: sql.NVarChar(sql.MAX),
          value: parent.description
        },
        { name: 'status', type: sql.NVarChar(sql.MAX), value: 'initial' }
      ]
    );
    const item = itemRows[0];
    await context.replicateSnapshot();

    // Create two unread source transactions before streaming. The first gives us a known, valid
    // checkpoint at which to remove CDC. The second changes both sides of the relationship atomically.
    let beforeChange = await getLatestLSN(connectionManager);
    await connectionManager.query(
      `UPDATE ${toQualifiedTableName(connectionManager.schema, 'test_item')}
       SET status = @status WHERE id = @item_id`,
      [
        { name: 'status', type: sql.NVarChar(sql.MAX), value: 'ready' },
        { name: 'item_id', type: sql.Int, value: item.id }
      ]
    );
    const readyItem = { ...item, status: 'ready' };
    await waitForPendingCDCChanges(beforeChange, connectionManager);

    beforeChange = await getLatestLSN(connectionManager);
    await connectionManager.query(
      `
      BEGIN TRANSACTION;
      UPDATE ${toQualifiedTableName(connectionManager.schema, 'test_parent')}
        SET description = @parent_description WHERE id = @parent_id;
      UPDATE ${toQualifiedTableName(connectionManager.schema, 'test_item')}
        SET expected_parent_description = @parent_description, status = @status WHERE id = @item_id;
      COMMIT TRANSACTION;
      `,
      [
        { name: 'parent_description', type: sql.NVarChar(sql.MAX), value: 'parent value after transition' },
        { name: 'status', type: sql.NVarChar(sql.MAX), value: 'transitioned' },
        { name: 'parent_id', type: sql.Int, value: parent.id },
        { name: 'item_id', type: sql.Int, value: item.id }
      ]
    );
    const transitionedItem = {
      ...item,
      expected_parent_description: 'parent value after transition',
      status: 'transitioned'
    };
    await waitForPendingCDCChanges(beforeChange, connectionManager);

    const commitGates = pauseSourceChangeCommitsAroundSchemaChange(context, { pauseBeforeSchemaChange: true });
    const streamingStarted = context.startStreaming();

    // The first item-only transaction is committed normally. Its foreign-key expectation still
    // matches the parent row, proving that the checkpoint before CDC removal is valid.
    await commitGates.beforeSchemaChange.paused;
    try {
      const checkpoint = await context.storage!.getCheckpoint();
      expect(checkpoint).not.toBeNull();
      const validState = await context.getFinalBucketStateAtCheckpoint('global[]', checkpoint!);
      const replicatedItem = readRow(validState, 'test_item', item.id);
      expect(replicatedItem).toEqual(readyItem);
      expect(readRow(validState, 'test_parent', replicatedItem.parent_id).description).toBe(
        replicatedItem.expected_parent_description
      );

      // Remove only the parent's capture instance. The source table and its current value remain,
      // but its unread half of the following parent+item transaction is now permanently unavailable.
      await disableCDCForTable(connectionManager, 'test_parent');
    } finally {
      commitGates.beforeSchemaChange.release();
    }
    await streamingStarted;

    // Old main detects the missing capture instance, skips the parent, then commits the item update
    // and the batch end LSN. This exposes a checkpoint beyond the lost parent update.
    await commitGates.afterSchemaChange.paused;
    try {
      const checkpoint = await context.storage!.getCheckpoint();
      expect(checkpoint).not.toBeNull();
      const inconsistentState = await context.getFinalBucketStateAtCheckpoint('global[]', checkpoint!);
      const replicatedItem = readRow(inconsistentState, 'test_item', item.id);
      const replicatedParent = readRow(inconsistentState, 'test_parent', replicatedItem.parent_id);

      // Both source updates committed in one transaction. A valid checkpoint containing the item
      // transition must contain the matching parent value. The valid assertion would be:
      // expect(replicatedParent.description).toBe(replicatedItem.expected_parent_description);
      // It fails because the parent's CDC history was dropped, while the surviving item capture
      // instance was allowed to advance the shared stream checkpoint past that transaction.
      expect(replicatedItem).toEqual(transitionedItem); // Proves the shared transaction LSN was committed.
      expect(replicatedParent.description).not.toBe(replicatedItem.expected_parent_description); // Failure: the related parent is stale.
    } finally {
      commitGates.afterSchemaChange.release();
    }
  });

  test.each(['drop', 'rename'] as const)(
    '%s of an exact table while stopped leaves its previously replicated data behind',
    async (schemaChange) => {
      let original!: TestData;

      {
        await using context = await CDCStreamTestContext.open(factory);
        const { connectionManager } = context;
        await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`);

        await createTestTableWithBasicId(connectionManager, 'test_data');
        original = await insertBasicIdTestData(connectionManager, 'test_data');
        await context.initializeReplication();
        expect(await context.getFinalBucketState('global[]')).toMatchObject([putOp('test_data', original)]);
      }

      await using restartedContext = await CDCStreamTestContext.open(factory, { doNotClear: true });
      if (schemaChange === 'drop') {
        await dropTestTable(restartedContext.connectionManager, 'test_data');
      } else {
        await renameTable(restartedContext.connectionManager, 'test_data', 'test_data_renamed');
      }

      await restartedContext.loadActiveSyncRules();
      await restartedContext.replicateSnapshot();
      await restartedContext.startStreaming();

      const staleState = await restartedContext.getFinalBucketState('global[]');

      // If restart is allowed to publish a checkpoint after discovering that the configured source
      // table is gone, that checkpoint must remove the table's previously replicated rows. The valid
      // assertion would be:
      // expect(findRow(staleState, 'test_data', original.id)).toBeUndefined();
      // It fails on old main because startup only resolves currently discoverable CDC tables. It does
      // not reconcile the persisted mapping for the table that disappeared while replication was
      // stopped, so the old row remains client-visible.
      expect(findRow(staleState, 'test_data', original.id)).toMatchObject(putOp('test_data', original));
    }
  );

  test('drop and recreate between metadata polls is observed in reverse order', async () => {
    await using context = await CDCStreamTestContext.open(factory, {
      cdcStreamOptions: { schemaCheckIntervalMs: 0 }
    });
    const { connectionManager } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`);

    await createTestTableWithBasicId(connectionManager, 'test_data');
    const oldRow = await insertBasicIdTestData(connectionManager, 'test_data');
    await context.replicateSnapshot();

    await dropTestTable(connectionManager, 'test_data');
    await createTestTableWithBasicId(connectionManager, 'test_data');
    const replacementRow = await insertBasicIdTestData(connectionManager, 'test_data');

    const schemaSpy = vi.spyOn(context.cdcStream, 'handleSchemaChange');
    await context.startStreaming();

    const finalState = await context.getFinalBucketState('global[]');
    const observedTypes = schemaSpy.mock.calls.map((call) => call[1].type);

    // The recreated table has the same logical name but a different SQL Server object identity. A
    // valid metadata transition must retire the old identity before admitting the replacement. The
    // valid assertion would be:
    // expect(observedTypes.slice(0, 2)).toEqual([SchemaChangeType.TABLE_DROP, SchemaChangeType.TABLE_CREATE]);
    // It fails on old main because schema polling checks for new capture instances before checking
    // whether cached tables still exist, so it reports the replacement CREATE before the old DROP.
    expect(observedTypes.slice(0, 2)).toEqual([SchemaChangeType.TABLE_CREATE, SchemaChangeType.TABLE_DROP]);
    expect(readRow(finalState, 'test_data', replacementRow.id).description).toBe(replacementRow.description);
    expect(readRow(finalState, 'test_data', oldRow.id).description).not.toBe(oldRow.description);
  });
}

function findRow(state: OplogEntry[], table: string, id: string | number): OplogEntry | undefined {
  return state.find(
    (operation) => operation.op === 'PUT' && operation.object_type === table && operation.object_id === String(id)
  );
}

function readRow(state: OplogEntry[], table: string, id: string | number): Record<string, any> {
  const operation = findRow(state, table, id);
  expect(operation, `${table}[${id}] should be present`).toBeDefined();
  return JSON.parse(operation!.data!);
}

/**
 * Provides deterministic commit boundaries immediately before and after a schema change.
 *
 * The tests create CDC lag before streaming begins. This wrapper preserves the real SQL Server
 * polling and storage writes, but pauses the long-lived streaming batch at two useful boundaries:
 *
 * 1. When `pauseBeforeSchemaChange` is enabled, `beforeSchemaChange` pauses after the first lagging
 *    source-row commit. A test can apply DDL at that known replication position while later CDC
 *    transactions remain unread.
 * 2. `handleSchemaChange()` then runs normally, including any queued table removal.
 * 3. `afterSchemaChange` pauses after the first subsequent source-row commit. A test can inspect the
 *    exact client-visible checkpoint before the next lagging transaction restores consistency.
 *
 * Both gates pause only after `batch.commit()` completes. They therefore inspect real committed
 * checkpoints rather than pending writer state. Requiring a `batch.save()` also avoids pausing on an
 * unrelated SQL Server LSN that contains no replicated row change.
 */
function pauseSourceChangeCommitsAroundSchemaChange(
  context: CDCStreamTestContext,
  gateOptions?: { pauseBeforeSchemaChange?: boolean }
) {
  let schemaChangeHandled = false;
  let sourceChangeHandled = false;
  let pausedBeforeSchemaChange = false;
  let pausedAfterSchemaChange = false;
  const beforeSchemaChange = createPauseGate();
  const afterSchemaChange = createPauseGate();

  const originalHandleSchemaChange = context.cdcStream.handleSchemaChange.bind(context.cdcStream);
  vi.spyOn(context.cdcStream, 'handleSchemaChange').mockImplementation(async (batch, change) => {
    // Run the real handler first so table drops, renames, and source checkpoints are persisted exactly
    // as they are in production. The flag only records that subsequent row writes occur after it.
    await originalHandleSchemaChange(batch, change);
    schemaChangeHandled = true;
  });

  const bucketStorage = context.storage!;
  const originalStartBatch = bucketStorage.startBatch.bind(bucketStorage);

  // Capture the long-lived streaming batch created after this helper is installed. Snapshot batches
  // have already completed at both call sites, so this does not alter snapshot behavior.
  vi.spyOn(bucketStorage, 'startBatch').mockImplementation((options, callback) =>
    originalStartBatch(options, async (batch) => {
      const originalSave = batch.save.bind(batch);
      vi.spyOn(batch, 'save').mockImplementation(async (record) => {
        // Save through the real writer, then remember that this commit will contain an actual source
        // row from the lag rather than only schema metadata or an unrelated LSN.
        const result = await originalSave(record);
        sourceChangeHandled = true;
        return result;
      });

      const originalCommit = batch.commit.bind(batch);
      vi.spyOn(batch, 'commit').mockImplementation(async (lsn, commitOptions) => {
        // Commit first so the checkpoint inspected by the test is genuinely client-visible. Pause
        // only afterward, preventing the next CDC transaction from immediately replacing it.
        const result = await originalCommit(lsn, commitOptions);
        if (sourceChangeHandled) {
          sourceChangeHandled = false;
          if (!schemaChangeHandled && gateOptions?.pauseBeforeSchemaChange && !pausedBeforeSchemaChange) {
            pausedBeforeSchemaChange = true;
            await beforeSchemaChange.pause();
          } else if (schemaChangeHandled && !pausedAfterSchemaChange) {
            pausedAfterSchemaChange = true;
            await afterSchemaChange.pause();
          }
        }
        return result;
      });
      await callback(batch);
    })
  );

  return {
    beforeSchemaChange: beforeSchemaChange.publicGate,
    afterSchemaChange: afterSchemaChange.publicGate
  };
}

function createPauseGate() {
  let resolvePaused!: () => void;
  let resolveRelease!: () => void;
  const paused = new Promise<void>((resolve) => (resolvePaused = resolve));
  const released = new Promise<void>((resolve) => (resolveRelease = resolve));

  return {
    pause: async () => {
      resolvePaused();
      await released;
    },
    publicGate: {
      paused,
      release: resolveRelease
    }
  };
}
