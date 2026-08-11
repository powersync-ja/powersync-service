import { LSN } from '@module/common/LSN.js';
import { MSSQLSourceTable } from '@module/common/MSSQLSourceTable.js';
import { CDCEventHandler, CDCPoller } from '@module/replication/CDCPoller.js';
import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';
import {
  createCheckpoint,
  escapeIdentifier,
  getCaptureInstances,
  getLatestLSN,
  getLatestReplicatedLSN,
  toQualifiedTableName
} from '@module/utils/mssql.js';
import { getReplicationIdentityColumns } from '@module/utils/schema.js';
import { SourceTable } from '@powersync/service-core';
import timers from 'timers/promises';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { clearTestDb, enableCDCForTable, TEST_CONNECTION_OPTIONS, waitForPendingCDCChanges } from './util.js';

describe('CDCPoller tests', { timeout: 60_000 }, () => {
  let connectionManager: MSSQLConnectionManager;

  beforeEach(async () => {
    connectionManager = new MSSQLConnectionManager(TEST_CONNECTION_OPTIONS, {});
    await clearTestDb(connectionManager);
  });

  afterEach(async () => {
    await connectionManager.end();
  });

  test('Deferred updates are collapsed into a single UPDATE', async () => {
    const tableName = 'test_deferred_update';
    await createDeferredUpdateTestTable(connectionManager, tableName);
    const qualifiedName = toQualifiedTableName(connectionManager.schema, tableName);

    const beforeInsertLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`INSERT INTO ${qualifiedName} (id, code) VALUES (1, 'V1'), (2, 'V2')`);
    await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

    // Only replicate changes made after the initial insert.
    const startLSN = await getLatestReplicatedLSN(connectionManager);

    const beforeUpdateLSN = await getLatestLSN(connectionManager);
    // Updating a column with a unique index, using the actual column value creates deferred updates
    await connectionManager.query(`UPDATE ${qualifiedName} SET code = code + 'A'`);
    await waitForPendingCDCChanges(beforeUpdateLSN, connectionManager);

    const table = await resolveSourceTable(connectionManager, tableName);
    const { operations } = await collectChanges({
      connectionManager,
      tables: [table],
      startLSN,
      expectedOperationCount: 2
    });

    expect(operations).toMatchObject([
      { operation: 'update', rowBefore: { id: 1, code: 'V1' }, rowAfter: { id: 1, code: 'V1A' } },
      { operation: 'update', rowBefore: { id: 2, code: 'V2' }, rowAfter: { id: 2, code: 'V2A' } }
    ]);
  });

  test('Normal deletes and inserts in the same transaction are not collapsed into updates', async () => {
    const tableName = 'test_mixed_transaction';
    await createDeferredUpdateTestTable(connectionManager, tableName);
    const qualifiedName = toQualifiedTableName(connectionManager.schema, tableName);

    const beforeInsertLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`
      INSERT INTO ${qualifiedName} (id, code) VALUES (1, 'V1'), (2, 'V2')
    `);
    await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

    const startLSN = await getLatestReplicatedLSN(connectionManager);

    const beforeUpdateLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`
      BEGIN TRAN;
        DELETE FROM ${qualifiedName} WHERE id = 1;
        INSERT INTO ${qualifiedName} (id, code) VALUES (1, 'V1again');
      COMMIT;
    `);
    await waitForPendingCDCChanges(beforeUpdateLSN, connectionManager);

    const table = await resolveSourceTable(connectionManager, tableName);
    const { operations } = await collectChanges({
      connectionManager,
      tables: [table],
      startLSN,
      expectedOperationCount: 2
    });

    expect(operations).toMatchObject([
      { operation: 'delete', row: { id: 1, code: 'V1' } },
      { operation: 'insert', row: { id: 1, code: 'V1again' } }
    ]);
  });

  test('In place updates emit a single UPDATE with the before and after rows', async () => {
    const tableName = 'test_in_place_update';
    await createDeferredUpdateTestTable(connectionManager, tableName);
    const qualifiedName = toQualifiedTableName(connectionManager.schema, tableName);

    const beforeInsertLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`INSERT INTO ${qualifiedName} (id, code) VALUES (1, 'V1')`);
    await waitForPendingCDCChanges(beforeInsertLSN, connectionManager);

    const startLSN = await getLatestReplicatedLSN(connectionManager);

    const beforeUpdateLSN = await getLatestLSN(connectionManager);
    // The new value does not derive from the current one, so the row stays put.
    await connectionManager.query(`UPDATE ${qualifiedName} SET code = 'V9' WHERE id = 1`);
    await waitForPendingCDCChanges(beforeUpdateLSN, connectionManager);

    const table = await resolveSourceTable(connectionManager, tableName);
    const { operations } = await collectChanges({
      connectionManager,
      tables: [table],
      startLSN,
      expectedOperationCount: 1
    });

    expect(operations).toMatchObject([
      { operation: 'update', rowBefore: { id: 1, code: 'V1' }, rowAfter: { id: 1, code: 'V9' } }
    ]);
  });

  test('Committed transaction count covers transactions across all replicated tables', async () => {
    const firstTableName = 'test_transaction_count_first';
    const secondTableName = 'test_transaction_count_second';
    await createDeferredUpdateTestTable(connectionManager, firstTableName);
    await createDeferredUpdateTestTable(connectionManager, secondTableName);
    const firstQualifiedName = toQualifiedTableName(connectionManager.schema, firstTableName);
    const secondQualifiedName = toQualifiedTableName(connectionManager.schema, secondTableName);

    const startLSN = await getLatestReplicatedLSN(connectionManager);

    // Two separate transactions, each applying to only one of the tables. Previously this would have been undercounted as 1 transaction.
    await connectionManager.query(`INSERT INTO ${firstQualifiedName} (id, code) VALUES (1, 'A1')`);
    await connectionManager.query(`INSERT INTO ${secondQualifiedName} (id, code) VALUES (1, 'B1')`);

    // CDC captures transactions in commit order, so waiting for a checkpoint written after both
    // inserts guarantees both have been captured before polling starts. That in turn guarantees
    // the poller sees both within a single polling cycle, which is what is being asserted here.
    const beforeCheckpointLSN = await getLatestLSN(connectionManager);
    await createCheckpoint(connectionManager);
    await waitForPendingCDCChanges(beforeCheckpointLSN, connectionManager);

    const { operations, commits } = await collectChanges({
      connectionManager,
      tables: [
        await resolveSourceTable(connectionManager, firstTableName),
        await resolveSourceTable(connectionManager, secondTableName)
      ],
      startLSN,
      expectedOperationCount: 2
    });

    expect(operations).toMatchObject([
      { operation: 'insert', row: { id: 1, code: 'A1' } },
      { operation: 'insert', row: { id: 1, code: 'B1' } }
    ]);

    // The checkpoint table is not replicated here, so only the two inserts are counted.
    const transactionCount = commits.reduce((total, commit) => total + commit.transactionCount, 0);
    expect(transactionCount).toEqual(2);
  });
});

/**
 *  A change as it was handed to the CDCEventHandler. The CDC rows are kept as received, so that
 *  assertions can match on whichever source columns they care about.
 */
type RecordedOperation =
  | { operation: 'insert'; row: any }
  | { operation: 'delete'; row: any }
  | { operation: 'update'; rowBefore: any; rowAfter: any };

/**
 *  A commit as it was reported to the CDCEventHandler at the end of a polling cycle.
 */
interface RecordedCommit {
  lsn: string;
  transactionCount: number;
}

/**
 *  Records the operations emitted by the CDCPoller so that their relative order can be asserted.
 */
class RecordingCDCEventHandler implements CDCEventHandler {
  readonly operations: RecordedOperation[] = [];
  readonly commits: RecordedCommit[] = [];

  async onInsert(row: any): Promise<void> {
    this.operations.push({ operation: 'insert', row });
  }

  async onUpdate(rowAfter: any, rowBefore: any): Promise<void> {
    this.operations.push({ operation: 'update', rowBefore, rowAfter });
  }

  async onDelete(row: any): Promise<void> {
    this.operations.push({ operation: 'delete', row });
  }

  async onCommit(lsn: string, transactionCount: number): Promise<void> {
    this.commits.push({ lsn, transactionCount });
  }

  async onSchemaChange(): Promise<void> {}
}

/**
 *  How long to wait for the expected operations before returning whatever arrived.
 */
const COLLECT_CHANGES_TIMEOUT_MS = 20_000;

interface CollectChangesOptions {
  connectionManager: MSSQLConnectionManager;
  tables: MSSQLSourceTable[];
  startLSN: LSN;
  expectedOperationCount: number;
}

/**
 *  Runs a CDCPoller from startLSN until the expected number of operations has been emitted, and
 *  returns the handler holding the operations and the commits in the order they were reported.
 */
async function collectChanges(options: CollectChangesOptions): Promise<RecordingCDCEventHandler> {
  const { connectionManager, tables, startLSN, expectedOperationCount } = options;
  const eventHandler = new RecordingCDCEventHandler();

  const poller = new CDCPoller({
    connectionManager,
    eventHandler,
    getReplicatedTables: () => tables,
    startLSN,
    additionalConfig: {
      pollingBatchSize: 100,
      pollingIntervalMs: 50,
      trustServerCertificate: true
    },
    schemaCheckIntervalMs: 60_000
  });

  const pollerPromise = poller.replicateUntilStopped();
  try {
    const deadline = Date.now() + COLLECT_CHANGES_TIMEOUT_MS;
    while (eventHandler.operations.length < expectedOperationCount && Date.now() < deadline) {
      // Rethrows if the poller fails instead of waiting out the full timeout.
      await Promise.race([timers.setTimeout(50), pollerPromise]);
    }
  } finally {
    await poller.stop();
    await pollerPromise;
  }

  return eventHandler;
}

/**
 *  Creates a table with a clustered primary key and a unique index over a separate column.
 *  A unique index is one of the prerequisites for triggering deferred updates in SQL Server.
 */
async function createDeferredUpdateTestTable(
  connectionManager: MSSQLConnectionManager,
  tableName: string
): Promise<void> {
  const qualifiedName = toQualifiedTableName(connectionManager.schema, tableName);
  await connectionManager.query(`
    CREATE TABLE ${qualifiedName} (
      id INT NOT NULL PRIMARY KEY,
      code VARCHAR(100) NOT NULL
    )
  `);
  await connectionManager.query(
    `CREATE UNIQUE NONCLUSTERED INDEX ${escapeIdentifier(`UX_${tableName}_code`)} ON ${qualifiedName}(code)`
  );
  await enableCDCForTable({ connectionManager, table: tableName });
}

/**
 *  Builds the MSSQLSourceTable that the CDCPoller needs, including the persisted capture-instance binding
 *  that would normally be populated by source-table reconciliation.
 */
async function resolveSourceTable(
  connectionManager: MSSQLConnectionManager,
  tableName: string
): Promise<MSSQLSourceTable> {
  const captureInstances = await getCaptureInstances({
    connectionManager,
    table: { schema: connectionManager.schema, name: tableName }
  });
  const details = [...captureInstances.values()][0];
  if (details == null) {
    throw new Error(`No CDC capture instance found for table ${tableName}`);
  }

  const replicaIdColumnsResult = await getReplicationIdentityColumns({
    connectionManager,
    tableName,
    schema: connectionManager.schema
  });

  const ref = {
    connectionTag: connectionManager.connectionTag,
    objectId: details.sourceTable.objectId,
    schema: details.sourceTable.schema,
    name: details.sourceTable.name,
    replicaIdColumns: replicaIdColumnsResult.columns
  };
  const sourceTable = new SourceTable({
    id: `${details.sourceTable.objectId}`,
    ref,
    objectId: details.sourceTable.objectId,
    replicaIdColumns: replicaIdColumnsResult.columns,
    snapshotComplete: true,
    bucketDataSources: [],
    parameterLookupSources: [],
    sourceMetadata: { captureTableObjectId: details.instances[0].objectId }
  });
  const table = new MSSQLSourceTable(ref, [sourceTable]);
  table.setCaptureInstance(details.instances);
  return table;
}
