import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { BinLogListener, SchemaChange, SchemaChangeType } from '@module/replication/zongji/BinLogListener.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import {
  clearTestDb,
  createBinlogListener,
  createTestDb,
  TEST_CONNECTION_OPTIONS,
  TestBinLogEventHandler
} from './util.js';
import { v4 as uuid } from 'uuid';
import { getMySQLVersion, qualifiedMySQLTable, satisfiesVersion } from '@module/utils/mysql-utils.js';
import crypto from 'crypto';
import { TablePattern } from '@powersync/service-sync-rules';

describe('BinlogListener tests', () => {
  const MAX_QUEUE_CAPACITY_MB = 1;
  const BINLOG_LISTENER_CONNECTION_OPTIONS = {
    ...TEST_CONNECTION_OPTIONS,
    binlog_queue_memory_limit: MAX_QUEUE_CAPACITY_MB
  };

  let connectionManager: MySQLConnectionManager;
  let eventHandler: TestBinLogEventHandler;
  let binLogListener: BinLogListener;
  let isMySQL57: boolean = false;

  beforeAll(async () => {
    connectionManager = new MySQLConnectionManager(BINLOG_LISTENER_CONNECTION_OPTIONS, {});
    const connection = await connectionManager.getConnection();
    const version = await getMySQLVersion(connection);
    isMySQL57 = satisfiesVersion(version, '5.7.x');
    connection.release();
  });

  beforeEach(async () => {
    const connection = await connectionManager.getConnection();
    await clearTestDb(connection);
    await connectionManager.query(`CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY, description MEDIUMTEXT)`);
    connection.release();
    eventHandler = new TestBinLogEventHandler();
    binLogListener = await createBinlogListener({
      connectionManager,
      sourceTables: [new TablePattern(connectionManager.databaseName, 'test_DATA')],
      eventHandler
    });
  });

  afterAll(async () => {
    await connectionManager.end();
  });

  test('Stop binlog listener', async () => {
    const stopSpy = vi.spyOn(binLogListener.zongji, 'stop');
    const queueStopSpy = vi.spyOn(binLogListener.processingQueue, 'kill');

    await binLogListener.start();
    await binLogListener.stop();

    expect(stopSpy).toHaveBeenCalled();
    expect(queueStopSpy).toHaveBeenCalled();
  });

  test('Zongji listener is stopped when processing queue reaches maximum memory size', async () => {
    const stopSpy = vi.spyOn(binLogListener.zongji, 'stop');

    // Pause the event handler to force a backlog on the processing queue
    eventHandler.pause();

    const ROW_COUNT = 10;
    await insertRows(connectionManager, ROW_COUNT);

    await binLogListener.start();

    // Wait for listener to stop due to queue reaching capacity
    await vi.waitFor(() => expect(stopSpy).toHaveBeenCalled(), { timeout: 5000 });

    expect(binLogListener.isQueueOverCapacity()).toBeTruthy();
    // Resume event processing
    eventHandler.unpause!();
    const restartSpy = vi.spyOn(binLogListener, 'start');

    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    await binLogListener.stop();
    // Confirm resume was called after unpausing
    expect(restartSpy).toHaveBeenCalled();
  });

  test('Row events: Write, update, delete', async () => {
    await binLogListener.start();

    const ROW_COUNT = 10;
    await insertRows(connectionManager, ROW_COUNT);
    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    expect(eventHandler.commitCount).equals(ROW_COUNT);

    await updateRows(connectionManager);
    await vi.waitFor(() => expect(eventHandler.rowsUpdated).equals(ROW_COUNT), { timeout: 5000 });

    await deleteRows(connectionManager);
    await vi.waitFor(() => expect(eventHandler.rowsDeleted).equals(ROW_COUNT), { timeout: 5000 });

    await binLogListener.stop();
  });

  test('Keepalive event', async () => {
    binLogListener.options.keepAliveInactivitySeconds = 1;
    await binLogListener.start();
    await vi.waitFor(() => expect(eventHandler.lastKeepAlive).toBeDefined(), { timeout: 10000 });
    await binLogListener.stop();
    expect(eventHandler.lastKeepAlive).toEqual(binLogListener.options.startGTID.comparable);
  });

  test('Schema change event: Rename table', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA RENAME test_DATA_new`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.RENAME_TABLE,
      connectionManager.databaseName,
      'test_DATA',
      'test_DATA_new'
    );
  });

  test('Schema change event: Rename multiple tables', async () => {
    // RENAME TABLE supports renaming multiple tables in a single statement
    // We generate a schema change event for each table renamed
    await binLogListener.start();
    await connectionManager.query(`RENAME TABLE 
    test_DATA TO test_DATA_new,
    test_DATA_new TO test_DATA
    `);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(2), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.RENAME_TABLE,
      connectionManager.databaseName,
      'test_DATA'
    );
    // New table name is undefined since the renamed table is not included by the database filter
    expect(eventHandler.schemaChanges[0].newTable).toBeUndefined();

    assertSchemaChange(
      eventHandler.schemaChanges[1],
      SchemaChangeType.RENAME_TABLE,
      connectionManager.databaseName,
      'test_DATA_new',
      'test_DATA'
    );
  });

  test('Schema change event: Truncate table', async () => {
    await binLogListener.start();
    await connectionManager.query(`TRUNCATE TABLE test_DATA`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.TRUNCATE_TABLE,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Drop table', async () => {
    await binLogListener.start();
    await connectionManager.query(`DROP TABLE test_DATA`);
    await connectionManager.query(`CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY, description MEDIUMTEXT)`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.DROP_TABLE,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Drop column', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA DROP COLUMN description`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Add column', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA ADD COLUMN new_column VARCHAR(255)`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Modify column', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA MODIFY COLUMN description TEXT`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Rename column via change statement', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA CHANGE COLUMN description description_new MEDIUMTEXT`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Rename column via rename statement', async () => {
    // Syntax ALTER TABLE RENAME COLUMN was only introduced in MySQL 8.0.0
    if (!isMySQL57) {
      await binLogListener.start();
      await connectionManager.query(`ALTER TABLE test_DATA RENAME COLUMN description TO description_new`);
      await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
      await binLogListener.stop();
      assertSchemaChange(
        eventHandler.schemaChanges[0],
        SchemaChangeType.ALTER_TABLE_COLUMN,
        connectionManager.databaseName,
        'test_DATA'
      );
    }
  });

  test('Schema change event: Multiple column changes', async () => {
    // ALTER TABLE can have multiple column changes in a single statement
    await binLogListener.start();
    await connectionManager.query(
      `ALTER TABLE test_DATA DROP COLUMN description, ADD COLUMN new_description TEXT, MODIFY COLUMN id VARCHAR(50)`
    );
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(3), { timeout: 5000 });
    await binLogListener.stop();
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );

    assertSchemaChange(
      eventHandler.schemaChanges[1],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );

    assertSchemaChange(
      eventHandler.schemaChanges[2],
      SchemaChangeType.ALTER_TABLE_COLUMN,
      connectionManager.databaseName,
      'test_DATA'
    );
  });

  test('Schema change event: Drop and Add primary key', async () => {
    await connectionManager.query(`CREATE TABLE test_constraints (id CHAR(36), description VARCHAR(100))`);
    const sourceTables = [new TablePattern(connectionManager.databaseName, 'test_constraints')];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_constraints ADD PRIMARY KEY (id)`);
    await connectionManager.query(`ALTER TABLE test_constraints DROP PRIMARY KEY`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(2), { timeout: 5000 });
    await binLogListener.stop();
    // Event for the add
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.REPLICATION_IDENTITY,
      connectionManager.databaseName,
      'test_constraints'
    );
    // Event for the drop
    assertSchemaChange(
      eventHandler.schemaChanges[1],
      SchemaChangeType.REPLICATION_IDENTITY,
      connectionManager.databaseName,
      'test_constraints'
    );
  });

  test('Schema change event: Add and drop unique constraint', async () => {
    await connectionManager.query(`CREATE TABLE test_constraints (id CHAR(36), description VARCHAR(100))`);
    const sourceTables = [new TablePattern(connectionManager.databaseName, 'test_constraints')];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_constraints ADD UNIQUE (description)`);
    await connectionManager.query(`ALTER TABLE test_constraints DROP INDEX description`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(2), { timeout: 5000 });
    await binLogListener.stop();
    // Event for the creation
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.REPLICATION_IDENTITY,
      connectionManager.databaseName,
      'test_constraints'
    );
    // Event for the drop
    assertSchemaChange(
      eventHandler.schemaChanges[1],
      SchemaChangeType.REPLICATION_IDENTITY,
      connectionManager.databaseName,
      'test_constraints'
    );
  });

  test('Schema change event: Add and drop a unique index', async () => {
    await connectionManager.query(`CREATE TABLE test_constraints (id CHAR(36), description VARCHAR(100))`);
    const sourceTables = [new TablePattern(connectionManager.databaseName, 'test_constraints')];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });
    await binLogListener.start();
    await connectionManager.query(`CREATE UNIQUE INDEX description_idx ON test_constraints (description)`);
    await connectionManager.query(`DROP INDEX description_idx ON test_constraints`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(2), { timeout: 5000 });
    await binLogListener.stop();
    // Event for the creation
    assertSchemaChange(
      eventHandler.schemaChanges[0],
      SchemaChangeType.REPLICATION_IDENTITY,
      connectionManager.databaseName,
      'test_constraints'
    );
    // Event for the drop
    assertSchemaChange(
      eventHandler.schemaChanges[1],
      SchemaChangeType.REPLICATION_IDENTITY,
      connectionManager.databaseName,
      'test_constraints'
    );
  });

  test('Schema changes for non-matching tables are ignored', async () => {
    // TableFilter = only match 'test_DATA'
    await binLogListener.start();
    await connectionManager.query(`CREATE TABLE test_ignored (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`ALTER TABLE test_ignored ADD COLUMN new_column VARCHAR(10)`);
    await connectionManager.query(`DROP TABLE test_ignored`);

    // "Anchor" event to latch onto, ensuring that the schema change events have finished
    await insertRows(connectionManager, 1);
    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(1), { timeout: 5000 });
    await binLogListener.stop();

    expect(eventHandler.schemaChanges.length).toBe(0);
  });

  test('Sequential schema change handling', async () => {
    // If there are multiple schema changes in the binlog processing queue, we only restart the binlog listener once
    // all the schema changes have been processed
    const sourceTables = [new TablePattern(connectionManager.databaseName, 'test_multiple')];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });

    await connectionManager.query(`CREATE TABLE test_multiple (id CHAR(36), description VARCHAR(100))`);
    await connectionManager.query(`ALTER TABLE test_multiple ADD COLUMN new_column VARCHAR(10)`);
    await connectionManager.query(`ALTER TABLE test_multiple ADD PRIMARY KEY (id)`);
    await connectionManager.query(`ALTER TABLE test_multiple MODIFY COLUMN new_column TEXT`);
    await connectionManager.query(`DROP TABLE test_multiple`);

    await binLogListener.start();

    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(4), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.ALTER_TABLE_COLUMN);
    expect(eventHandler.schemaChanges[1].type).toBe(SchemaChangeType.REPLICATION_IDENTITY);
    expect(eventHandler.schemaChanges[2].type).toBe(SchemaChangeType.ALTER_TABLE_COLUMN);
    expect(eventHandler.schemaChanges[3].type).toBe(SchemaChangeType.DROP_TABLE);
  });

  test('Unprocessed binlog event received that does match the current table schema', async () => {
    // If we process a binlog event for a table which has since had its schema changed, we expect the binlog listener to stop with an error
    const sourceTables = [new TablePattern(connectionManager.databaseName, 'test_failure')];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });

    await connectionManager.query(`CREATE TABLE test_failure (id CHAR(36), description VARCHAR(100))`);
    await connectionManager.query(`INSERT INTO test_failure(id, description) VALUES('${uuid()}','test_failure')`);
    await connectionManager.query(`ALTER TABLE test_failure DROP COLUMN description`);

    await binLogListener.start();

    await expect(() => binLogListener.replicateUntilStopped()).rejects.toThrow(
      /that does not match its current schema/
    );
  });

  test('Unprocessed binlog event received for a dropped table', async () => {
    const sourceTables = [new TablePattern(connectionManager.databaseName, 'test_failure')];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });

    // If we process a binlog event for a table which has since been dropped, we expect the binlog listener to stop with an error
    await connectionManager.query(`CREATE TABLE test_failure (id CHAR(36), description VARCHAR(100))`);
    await connectionManager.query(`INSERT INTO test_failure(id, description) VALUES('${uuid()}','test_failure')`);
    await connectionManager.query(`DROP TABLE test_failure`);

    await binLogListener.start();

    await expect(() => binLogListener.replicateUntilStopped()).rejects.toThrow(/or the table has been dropped/);
  });

  test('Multi database events', async () => {
    await createTestDb(connectionManager, 'multi_schema');
    const testTable = qualifiedMySQLTable('test_DATA_multi', 'multi_schema');
    await connectionManager.query(`CREATE TABLE ${testTable} (id CHAR(36) PRIMARY KEY,description TEXT);`);

    const sourceTables = [
      new TablePattern(connectionManager.databaseName, 'test_DATA'),
      new TablePattern('multi_schema', 'test_DATA_multi')
    ];
    binLogListener = await createBinlogListener({
      connectionManager,
      eventHandler,
      sourceTables
    });
    await binLogListener.start();

    // Default database insert into test_DATA
    await insertRows(connectionManager, 1);
    // multi_schema database insert into test_DATA_multi
    await connectionManager.query(`INSERT INTO ${testTable}(id, description) VALUES('${uuid()}','test')`);
    await connectionManager.query(`DROP TABLE ${testTable}`);

    await vi.waitFor(() => expect(eventHandler.schemaChanges.length).toBe(1), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.rowsWritten).toBe(2);
    assertSchemaChange(eventHandler.schemaChanges[0], SchemaChangeType.DROP_TABLE, 'multi_schema', 'test_DATA_multi');
  });

  function assertSchemaChange(
    change: SchemaChange,
    type: SchemaChangeType,
    schema: string,
    table: string,
    newTable?: string
  ) {
    expect(change.type).toBe(type);
    expect(change.schema).toBe(schema);
    expect(change.table).toEqual(table);
    if (newTable) {
      expect(change.newTable).toEqual(newTable);
    }
  }
});

async function insertRows(connectionManager: MySQLConnectionManager, count: number) {
  for (let i = 0; i < count; i++) {
    await connectionManager.query(
      `INSERT INTO test_DATA(id, description) VALUES('${uuid()}','test${i} ${crypto.randomBytes(100_000).toString('hex')}')`
    );
  }
}

async function updateRows(connectionManager: MySQLConnectionManager) {
  await connectionManager.query(`UPDATE test_DATA SET description='updated'`);
}

async function deleteRows(connectionManager: MySQLConnectionManager) {
  await connectionManager.query(`DELETE FROM test_DATA`);
}
