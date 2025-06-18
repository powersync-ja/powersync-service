import { describe, test, beforeEach, vi, expect, afterEach } from 'vitest';
import {
  BinLogEventHandler,
  BinLogListener,
  Row,
  SchemaChange,
  SchemaChangeType
} from '@module/replication/zongji/BinLogListener.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';
import { v4 as uuid } from 'uuid';
import * as common from '@module/common/common-index.js';
import { createRandomServerId } from '@module/utils/mysql-utils.js';
import { TableMapEntry } from '@powersync/mysql-zongji';
import crypto from 'crypto';

describe('BinlogListener tests', () => {
  const MAX_QUEUE_CAPACITY_MB = 1;
  const BINLOG_LISTENER_CONNECTION_OPTIONS = {
    ...TEST_CONNECTION_OPTIONS,
    binlog_queue_memory_limit: MAX_QUEUE_CAPACITY_MB
  };

  let connectionManager: MySQLConnectionManager;
  let eventHandler: TestBinLogEventHandler;
  let binLogListener: BinLogListener;

  beforeEach(async () => {
    connectionManager = new MySQLConnectionManager(BINLOG_LISTENER_CONNECTION_OPTIONS, {});
    const connection = await connectionManager.getConnection();
    await clearTestDb(connection);
    await connection.query(`CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY, description MEDIUMTEXT)`);
    connection.release();
    const fromGTID = await getFromGTID(connectionManager);

    eventHandler = new TestBinLogEventHandler();
    binLogListener = new BinLogListener({
      connectionManager: connectionManager,
      eventHandler: eventHandler,
      startPosition: fromGTID.position,
      includedTables: ['test_DATA'],
      serverId: createRandomServerId(1)
    });
  });

  afterEach(async () => {
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

  test('Pause Zongji binlog listener when processing queue reaches maximum memory size', async () => {
    const pauseSpy = vi.spyOn(binLogListener.zongji, 'pause');
    const resumeSpy = vi.spyOn(binLogListener.zongji, 'resume');

    // Pause the event handler to force a backlog on the processing queue
    eventHandler.pause();

    const ROW_COUNT = 10;
    await insertRows(connectionManager, ROW_COUNT);

    await binLogListener.start();

    // Wait for listener to pause due to queue reaching capacity
    await vi.waitFor(() => expect(pauseSpy).toHaveBeenCalled(), { timeout: 5000 });

    expect(binLogListener.isQueueOverCapacity()).toBeTruthy();
    // Resume event processing
    eventHandler.unpause!();

    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    await binLogListener.stop();
    // Confirm resume was called after unpausing
    expect(resumeSpy).toHaveBeenCalled();
  });

  test('Binlog row events are correctly forwarded to provided binlog events handler', async () => {
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

  test('ALTER TABLE RENAME schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA RENAME test_DATA_new`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.RENAME_TABLE);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].newTable).toEqual('test_DATA_new');
  });

  test('RENAME TABLE schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`RENAME TABLE test_DATA TO test_DATA_new`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.RENAME_TABLE);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].newTable).toEqual('test_DATA_new');
  });

  test('RENAME TABLE multipe table schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`RENAME TABLE 
    test_DATA TO test_DATA_new,
    test_DATA_new TO test_DATA
    `);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length == 2).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.RENAME_TABLE);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].newTable).toEqual('test_DATA_new');

    expect(eventHandler.schemaChanges[1].type).toBe(SchemaChangeType.RENAME_TABLE);
    expect(eventHandler.schemaChanges[1].table).toEqual('test_DATA_new');
    expect(eventHandler.schemaChanges[1].newTable).toEqual('test_DATA');
  });

  test('TRUNCATE TABLE schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`TRUNCATE TABLE test_DATA`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.TRUNCATE_TABLE);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
  });

  test('DROP AND CREATE TABLE schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`DROP TABLE test_DATA`);
    await connectionManager.query(`CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY, description MEDIUMTEXT)`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length === 2).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.DROP_TABLE);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[1].type).toBe(SchemaChangeType.CREATE_TABLE);
    expect(eventHandler.schemaChanges[1].table).toEqual('test_DATA');
  });

  test('ALTER TABLE DROP COLUMN schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA DROP COLUMN description`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.DROP_COLUMN);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].column?.column).toEqual('description');
  });

  test('ALTER TABLE ADD COLUMN schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA ADD COLUMN new_column VARCHAR(255)`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.ADD_COLUMN);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].column?.column).toEqual('new_column');
  });

  test('ALTER TABLE MODIFY COLUMN type schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA MODIFY COLUMN description TEXT`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.MODIFY_COLUMN);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].column?.column).toEqual('description');
  });

  test('ALTER TABLE CHANGE COLUMN column rename schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA CHANGE COLUMN description description_new MEDIUMTEXT`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.RENAME_COLUMN);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].column?.column).toEqual('description');
    expect(eventHandler.schemaChanges[0].column?.newColumn).toEqual('description_new');
  });

  test('ALTER TABLE RENAME COLUMN column rename schema changes', async () => {
    await binLogListener.start();
    await connectionManager.query(`ALTER TABLE test_DATA RENAME COLUMN description TO description_new`);
    await vi.waitFor(() => expect(eventHandler.schemaChanges.length > 0).toBeTruthy(), { timeout: 5000 });
    await binLogListener.stop();
    expect(eventHandler.schemaChanges[0].type).toBe(SchemaChangeType.RENAME_COLUMN);
    expect(eventHandler.schemaChanges[0].table).toEqual('test_DATA');
    expect(eventHandler.schemaChanges[0].column?.column).toEqual('description');
    expect(eventHandler.schemaChanges[0].column?.newColumn).toEqual('description_new');
  });
});

async function getFromGTID(connectionManager: MySQLConnectionManager) {
  const connection = await connectionManager.getConnection();
  const fromGTID = await common.readExecutedGtid(connection);
  connection.release();

  return fromGTID;
}

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

class TestBinLogEventHandler implements BinLogEventHandler {
  rowsWritten = 0;
  rowsUpdated = 0;
  rowsDeleted = 0;
  commitCount = 0;
  schemaChanges: SchemaChange[] = [];

  unpause: ((value: void | PromiseLike<void>) => void) | undefined;
  private pausedPromise: Promise<void> | undefined;

  pause() {
    this.pausedPromise = new Promise((resolve) => {
      this.unpause = resolve;
    });
  }

  async onWrite(rows: Row[], tableMap: TableMapEntry) {
    if (this.pausedPromise) {
      await this.pausedPromise;
    }
    this.rowsWritten = this.rowsWritten + rows.length;
  }

  async onUpdate(afterRows: Row[], beforeRows: Row[], tableMap: TableMapEntry) {
    this.rowsUpdated = this.rowsUpdated + afterRows.length;
  }

  async onDelete(rows: Row[], tableMap: TableMapEntry) {
    this.rowsDeleted = this.rowsDeleted + rows.length;
  }

  async onCommit(lsn: string) {
    this.commitCount++;
  }

  async onSchemaChange(change: SchemaChange) {
    this.schemaChanges.push(change);
  }
  async onTransactionStart(options: { timestamp: Date }) {}
  async onRotate() {}
}
