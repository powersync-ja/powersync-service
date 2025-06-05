import { describe, test, beforeEach, vi, expect, afterEach } from 'vitest';
import { BinLogEventHandler, BinLogListener, Row } from '@module/replication/zongji/BinLogListener.js';
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
      includedTables: ['test_DATA', 'test_DATA_new'],
      serverId: createRandomServerId(1)
    });
  });

  afterEach(async () => {
    await connectionManager.end();
  });

  test('Stop binlog listener', async () => {
    const stopSpy = vi.spyOn(binLogListener.zongji, 'stop');
    const queueStopSpy = vi.spyOn(binLogListener.processingQueue, 'kill');

    const startPromise = binLogListener.start();
    setTimeout(async () => binLogListener.stop(), 50);

    await expect(startPromise).resolves.toBeUndefined();
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

    const startPromise = binLogListener.start();

    // Wait for listener to pause due to queue reaching capacity
    await vi.waitFor(() => expect(pauseSpy).toHaveBeenCalled(), { timeout: 5000 });

    expect(binLogListener.isQueueOverCapacity()).toBeTruthy();
    // Resume event processing
    eventHandler.unpause!();

    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    binLogListener.stop();
    await expect(startPromise).resolves.toBeUndefined();
    // Confirm resume was called after unpausing
    expect(resumeSpy).toHaveBeenCalled();
  });

  test('Binlog row events are correctly forwarded to provided binlog events handler', async () => {
    const startPromise = binLogListener.start();

    const ROW_COUNT = 10;
    await insertRows(connectionManager, ROW_COUNT);
    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    expect(eventHandler.commitCount).equals(ROW_COUNT);

    await updateRows(connectionManager);
    await vi.waitFor(() => expect(eventHandler.rowsUpdated).equals(ROW_COUNT), { timeout: 5000 });

    await deleteRows(connectionManager);
    await vi.waitFor(() => expect(eventHandler.rowsDeleted).equals(ROW_COUNT), { timeout: 5000 });

    binLogListener.stop();
    await expect(startPromise).resolves.toBeUndefined();
  });

  test('Binlog schema change events are correctly forwarded to provided binlog events handler', async () => {
    const startPromise = binLogListener.start();
    await connectionManager.query(`RENAME TABLE test_DATA TO test_DATA_new`);
    // Table map events are only emitted before row events
    await connectionManager.query(
      `INSERT INTO test_DATA_new(id, description) VALUES('${uuid()}','test ${crypto.randomBytes(100).toString('hex')}')`
    );
    await vi.waitFor(() => expect(eventHandler.latestSchemaChange).toBeDefined(), { timeout: 5000 });
    binLogListener.stop();
    await expect(startPromise).resolves.toBeUndefined();
    expect(eventHandler.latestSchemaChange?.tableName).toEqual('test_DATA_new');
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
  latestSchemaChange: TableMapEntry | undefined;

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

  async onSchemaChange(tableMap: TableMapEntry) {
    this.latestSchemaChange = tableMap;
  }
}
