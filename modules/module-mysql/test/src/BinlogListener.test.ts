import { describe, test, beforeEach, vi, expect, afterEach } from 'vitest';
import { BinlogEventHandler, BinlogListener, Row } from '@module/replication/zongji/BinlogListener.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';
import { v4 as uuid } from 'uuid';
import * as common from '@module/common/common-index.js';
import { createRandomServerId } from '@module/utils/mysql-utils.js';
import { TableMapEntry } from '@powersync/mysql-zongji';

describe('BinlogListener tests', () => {
  const MAX_QUEUE_SIZE = 10;
  const BINLOG_LISTENER_CONNECTION_OPTIONS = {
    ...TEST_CONNECTION_OPTIONS,
    max_binlog_queue_size: MAX_QUEUE_SIZE
  };

  let connectionManager: MySQLConnectionManager;
  let abortController: AbortController;
  let eventHandler: TestBinlogEventHandler;
  let binlogListener: BinlogListener;

  beforeEach(async () => {
    connectionManager = new MySQLConnectionManager(BINLOG_LISTENER_CONNECTION_OPTIONS, {});
    const connection = await connectionManager.getConnection();
    await clearTestDb(connection);
    await connection.query(`CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY, description text)`);
    connection.release();
    const fromGTID = await getFromGTID(connectionManager);

    abortController = new AbortController();
    eventHandler = new TestBinlogEventHandler();
    binlogListener = new BinlogListener({
      connectionManager: connectionManager,
      eventHandler: eventHandler,
      startPosition: fromGTID.position,
      includedTables: ['test_DATA'],
      serverId: createRandomServerId(1),
      abortSignal: abortController.signal
    });
  });

  afterEach(async () => {
    await connectionManager.end();
  });

  test('Binlog listener stops on abort signal', async () => {
    const stopSpy = vi.spyOn(binlogListener.zongji, 'stop');

    setTimeout(() => abortController.abort(), 10);
    await expect(binlogListener.start()).resolves.toBeUndefined();
    expect(stopSpy).toHaveBeenCalled();
  });

  test('Pause Zongji binlog listener when processing queue reaches max size', async () => {
    const pauseSpy = vi.spyOn(binlogListener.zongji, 'pause');
    const resumeSpy = vi.spyOn(binlogListener.zongji, 'resume');
    const queueSpy = vi.spyOn(binlogListener.processingQueue, 'length');

    const ROW_COUNT = 100;
    await insertRows(connectionManager, ROW_COUNT);

    const startPromise = binlogListener.start();

    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    abortController.abort();
    await expect(startPromise).resolves.toBeUndefined();

    // Count how many times the queue reached the max size. Consequently, we expect the listener to have paused and resumed that many times.
    const overThresholdCount = queueSpy.mock.results.map((r) => r.value).filter((v) => v === MAX_QUEUE_SIZE).length;
    expect(pauseSpy).toHaveBeenCalledTimes(overThresholdCount);
    expect(resumeSpy).toHaveBeenCalledTimes(overThresholdCount);
  });

  test('Binlog events are correctly forwarded to provided binlog events handler', async () => {
    const startPromise = binlogListener.start();

    const ROW_COUNT = 10;
    await insertRows(connectionManager, ROW_COUNT);
    await vi.waitFor(() => expect(eventHandler.rowsWritten).equals(ROW_COUNT), { timeout: 5000 });
    expect(eventHandler.commitCount).equals(ROW_COUNT);

    await updateRows(connectionManager);
    await vi.waitFor(() => expect(eventHandler.rowsUpdated).equals(ROW_COUNT), { timeout: 5000 });

    await deleteRows(connectionManager);
    await vi.waitFor(() => expect(eventHandler.rowsDeleted).equals(ROW_COUNT), { timeout: 5000 });

    abortController.abort();
    await expect(startPromise).resolves.toBeUndefined();
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
    await connectionManager.query(`INSERT INTO test_DATA(id, description) VALUES('${uuid()}','test${i}')`);
  }
}

async function updateRows(connectionManager: MySQLConnectionManager) {
  await connectionManager.query(`UPDATE test_DATA SET description='updated'`);
}

async function deleteRows(connectionManager: MySQLConnectionManager) {
  await connectionManager.query(`DELETE FROM test_DATA`);
}

class TestBinlogEventHandler implements BinlogEventHandler {
  rowsWritten = 0;
  rowsUpdated = 0;
  rowsDeleted = 0;
  commitCount = 0;

  async onWrite(rows: Row[], tableMap: TableMapEntry) {
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
}
