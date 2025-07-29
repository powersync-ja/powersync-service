import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { BinLogListener } from '@module/replication/zongji/BinLogListener.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { clearTestDb, getFromGTID, TEST_CONNECTION_OPTIONS, TestBinLogEventHandler } from './util.js';
import * as mysql_utils from '@module/utils/mysql-utils.js';
import { createRandomServerId } from '@module/utils/mysql-utils.js';
import { TablePattern } from '@powersync/service-sync-rules';
import {
  ensureKeepAliveConfiguration,
  getLastKeepAlive,
  KEEP_ALIVE_TABLE,
  pingKeepAlive,
  tearDownKeepAlive
} from '@module/common/keepalive.js';

describe('MySQL Binlog KeepAlive tests', () => {
  const BINLOG_LISTENER_CONNECTION_OPTIONS = {
    ...TEST_CONNECTION_OPTIONS,
    binlog_queue_memory_limit: 5
  };

  let connectionManager: MySQLConnectionManager;

  beforeAll(async () => {
    connectionManager = new MySQLConnectionManager(BINLOG_LISTENER_CONNECTION_OPTIONS, {});
  });

  beforeEach(async () => {
    const connection = await connectionManager.getConnection();
    await clearTestDb(connection);
    connection.release();
  });

  afterAll(async () => {
    await connectionManager.end();
  });

  test('EnsureKeepAliveConfiguration - Table is created when absent', async () => {
    const connection = await connectionManager.getConnection();
    const [results] = await mysql_utils.retriedQuery({
      connection: connection,
      query: `
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_NAME = '${KEEP_ALIVE_TABLE}'
      `
    });
    // Confirms table doesn't exist yet
    expect(results.length).toBe(0);
    await ensureKeepAliveConfiguration(connection);
    const lastKeepAlive = await getLastKeepAlive(connection);
    expect(lastKeepAlive).toBeDefined();
    connection.release();
  });

  test('EnsureKeepAliveConfiguration - Has no effect if table is already present', async () => {
    const connection = await connectionManager.getConnection();
    await ensureKeepAliveConfiguration(connection);
    const keepAliveBefore = await getLastKeepAlive(connection);

    expect(() => ensureKeepAliveConfiguration(connection)).not.toThrow();
    const keepAliveAfter = await getLastKeepAlive(connection);
    expect(keepAliveBefore).toEqual(keepAliveAfter);
    connection.release();
  });

  test('PingKeepAlive - Updates last keepalive timestamp', async () => {
    const connection = await connectionManager.getConnection();
    await ensureKeepAliveConfiguration(connection);
    const keepAliveBefore = await getLastKeepAlive(connection);
    expect(keepAliveBefore).toBeDefined();

    await pingKeepAlive(connection);
    const keepAliveAfter = await getLastKeepAlive(connection);
    expect(keepAliveAfter).toBeDefined();

    expect(keepAliveBefore! < keepAliveAfter!);
    connection.release();
  });

  test('PingKeepAlive - Results in a binlog row update event', async () => {
    const connection = await connectionManager.getConnection();
    await ensureKeepAliveConfiguration(connection);

    const sourceTables = [new TablePattern(connectionManager.databaseName, KEEP_ALIVE_TABLE)];
    const fromGTID = await getFromGTID(connectionManager);
    const eventHandler = new TestBinLogEventHandler();
    const binLogListener = new BinLogListener({
      connectionManager: connectionManager,
      eventHandler: eventHandler,
      startPosition: fromGTID.position,
      sourceTables: sourceTables,
      serverId: createRandomServerId(1)
    });

    await binLogListener.start();

    await pingKeepAlive(connection);

    await vi.waitFor(() => expect(eventHandler.commitCount).toBe(1), { timeout: 5000 });
    await binLogListener.stop();

    expect(eventHandler.rowsUpdated).toBe(1);
    connection.release();
  });

  test('PingKeepAlive - Results in a commit event', async () => {
    // Even when not specifically listening for row events from the keepalive table,
    // we should still receive a commit event when a keepalive happens
    const connection = await connectionManager.getConnection();
    await ensureKeepAliveConfiguration(connection);

    const sourceTables: TablePattern[] = [];
    const fromGTID = await getFromGTID(connectionManager);
    const eventHandler = new TestBinLogEventHandler();
    const binLogListener = new BinLogListener({
      connectionManager: connectionManager,
      eventHandler: eventHandler,
      startPosition: fromGTID.position,
      sourceTables: sourceTables,
      serverId: createRandomServerId(1)
    });

    await binLogListener.start();

    await pingKeepAlive(connection);

    await vi.waitFor(() => expect(eventHandler.commitCount).toBe(1), { timeout: 5000 });
    await binLogListener.stop();

    expect(eventHandler.rowsUpdated).toBe(0);
    expect(eventHandler.commitCount).toBe(1);
    connection.release();
  });

  test('TearDownKeepAlive - Drops the KeepAlive table if present', async () => {
    const connection = await connectionManager.getConnection();

    // Ensure the table is created
    await ensureKeepAliveConfiguration(connection);

    await tearDownKeepAlive(connection);

    const [results] = await mysql_utils.retriedQuery({
      connection: connection,
      query: `
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_NAME = '${KEEP_ALIVE_TABLE}'
      `
    });
    // Confirms table doesn't exist anymore
    expect(results.length).toBe(0);

    connection.release();
  });
});
