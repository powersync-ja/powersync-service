import * as common from '@module/common/common-index.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { BinLogEventHandler, BinLogListener, Row, SchemaChange } from '@module/replication/zongji/BinLogListener.js';
import * as types from '@module/types/types.js';
import { createRandomServerId, getMySQLVersion, isVersionAtLeast } from '@module/utils/mysql-utils.js';
import { TableMapEntry } from '@powersync/mysql-zongji';
import { TestStorageConfig } from '@powersync/service-core';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { TablePattern } from '@powersync/service-sync-rules';
import mysqlPromise from 'mysql2/promise';
import { describe, TestOptions } from 'vitest';
import { env } from './env.js';

export const TEST_URI = env.MYSQL_TEST_URI;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mysql',
  uri: TEST_URI
});

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.test_utils.postgresTestSetup({
  url: env.PG_STORAGE_TEST_URL
});

export function describeWithStorage(options: TestOptions, fn: (factory: TestStorageConfig) => void) {
  describe.skipIf(!env.TEST_MONGO_STORAGE)(`mongodb storage`, options, function () {
    return fn(INITIALIZED_MONGO_STORAGE_FACTORY);
  });

  describe.skipIf(!env.TEST_POSTGRES_STORAGE)(`postgres storage`, options, function () {
    return fn(INITIALIZED_POSTGRES_STORAGE_FACTORY);
  });
}

export async function clearTestDb(connection: mysqlPromise.Connection) {
  const version = await getMySQLVersion(connection);
  if (isVersionAtLeast(version, '8.4.0')) {
    await connection.query('RESET BINARY LOGS AND GTIDS');
  } else {
    await connection.query('RESET MASTER');
  }

  const [result] = await connection.query<mysqlPromise.RowDataPacket[]>(
    `SELECT TABLE_NAME FROM information_schema.tables
     WHERE TABLE_SCHEMA = '${TEST_CONNECTION_OPTIONS.database}'`
  );
  for (let row of result) {
    const name = row.TABLE_NAME;
    if (name.startsWith('test_')) {
      await connection.query(`DROP TABLE ${name}`);
    }
  }
}

export async function createTestDb(connectionManager: MySQLConnectionManager, dbName: string) {
  await connectionManager.query(`DROP DATABASE IF EXISTS ${dbName}`);
  await connectionManager.query(`CREATE DATABASE IF NOT EXISTS ${dbName}`);
}

export async function getFromGTID(connectionManager: MySQLConnectionManager) {
  const connection = await connectionManager.getConnection();
  const fromGTID = await common.readExecutedGtid(connection);
  connection.release();

  return fromGTID;
}

export interface CreateBinlogListenerParams {
  connectionManager: MySQLConnectionManager;
  eventHandler: BinLogEventHandler;
  sourceTables: TablePattern[];
  startGTID?: common.ReplicatedGTID;
}
export async function createBinlogListener(params: CreateBinlogListenerParams): Promise<BinLogListener> {
  let { connectionManager, eventHandler, sourceTables, startGTID } = params;

  if (!startGTID) {
    startGTID = await getFromGTID(connectionManager);
  }

  return new BinLogListener({
    connectionManager: connectionManager,
    eventHandler: eventHandler,
    startGTID: startGTID!,
    sourceTables: sourceTables,
    serverId: createRandomServerId(1)
  });
}

export class TestBinLogEventHandler implements BinLogEventHandler {
  rowsWritten = 0;
  rowsUpdated = 0;
  rowsDeleted = 0;
  commitCount = 0;
  schemaChanges: SchemaChange[] = [];
  lastKeepAlive: string | undefined;

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
  async onKeepAlive(lsn: string) {
    this.lastKeepAlive = lsn;
  }
}
