import * as types from '@module/types/types.js';
import { getMySQLVersion, isVersionAtLeast } from '@module/utils/mysql-utils.js';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import mysqlPromise from 'mysql2/promise';
import { env } from './env.js';
import { describe, TestOptions } from 'vitest';
import { TestStorageFactory } from '@powersync/service-core';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';

export const TEST_URI = env.MYSQL_TEST_URI;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mysql',
  uri: TEST_URI
});

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.MongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.PostgresTestStorageFactoryGenerator({
  url: env.PG_STORAGE_TEST_URL
});

export function describeWithStorage(options: TestOptions, fn: (factory: TestStorageFactory) => void) {
  describe.skipIf(!env.TEST_MONGO_STORAGE)(`mongodb storage`, options, function () {
    fn(INITIALIZED_MONGO_STORAGE_FACTORY);
  });

  describe.skipIf(!env.TEST_POSTGRES_STORAGE)(`postgres storage`, options, function () {
    fn(INITIALIZED_POSTGRES_STORAGE_FACTORY);
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
