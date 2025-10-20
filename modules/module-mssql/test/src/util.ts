import * as types from '@module/types/types.js';
import { logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory, InternalOpId, ReplicationCheckpoint, TestStorageFactory } from '@powersync/service-core';

import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';

import { describe, TestOptions } from 'vitest';
import { env } from './env.js';
import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';
import { getLatestLSN } from '@module/utils/mssql.js';
import sql from 'mssql';

export const TEST_URI = env.MSSQL_TEST_URI;

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

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mssql',
  uri: TEST_URI
});

/**
 *  Clears all test tables (those prefixed with 'test_') from the database. Also removes CDC instances for those tables.
 * @param connectionManager
 */
export async function clearTestDb(connectionManager: MSSQLConnectionManager) {
  const { recordset: tables } = await connectionManager.query(`
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE 'test_%'
    `);
  for (const row of tables) {
    // Disable CDC for the table if enabled
    await connectionManager.execute('sys.sp_cdc_disable_table', [
      { name: 'source_schema', value: row.TABLE_SCHEMA },
      { name: 'source_name', value: row.TABLE_NAME },
      { name: 'capture_instance', value: 'all' }
    ]);
    // Drop Tables
    await connectionManager.query(`DROP TABLE [${row.TABLE_NAME}]`);
  }
}

/**
 *  Create a new database for testing and enables CDC on it.
 *  @param connectionManager
 *  @param dbName
 */
export async function createTestDb(connectionManager: MSSQLConnectionManager, dbName: string) {
  await connectionManager.query(`DROP DATABASE IF EXISTS ${dbName}`);
  await connectionManager.query(`CREATE DATABASE ${dbName}`);
  await connectionManager.execute(`
    USE ${dbName};
    GO

    EXEC sys.sp_cdc_enable_db;
    GO`);
}

export interface EnableCDCForTableOptions {
  connectionManager: MSSQLConnectionManager;
  schema: string;
  table: string;
}

export async function enableCDCForTable(options: EnableCDCForTableOptions): Promise<void> {
  const { connectionManager, schema, table } = options;

  await connectionManager.execute('sys.sp_cdc_enable_table', [
    { name: 'source_schema', value: schema },
    { name: 'source_name', value: table },
    { name: 'role_name', value: 'NULL' },
    { name: 'supports_net_changes', value: 1 }
  ]);
}

export async function getClientCheckpoint(
  connectionManager: MSSQLConnectionManager,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<InternalOpId> {
  const start = Date.now();

  const lsn = await getLatestLSN(connectionManager);

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;
  let lastCp: ReplicationCheckpoint | null = null;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const storage = await storageFactory.getActiveStorage();
    const cp = await storage?.getCheckpoint();
    if (cp == null) {
      throw new Error('No sync rules available');
    }
    lastCp = cp;
    if (cp.lsn != null && cp.lsn >= lsn.toString()) {
      logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
      return cp.checkpoint;
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error(`Timeout while waiting for checkpoint ${lsn}. Last checkpoint: ${lastCp?.lsn}`);
}
