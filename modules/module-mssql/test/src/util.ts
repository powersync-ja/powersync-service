import * as types from '@module/types/types.js';
import { logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory, InternalOpId, ReplicationCheckpoint, TestStorageFactory } from '@powersync/service-core';

import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';

import { describe, TestOptions } from 'vitest';
import { env } from './env.js';
import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';
import {
  createCheckpoint,
  enableCDCForTable,
  escapeIdentifier,
  getLatestLSN,
  toQualifiedTableName
} from '@module/utils/mssql.js';
import sql from 'mssql';
import { v4 as uuid } from 'uuid';
import { LSN } from '@module/common/LSN.js';

export const TEST_URI = env.MSSQL_TEST_URI;

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.test_utils.postgresTestStorageFactoryGenerator({
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
  uri: TEST_URI,
  additionalConfig: {
    pollingBatchSize: 10,
    pollingIntervalMs: 1000,
    trustServerCertificate: true
  }
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

export async function dropTestTable(connectionManager: MSSQLConnectionManager, tableName: string) {
  // await connectionManager.execute('sys.sp_cdc_disable_table', [
  //   { name: 'source_schema', value: connectionManager.schema },
  //   { name: 'source_name', value: tableName },
  //   { name: 'capture_instance', value: 'all' }
  // ]);
  await connectionManager.query(`DROP TABLE [${tableName}]`);
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

export async function createTestTable(connectionManager: MSSQLConnectionManager, tableName: string): Promise<void> {
  await connectionManager.query(`
    CREATE TABLE ${escapeIdentifier(connectionManager.schema)}.${escapeIdentifier(tableName)} (
      id UNIQUEIDENTIFIER PRIMARY KEY,
      description VARCHAR(MAX)
    )
  `);
  await enableCDCForTable({ connectionManager, table: tableName });
}

export async function createTestTableWithBasicId(
  connectionManager: MSSQLConnectionManager,
  tableName: string
): Promise<void> {
  await connectionManager.query(`
    CREATE TABLE ${escapeIdentifier(connectionManager.schema)}.${escapeIdentifier(tableName)} (
      id INT IDENTITY(1,1) PRIMARY KEY,
      description VARCHAR(MAX)
    )
  `);
  await enableCDCForTable({ connectionManager, table: tableName });
}

export interface TestData {
  id: string;
  description: string;
}
export async function insertTestData(connectionManager: MSSQLConnectionManager, tableName: string): Promise<TestData> {
  const id = createUpperCaseUUID();
  const description = `description_${id}`;
  await connectionManager.query(
    `
    INSERT INTO ${escapeIdentifier(connectionManager.schema)}.${escapeIdentifier(tableName)} (id, description) VALUES (@id, @description)
  `,
    [
      { name: 'id', type: sql.UniqueIdentifier, value: id },
      { name: 'description', type: sql.NVarChar(sql.MAX), value: description }
    ]
  );

  return { id, description };
}

export async function insertBasicIdTestData(
  connectionManager: MSSQLConnectionManager,
  tableName: string
): Promise<TestData> {
  const description = `description_${Math.floor(Math.random() * 1000000)}`;
  const { recordset: result } = await connectionManager.query(
    `
    INSERT INTO ${toQualifiedTableName(connectionManager.schema, tableName)} (description) 
    OUTPUT INSERTED.id 
    VALUES (@description)
    `,
    [{ name: 'description', type: sql.NVarChar(sql.MAX), value: description }]
  );
  const id = result[0].id;

  return { id, description };
}

export async function waitForPendingCDCChanges(
  beforeLSN: LSN,
  connectionManager: MSSQLConnectionManager
): Promise<void> {
  while (true) {
    const { recordset: result } = await connectionManager.query(
      `
    SELECT TOP 1 start_lsn
    FROM cdc.lsn_time_mapping
    WHERE start_lsn > @before_lsn
    ORDER BY start_lsn DESC
    `,
      [{ name: 'before_lsn', type: sql.VarBinary, value: beforeLSN.toBinary() }]
    );

    if (result.length === 0) {
      logger.info(`CDC changes pending. Waiting for 200ms...`);
      await new Promise((resolve) => setTimeout(resolve, 200));
    } else {
      logger.info(`Found LSN: ${LSN.fromBinary(result[0].start_lsn).toString()}`);
      return;
    }
  }
}

export async function getClientCheckpoint(
  connectionManager: MSSQLConnectionManager,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<InternalOpId> {
  const start = Date.now();

  const lsn = await getLatestLSN(connectionManager);
  await createCheckpoint(connectionManager);

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;
  let lastCp: ReplicationCheckpoint | null = null;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const storage = await storageFactory.getActiveStorage();
    const cp = await storage?.getCheckpoint();
    if (cp != null) {
      lastCp = cp;
      if (cp.lsn != null && cp.lsn >= lsn.toString()) {
        logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
        return cp.checkpoint;
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error(`Timeout while waiting for checkpoint ${lsn}. Last checkpoint: ${lastCp?.lsn}`);
}

/**
 *  Generates a new UUID string in uppercase for testing purposes to match the SQL Server UNIQUEIDENTIFIER format.
 */
export function createUpperCaseUUID(): string {
  return uuid().toUpperCase();
}
