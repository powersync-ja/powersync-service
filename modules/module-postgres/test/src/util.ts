import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import * as types from '@module/types/types.js';
import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory, OpId } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { env } from './env.js';

export const TEST_URI = env.PG_TEST_URL;

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.MongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.PostgresTestStorageFactoryGenerator({
  url: env.PG_STORAGE_TEST_URL
});

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'postgresql',
  uri: TEST_URI,
  sslmode: 'disable'
});

export async function clearTestDb(db: pgwire.PgClient) {
  await db.query(
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots where active = false and slot_name like 'test_%'"
  );

  await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
  try {
    await db.query(`DROP PUBLICATION powersync`);
  } catch (e) {
    // Ignore
  }

  await db.query(`CREATE PUBLICATION powersync FOR ALL TABLES`);

  const tableRows = pgwire.pgwireRows(
    await db.query(`SELECT table_name FROM information_schema.tables where table_schema = 'public'`)
  );
  for (let row of tableRows) {
    const name = row.table_name;
    if (name.startsWith('test_')) {
      await db.query(`DROP TABLE public.${lib_postgres.escapeIdentifier(name)}`);
    }
  }
}

export async function connectPgWire(type?: 'replication' | 'standard') {
  const db = await pgwire.connectPgWire(TEST_CONNECTION_OPTIONS, { type });
  return db;
}

export function connectPgPool() {
  const db = pgwire.connectPgWirePool(TEST_CONNECTION_OPTIONS);
  return db;
}

export async function getClientCheckpoint(
  db: pgwire.PgClient,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<OpId> {
  const start = Date.now();

  const api = new PostgresRouteAPIAdapter(db);
  const lsn = await api.createReplicationHead(async (lsn) => lsn);

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const storage = await storageFactory.getActiveStorage();
    const cp = await storage?.getCheckpoint();
    if (cp == null) {
      throw new Error('No sync rules available');
    }
    if (cp.lsn && cp.lsn >= lsn) {
      logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
      return cp.checkpoint;
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error('Timeout while waiting for checkpoint');
}
