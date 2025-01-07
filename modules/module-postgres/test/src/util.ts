import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import * as types from '@module/types/types.js';
import * as pg_utils from '@module/utils/pgwire_utils.js';
import { logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory, Metrics, OpId } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as pgwire from '@powersync/service-jpgwire';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import { env } from './env.js';

// The metrics need to be initialized before they can be used
await Metrics.initialise({
  disable_telemetry_sharing: true,
  powersync_instance_id: 'test',
  internal_metrics_endpoint: 'unused.for.tests.com'
});
Metrics.getInstance().resetCounters();

export const TEST_URI = env.PG_TEST_URL;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'postgresql',
  uri: TEST_URI,
  sslmode: 'disable'
});

export type StorageFactory = () => Promise<BucketStorageFactory>;

export const INITIALIZED_MONGO_STORAGE_FACTORY: StorageFactory = async (options?: test_utils.StorageOptions) => {
  const db = await connectMongo();

  // None of the PG tests insert data into this collection, so it was never created
  if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
    await db.db.createCollection('bucket_parameters');
  }

  if (!options?.doNotClear) {
    await db.clear();
  }

  return new mongo_storage.storage.MongoBucketStorage(db, {
    slot_name_prefix: 'test_'
  });
};

export async function connectMongo() {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client = mongo_storage.storage.createMongoClient(env.MONGO_TEST_URL, {
    connectTimeoutMS: env.CI ? 15_000 : 5_000,
    socketTimeoutMS: env.CI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: env.CI ? 15_000 : 2_500
  });
  return new mongo_storage.storage.PowerSyncMongo(client);
}

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
      await db.query(`DROP TABLE public.${pg_utils.escapeIdentifier(name)}`);
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
  bucketStorage: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<OpId> {
  const start = Date.now();

  const api = new PostgresRouteAPIAdapter(db);
  const lsn = await api.getReplicationHead();

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const cp = await bucketStorage.getActiveCheckpoint();
    if (!cp.hasSyncRules()) {
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
