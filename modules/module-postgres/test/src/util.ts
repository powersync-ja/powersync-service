import * as types from '@module/types/types.js';
import * as pg_utils from '@module/utils/pgwire_utils.js';
import { BucketStorageFactory, Metrics, MongoBucketStorage, OpId } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import { env } from './env.js';
import { pgwireRows } from '@powersync/service-jpgwire';
import { logger } from '@powersync/lib-services-framework';
import { connectMongo, StorageFactory } from '@core-tests/util.js';

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

export const INITIALIZED_MONGO_STORAGE_FACTORY: StorageFactory = async () => {
  const db = await connectMongo();
  // None of the PG tests insert data into this collection, so it was never created
  await db.db.createCollection('bucket_parameters');
  await db.clear();

  return new MongoBucketStorage(db, { slot_name_prefix: 'test_' });
};

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

  const [{ lsn }] = pgwireRows(await db.query(`SELECT pg_logical_emit_message(false, 'powersync', 'ping') as lsn`));

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const cp = await bucketStorage.getActiveCheckpoint();
    if (!cp.hasSyncRules()) {
      throw new Error('No sync rules available');
    }
    if (cp.lsn >= lsn) {
      logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
      return cp.checkpoint;
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error('Timeout while waiting for checkpoint');
}
