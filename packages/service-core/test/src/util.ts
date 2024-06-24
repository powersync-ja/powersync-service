import * as pgwire from '@powersync/service-jpgwire';
import { normalizeConnection } from '@powersync/service-types';
import * as mongo from 'mongodb';
import { BucketStorageFactory } from '../../src/storage/BucketStorage.js';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { PowerSyncMongo } from '../../src/storage/mongo/db.js';
import { escapeIdentifier } from '../../src/util/pgwire_utils.js';
import { env } from './env.js';
import { Metrics } from '@/metrics/Metrics.js';

// The metrics need to be initialised before they can be used
await Metrics.initialise({
  disable_telemetry_sharing: true,
  powersync_instance_id: 'test',
  internal_metrics_endpoint: 'unused.for.tests.com',
  additional_metric_endpoints: []
});
Metrics.getInstance().resetCounters();

export const TEST_URI = env.PG_TEST_URL;

export type StorageFactory = () => Promise<BucketStorageFactory>;

export const MONGO_STORAGE_FACTORY: StorageFactory = async () => {
  const db = await connectMongo();
  await db.clear();
  return new MongoBucketStorage(db, { slot_name_prefix: 'test_' });
};

export async function clearTestDb(db: pgwire.PgClient) {
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
      await db.query(`DROP TABLE public.${escapeIdentifier(name)}`);
    }
  }
}

export const TEST_CONNECTION_OPTIONS = normalizeConnection({
  type: 'postgresql',
  uri: TEST_URI,
  sslmode: 'disable'
});

export async function connectPgWire(type?: 'replication' | 'standard') {
  const db = await pgwire.connectPgWire(TEST_CONNECTION_OPTIONS, { type });
  return db;
}

export function connectPgPool() {
  const db = pgwire.connectPgWirePool(TEST_CONNECTION_OPTIONS);
  return db;
}

export async function connectMongo() {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client = new mongo.MongoClient(env.MONGO_TEST_URL, {
    connectTimeoutMS: env.CI ? 15_000 : 5_000,
    socketTimeoutMS: env.CI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: env.CI ? 15_000 : 2_500
  });
  const db = new PowerSyncMongo(client);
  return db;
}
