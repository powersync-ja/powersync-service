import * as pgwire from '@powersync/service-jpgwire';
import { normalizeConnection } from '@powersync/service-types';
import * as mongo from 'mongodb';
import { BucketStorageFactory, SyncBucketDataBatch } from '../../src/storage/BucketStorage.js';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { PowerSyncMongo } from '../../src/storage/mongo/db.js';
import { escapeIdentifier } from '../../src/util/pgwire_utils.js';
import { env } from './env.js';
import { Metrics } from '@/metrics/Metrics.js';
import { hashData } from '@/util/utils.js';
import { SourceTable } from '@/storage/SourceTable.js';
import * as bson from 'bson';
import { SyncBucketData } from '@/util/protocol-types.js';

// The metrics need to be initialised before they can be used
await Metrics.initialise({
  disable_telemetry_sharing: true,
  powersync_instance_id: 'test',
  internal_metrics_endpoint: 'unused.for.tests.com'
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

export function makeTestTable(name: string, columns?: string[] | undefined) {
  const relId = hashData('table', name, (columns ?? ['id']).join(','));
  const id = new bson.ObjectId('6544e3899293153fa7b38331');
  return new SourceTable(
    id,
    SourceTable.DEFAULT_TAG,
    relId,
    SourceTable.DEFAULT_SCHEMA,
    name,
    (columns ?? ['id']).map((column) => ({ name: column, typeOid: 25 })),
    true
  );
}

export function getBatchData(batch: SyncBucketData[] | SyncBucketDataBatch[] | SyncBucketDataBatch) {
  const first = getFirst(batch);
  if (first == null) {
    return [];
  }
  return first.data.map((d) => {
    return {
      op_id: d.op_id,
      op: d.op,
      object_id: d.object_id,
      checksum: d.checksum
    };
  });
}

export function getBatchMeta(batch: SyncBucketData[] | SyncBucketDataBatch[] | SyncBucketDataBatch) {
  const first = getFirst(batch);
  if (first == null) {
    return null;
  }
  return {
    has_more: first.has_more,
    after: first.after,
    next_after: first.next_after
  };
}

function getFirst(batch: SyncBucketData[] | SyncBucketDataBatch[] | SyncBucketDataBatch): SyncBucketData | null {
  if (!Array.isArray(batch)) {
    return batch.batch;
  }
  if (batch.length == 0) {
    return null;
  }
  let first = batch[0];
  if ((first as SyncBucketDataBatch).batch != null) {
    return (first as SyncBucketDataBatch).batch;
  } else {
    return first as SyncBucketData;
  }
}
