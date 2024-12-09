import * as types from '@module/types/types.js';
import { BucketStorageFactory, Metrics } from '@powersync/service-core';

import { MongoBucketStorage } from '@module/storage/MongoBucketStorage.js';
import { PowerSyncMongo } from '@module/storage/storage-index.js';
import { test_utils } from '@powersync/service-core-tests';
import * as mongo from 'mongodb';
import { env } from './env.js';

// The metrics need to be initialized before they can be used
await Metrics.initialise({
  disable_telemetry_sharing: true,
  powersync_instance_id: 'test',
  internal_metrics_endpoint: 'unused.for.tests.com'
});
Metrics.getInstance().resetCounters();

export const TEST_URI = env.MONGO_TEST_DATA_URL;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mongodb',
  uri: TEST_URI
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

  return new MongoBucketStorage(db, { slot_name_prefix: 'test_' });
};

export async function clearTestDb(db: mongo.Db) {
  await db.dropDatabase();
}

export async function connectMongoData() {
  const client = new mongo.MongoClient(env.MONGO_TEST_DATA_URL, {
    connectTimeoutMS: env.CI ? 15_000 : 5_000,
    socketTimeoutMS: env.CI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: env.CI ? 15_000 : 2_500,
    useBigInt64: true
  });
  const dbname = new URL(env.MONGO_TEST_DATA_URL).pathname.substring(1);
  return { client, db: client.db(dbname) };
}

export async function connectMongo() {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client = new mongo.MongoClient(env.MONGO_TEST_URL, {
    connectTimeoutMS: env.CI ? 15_000 : 5_000,
    socketTimeoutMS: env.CI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: env.CI ? 15_000 : 2_500
  });
  return new PowerSyncMongo(client);
}
