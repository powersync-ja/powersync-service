import * as types from '@module/types/types.js';

import { test_utils } from '@powersync/service-core-tests';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as mongo from 'mongodb';
import { env } from './env.js';

export const TEST_URI = env.MONGO_TEST_DATA_URL;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mongodb',
  uri: TEST_URI
});

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

export const INITIALIZED_MONGO_STORAGE_FACTORY = async (options?: test_utils.StorageOptions) => {
  const db = await connectMongo();

  // None of the PG tests insert data into this collection, so it was never created
  if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
    await db.db.createCollection('bucket_parameters');
  }
  if (!options?.doNotClear) {
    await db.clear();
  }

  return new mongo_storage.MongoBucketStorage(db, { slot_name_prefix: 'test_' });
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
