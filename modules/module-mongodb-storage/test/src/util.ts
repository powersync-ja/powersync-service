import { PowerSyncMongo } from '@module/storage/implementation/db.js';
import { MongoBucketStorage } from '@module/storage/MongoBucketStorage.js';
import { StorageFactory, test_utils } from '@powersync/service-core-tests';
import * as mongo from 'mongodb';
import { env } from './env.js';

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
