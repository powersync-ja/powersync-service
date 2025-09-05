import { mongo } from '@powersync/lib-service-mongodb';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';

import * as types from '@module/types/types.js';
import { env } from './env.js';
import { BSON_DESERIALIZE_DATA_OPTIONS, TestStorageFactory } from '@powersync/service-core';
import { describe, TestOptions } from 'vitest';

export const TEST_URI = env.MONGO_TEST_DATA_URL;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mongodb',
  uri: TEST_URI
});

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

export async function clearTestDb(db: mongo.Db) {
  await db.dropDatabase();
}

export async function connectMongoData() {
  const client = new mongo.MongoClient(env.MONGO_TEST_DATA_URL, {
    connectTimeoutMS: env.CI ? 15_000 : 5_000,
    socketTimeoutMS: env.CI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: env.CI ? 15_000 : 2_500,
    ...BSON_DESERIALIZE_DATA_OPTIONS
  });
  const dbname = new URL(env.MONGO_TEST_DATA_URL).pathname.substring(1);
  return { client, db: client.db(dbname) };
}
