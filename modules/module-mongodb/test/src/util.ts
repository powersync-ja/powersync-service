import { mongo } from '@powersync/lib-service-mongodb';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';

import * as types from '@module/types/types.js';
import {
  BSON_DESERIALIZE_DATA_OPTIONS,
  SUPPORTED_STORAGE_VERSIONS,
  TestStorageConfig,
  TestStorageFactory
} from '@powersync/service-core';
import { describe, TestOptions } from 'vitest';
import { env } from './env.js';

export const TEST_URI = env.MONGO_TEST_DATA_URL;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mongodb',
  uri: TEST_URI
});

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.test_utils.postgresTestSetup({
  url: env.PG_STORAGE_TEST_URL
});

export const TEST_STORAGE_VERSIONS = SUPPORTED_STORAGE_VERSIONS;

export interface StorageVersionTestContext {
  factory: TestStorageFactory;
  storageVersion: number;
}

export function describeWithStorage(options: TestOptions, fn: (context: StorageVersionTestContext) => void) {
  const describeFactory = (storageName: string, config: TestStorageConfig) => {
    describe(`${storageName} storage`, options, function () {
      for (const storageVersion of TEST_STORAGE_VERSIONS) {
        describe(`storage v${storageVersion}`, function () {
          fn({
            factory: config.factory,
            storageVersion
          });
        });
      }
    });
  };

  if (env.TEST_MONGO_STORAGE) {
    describeFactory('mongodb', INITIALIZED_MONGO_STORAGE_FACTORY);
  }

  if (env.TEST_POSTGRES_STORAGE) {
    describeFactory('postgres', INITIALIZED_POSTGRES_STORAGE_FACTORY);
  }
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
