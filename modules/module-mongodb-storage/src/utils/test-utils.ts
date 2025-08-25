import { mongo } from '@powersync/lib-service-mongodb';
import { PowerSyncMongo } from '../storage/implementation/db.js';
import { TestStorageOptions } from '@powersync/service-core';
import { MongoReportStorage } from '../storage/MongoReportStorage.js';
import { MongoBucketStorage } from '../storage/MongoBucketStorage.js';

export type MongoTestStorageOptions = {
  url: string;
  isCI: boolean;
};

export function mongoTestStorageFactoryGenerator(factoryOptions: MongoTestStorageOptions) {
  return async (options?: TestStorageOptions) => {
    const db = connectMongoForTests(factoryOptions.url, factoryOptions.isCI);

    // None of the tests insert data into this collection, so it was never created
    if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
      await db.db.createCollection('bucket_parameters');
    }

    // Full migrations are not currently run for tests, so we manually create this
    await db.createCheckpointEventsCollection();

    if (!options?.doNotClear) {
      await db.clear();
    }

    return new MongoBucketStorage(db, { slot_name_prefix: 'test_' });
  };
}

export function mongoTestReportStorageFactoryGenerator(factoryOptions: MongoTestStorageOptions) {
  return async (options?: TestStorageOptions) => {
    const db = connectMongoForTests(factoryOptions.url, factoryOptions.isCI);

    await db.createConnectionReportingCollection();

    if (!options?.doNotClear) {
      await db.clear();
    }

    return new MongoReportStorage(db);
  };
}

export const connectMongoForTests = (url: string, isCI: boolean) => {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client = new mongo.MongoClient(url, {
    connectTimeoutMS: isCI ? 15_000 : 5_000,
    socketTimeoutMS: isCI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: isCI ? 15_000 : 2_500
  });
  return new PowerSyncMongo(client);
};
