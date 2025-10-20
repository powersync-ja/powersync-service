import { TestStorageOptions } from '@powersync/service-core';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { connectMongoForTests } from './util.js';
import { MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';

export type MongoTestStorageOptions = {
  url: string;
  isCI: boolean;
  internalOptions?: MongoSyncBucketStorageOptions;
};

export const MongoTestStorageFactoryGenerator = (factoryOptions: MongoTestStorageOptions) => {
  return async (options?: TestStorageOptions) => {
    const db = connectMongoForTests(factoryOptions.url, factoryOptions.isCI);

    // None of the tests insert data into this collection, so it was never created
    if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
      await db.db.createCollection('bucket_parameters');
    }

    if (!options?.doNotClear) {
      await db.clear();
    }

    // Full migrations are not currently run for tests, so we manually create the important ones
    await db.createCheckpointEventsCollection();
    await db.createBucketStateIndex();
    await db.createBucketStateIndex2();

    return new MongoBucketStorage(db, { slot_name_prefix: 'test_' }, factoryOptions.internalOptions);
  };
};
