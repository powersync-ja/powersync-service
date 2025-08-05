import { TestStorageOptions } from '@powersync/service-core';
import { connectMongoForTests } from './util.js';
import { MongoReportStorage } from '../MongoReportStorage.js';

export type MongoTestStorageOptions = {
  url: string;
  isCI: boolean;
};

export const MongoTestReportStorageFactoryGenerator = (factoryOptions: MongoTestStorageOptions) => {
  return async (options?: TestStorageOptions) => {
    const db = connectMongoForTests(factoryOptions.url, factoryOptions.isCI);

    await db.createSdkReportingCollection();

    if (!options?.doNotClear) {
      await db.clear();
    }

    return new MongoReportStorage(db);
  };
};
