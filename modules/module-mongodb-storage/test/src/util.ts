import { env } from './env.js';

import { MongoTestStorageFactoryGenerator } from '@module/storage/implementation/MongoTestStorageFactoryGenerator.js';
import { MongoTestReportStorageFactoryGenerator } from '@module/storage/implementation/MongoTestReportStorageFactoryGenerator.js';

export const INITIALIZED_MONGO_STORAGE_FACTORY = MongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_MONGO_REPORT_STORAGE_FACTORY = MongoTestReportStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});
