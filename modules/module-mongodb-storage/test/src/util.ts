import { env } from './env.js';
import { mongoTestReportStorageFactoryGenerator, mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_MONGO_REPORT_STORAGE_FACTORY = mongoTestReportStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});
