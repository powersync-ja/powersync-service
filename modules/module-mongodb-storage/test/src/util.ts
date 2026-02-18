import { env } from './env.js';
import { mongoTestReportStorageFactoryGenerator, mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { CURRENT_STORAGE_VERSION, LEGACY_STORAGE_VERSION } from '@powersync/service-core';

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_MONGO_REPORT_STORAGE_FACTORY = mongoTestReportStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const TEST_STORAGE_VERSIONS = [LEGACY_STORAGE_VERSION, CURRENT_STORAGE_VERSION];
