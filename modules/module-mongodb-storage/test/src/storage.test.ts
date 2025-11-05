import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { env } from './env.js';
import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';

describe('Mongo Sync Bucket Storage - Parameters', () =>
  register.registerDataStorageParameterTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Mongo Sync Bucket Storage - Data', () =>
  register.registerDataStorageDataTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Mongo Sync Bucket Storage - Checkpoints', () =>
  register.registerDataStorageCheckpointTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Sync Bucket Validation', register.registerBucketValidationTests);

describe('Mongo Sync Bucket Storage - split operations', () =>
  register.registerDataStorageDataTests(
    mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      internalOptions: {
        checksumOptions: {
          bucketBatchLimit: 100,
          operationBatchLimit: 1
        }
      }
    })
  ));

describe('Mongo Sync Bucket Storage - split buckets', () =>
  register.registerDataStorageDataTests(
    mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      internalOptions: {
        checksumOptions: {
          bucketBatchLimit: 1,
          operationBatchLimit: 100
        }
      }
    })
  ));
