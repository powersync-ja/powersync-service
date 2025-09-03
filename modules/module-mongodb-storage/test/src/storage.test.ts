import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { env } from './env.js';
import { MongoTestStorageFactoryGenerator } from '@module/storage/implementation/MongoTestStorageFactoryGenerator.js';
import { MongoChecksumOptions } from '@module/storage/implementation/MongoChecksums.js';

describe('Mongo Sync Bucket Storage - Parameters', () =>
  register.registerDataStorageParameterTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Mongo Sync Bucket Storage - Data', () =>
  register.registerDataStorageDataTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Mongo Sync Bucket Storage - Checkpoints', () =>
  register.registerDataStorageCheckpointTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Sync Bucket Validation', register.registerBucketValidationTests);

describe('Mongo Sync Bucket Storage - split operations', () =>
  register.registerDataStorageDataTests(
    MongoTestStorageFactoryGenerator({
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
    MongoTestStorageFactoryGenerator({
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

describe('Mongo Sync Bucket Storage - checksum calculations', () => {
  // This test tests 4 buckets x 4 operations in each.
  // We specifically use operationBatchLimit that does not have factors in common with 4,
  // as well some that do.
  const params: MongoChecksumOptions[] = [
    {
      bucketBatchLimit: 100,
      operationBatchLimit: 3
    },

    {
      bucketBatchLimit: 10,
      operationBatchLimit: 7
    },

    {
      bucketBatchLimit: 3,
      operationBatchLimit: 1
    },
    {
      bucketBatchLimit: 1,
      operationBatchLimit: 3
    },
    {
      bucketBatchLimit: 2,
      operationBatchLimit: 4
    },
    {
      bucketBatchLimit: 4,
      operationBatchLimit: 12
    }
  ];
  for (let options of params) {
    describe(`${options.bucketBatchLimit}|${options.operationBatchLimit}`, () => {
      register.testChecksumBatching(
        MongoTestStorageFactoryGenerator({
          url: env.MONGO_TEST_URL,
          isCI: env.CI,
          internalOptions: {
            checksumOptions: options
          }
        })
      );
    });
  }
});
