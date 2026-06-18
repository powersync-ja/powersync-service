import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { updateSyncRulesFromYaml } from '@powersync/service-core';
import { register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import type { MongoBucketStorage } from '../../src/index.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

for (let storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Mongo Sync Bucket Storage - Parameters - v${storageVersion}`, () =>
    register.registerDataStorageParameterTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));

  describe(`Mongo Sync Bucket Storage - Data - v${storageVersion}`, () =>
    register.registerDataStorageDataTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));

  describe(`Mongo Sync Bucket Storage - Checkpoints - v${storageVersion}`, () =>
    register.registerDataStorageCheckpointTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));
}

describe('Sync Bucket Validation', register.registerBucketValidationTests);

test('Mongo v3 bucket data collections use the optimized bucket/op index', async () => {
  await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
  const replicationStream = await factory.updateSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test
`,
      { storageVersion: 3 }
    )
  );

  const bucketStorage = factory.getInstance(replicationStream);
  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  void writer;

  const mongoFactory = factory as MongoBucketStorage;
  const collections = await mongoFactory.db.listBucketDataCollectionsV3(Number(replicationStream.replicationStreamId));
  expect(collections).toHaveLength(1);
  const indexes = await collections[0].indexes();
  expect(indexes).toContainEqual(
    expect.objectContaining({
      name: 'bucket_op',
      key: {
        '_id.b': 1,
        '_id.o': 1,
        checksum: 1,
        op: 1
      }
    })
  );
});

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
