import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

for (let storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Mongo Sync Bucket Storage - Parameters - v${storageVersion}`, () =>
    register.registerDataStorageParameterTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));

  describe(`Mongo Sync Bucket Storage - Data - v${storageVersion}`, () =>
    register.registerDataStorageDataTests({
      ...INITIALIZED_MONGO_STORAGE_FACTORY,
      storageVersion,
      compressedBucketStorage: storageVersion >= 3
    }));

  describe(`Mongo Sync Bucket Storage - Checkpoints - v${storageVersion}`, () =>
    register.registerDataStorageCheckpointTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));

  describe(`Mongo Sync Bucket Storage - write checkpoint metadata - v${storageVersion}`, () => {
    test('uses checkpoint_requested_at as the client-requested checkpoint marker', async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
bucket_definitions:
  global:
    data: []
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules);

      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '5/0' }, checkpoint_request_id: 42n }
      ]);
      const requested = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      expect(requested?.checkpoint_requested_at).toBeInstanceOf(Date);

      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '6/0' }, checkpoint_request_id: 41n }
      ]);
      const stale = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      expect(stale?.checkpoint_requested_at).toEqual(requested?.checkpoint_requested_at);

      await bucketStorage.createManagedWriteCheckpoints([{ user_id: 'user1', heads: { '1': '7/0' } }]);
      const generated = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      // Generated checkpoints unset the field, keeping the document out of the
      // partial checkpoint_requested_at index.
      expect(generated).not.toBeNull();
      expect(generated?.checkpoint_requested_at).toBeUndefined();

      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user2', heads: { '1': '8/0' }, checkpoint_request_id: 50n }
      ]);
      await factory.db.write_checkpoints.updateOne(
        { user_id: 'user2' },
        { $set: { checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z') } }
      );
      await bucketStorage.compact({
        compactBuckets: [],
        deleteCheckpointRequestsBefore: new Date('2024-02-01T00:00:00.000Z')
      });
      await expect(factory.db.write_checkpoints.findOne({ user_id: 'user2' })).resolves.toBeNull();

      bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('1/1');
      const customCheckpointRequestedAt = new Date('2024-01-01T00:00:00.000Z');
      writer.addCustomWriteCheckpoint({
        user_id: 'custom1',
        checkpoint: 51n,
        checkpoint_requested_at: customCheckpointRequestedAt
      });
      await writer.flush();
      const customRequested = await factory.db.custom_write_checkpoints.findOne({ user_id: 'custom1' });
      expect(customRequested?.checkpoint_requested_at).toEqual(customCheckpointRequestedAt);

      writer.addCustomWriteCheckpoint({
        user_id: 'custom1',
        checkpoint: 52n
      });
      await writer.flush();
      const customGenerated = await factory.db.custom_write_checkpoints.findOne({ user_id: 'custom1' });
      expect(customGenerated).not.toBeNull();
      expect(customGenerated?.checkpoint_requested_at).toBeUndefined();
    });
  });
}

describe('Sync Bucket Validation', register.registerBucketValidationTests);

describe('Mongo Sync Bucket Storage - split operations', () =>
  register.registerDataStorageDataTests(
    mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      checksumOptions: {
        bucketBatchLimit: 100,
        operationBatchLimit: 1
      }
    })
  ));

describe('Mongo Sync Bucket Storage - split buckets', () =>
  register.registerDataStorageDataTests(
    mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      checksumOptions: {
        bucketBatchLimit: 1,
        operationBatchLimit: 100
      }
    })
  ));
