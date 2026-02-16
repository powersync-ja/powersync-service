import { register, TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { storage, SyncRulesBucketStorage } from '@powersync/service-core';

describe('Mongo Sync Bucket Storage Compact', () => {
  register.registerCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY);

  describe('with blank bucket_state', () => {
    // This can happen when migrating from older service versions, that did not populate bucket_state yet.
    const populate = async (bucketStorage: SyncRulesBucketStorage) => {
      await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 't1',
            owner_id: 'u1'
          },
          afterReplicaId: test_utils.rid('t1')
        });

        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 't2',
            owner_id: 'u2'
          },
          afterReplicaId: test_utils.rid('t2')
        });

        await batch.commit('1/1');
      });

      return bucketStorage.getCheckpoint();
    };

    const setup = async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();
      const syncRules = await factory.updateSyncRules({
        content: `
bucket_definitions:
  by_user:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `
      });
      const bucketStorage = factory.getInstance(syncRules);
      const { checkpoint } = await populate(bucketStorage);

      return { bucketStorage, checkpoint, factory };
    };

    test('full compact', async () => {
      const { bucketStorage, checkpoint, factory } = await setup();

      // Simulate bucket_state from old version not being available
      await factory.db.bucket_state.deleteMany({});

      await bucketStorage.compact({
        clearBatchLimit: 200,
        moveBatchLimit: 10,
        moveBatchQueryLimit: 10,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId: checkpoint,
        signal: null as any
      });

      const checksumAfter = await bucketStorage.getChecksums(checkpoint, ['by_user["u1"]', 'by_user["u2"]']);
      expect(checksumAfter.get('by_user["u1"]')).toEqual({
        bucket: 'by_user["u1"]',
        checksum: -659469718,
        count: 1
      });
      expect(checksumAfter.get('by_user["u2"]')).toEqual({
        bucket: 'by_user["u2"]',
        checksum: 430217650,
        count: 1
      });
    });

    test('populatePersistentChecksumCache', async () => {
      // Populate old sync rules version
      const { factory } = await setup();

      // Not populate another version (bucket definition name changed)
      const syncRules = await factory.updateSyncRules({
        content: `
bucket_definitions:
  by_user2:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `
      });
      const bucketStorage = factory.getInstance(syncRules);

      await populate(bucketStorage);
      const { checkpoint } = await bucketStorage.getCheckpoint();

      // Default is to small small numbers - should be a no-op
      const result0 = await bucketStorage.populatePersistentChecksumCache({
        maxOpId: checkpoint
      });
      expect(result0.buckets).toEqual(0);

      // This should cache the checksums for the two buckets
      const result1 = await bucketStorage.populatePersistentChecksumCache({
        maxOpId: checkpoint,
        minBucketChanges: 1
      });
      expect(result1.buckets).toEqual(2);

      // This should be a no-op, as the checksums are already cached
      const result2 = await bucketStorage.populatePersistentChecksumCache({
        maxOpId: checkpoint,
        minBucketChanges: 1
      });
      expect(result2.buckets).toEqual(0);

      const checksumAfter = await bucketStorage.getChecksums(checkpoint, ['by_user2["u1"]', 'by_user2["u2"]']);
      expect(checksumAfter.get('by_user2["u1"]')).toEqual({
        bucket: 'by_user2["u1"]',
        checksum: -659469718,
        count: 1
      });
      expect(checksumAfter.get('by_user2["u2"]')).toEqual({
        bucket: 'by_user2["u2"]',
        checksum: 430217650,
        count: 1
      });
    });
  });
});

describe('Mongo Sync Parameter Storage Compact', () => {
  register.registerParameterCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY);
});
