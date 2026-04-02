import { VersionedPowerSyncMongoV3 } from '@module/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { storage, SyncRulesBucketStorage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('Mongo Sync Bucket Storage Compact', () => {
  register.registerCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY);

  describe('with blank bucket_state', () => {
    // This can happen when migrating from older service versions, that did not populate bucket_state yet.
    const populate = async (bucketStorage: SyncRulesBucketStorage, sourceTableIndex: number) => {
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

      const sourceTable = await test_utils.resolveTestTable(
        writer,
        'test',
        ['id'],
        INITIALIZED_MONGO_STORAGE_FACTORY,
        sourceTableIndex
      );
      await writer.markAllSnapshotDone('1/1');

      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          owner_id: 'u1'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          owner_id: 'u2'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await writer.commit('1/1');

      return bucketStorage.getCheckpoint();
    };

    const setup = async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
bucket_definitions:
  by_user:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const { checkpoint } = await populate(bucketStorage, 1);

      return { bucketStorage, checkpoint, factory, syncRules };
    };

    test('full compact', async () => {
      const { bucketStorage, checkpoint, factory, syncRules } = await setup();
      const storageDb = (bucketStorage as any).db;

      // Simulate bucket_state from old version not being available
      if (storageDb.storageConfig.incrementalReprocessing) {
        await storageDb.bucketStateV3(bucketStorage.group_id).deleteMany({});
      } else {
        await factory.db.bucket_state.deleteMany({});
      }

      await bucketStorage.compact({
        clearBatchLimit: 200,
        moveBatchLimit: 10,
        moveBatchQueryLimit: 10,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId: checkpoint,
        signal: null as any
      });

      const users = ['u1', 'u2'];
      const userRequests = users.map((user) => bucketRequest(syncRules, `by_user["${user}"]`));
      const [u1Request, u2Request] = userRequests;
      const checksumAfter = await bucketStorage.getChecksums(checkpoint, userRequests);
      expect(checksumAfter.get(u1Request.bucket)).toEqual({
        bucket: u1Request.bucket,
        checksum: -659469718,
        count: 1
      });
      expect(checksumAfter.get(u2Request.bucket)).toEqual({
        bucket: u2Request.bucket,
        checksum: 430217650,
        count: 1
      });
    });

    test('populatePersistentChecksumCache', async () => {
      // Populate old sync rules version
      const { factory } = await setup();

      // Now populate another version (bucket definition name changed)
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
bucket_definitions:
  by_user2:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const storageDb = (bucketStorage as any).db;

      await populate(bucketStorage, 2);
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

      const users = ['u1', 'u2'];
      const userRequests = users.map((user) => bucketRequest(syncRules, `by_user2["${user}"]`));
      const [u1Request, u2Request] = userRequests;
      const checksumAfter = await bucketStorage.getChecksums(checkpoint, userRequests);
      expect(checksumAfter.get(u1Request.bucket)).toEqual({
        bucket: u1Request.bucket,
        checksum: -659469718,
        count: 1
      });
      expect(checksumAfter.get(u2Request.bucket)).toEqual({
        bucket: u2Request.bucket,
        checksum: 430217650,
        count: 1
      });
    });

    test('dirty bucket discovery handles bigint bucket_state bytes', async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const storageDb = bucketStorage.db;

      // This simulates bucket_state created using bigint bytes.
      // This typically happens when buckets get very large (> 2GiB). We don't want to create that much
      // data in the tests, so we directly insert the bucket_state here.
      if (storageDb.storageConfig.incrementalReprocessing) {
        const bucketStateCollection = (storageDb as VersionedPowerSyncMongoV3).bucketStateV3(bucketStorage.group_id);
        await bucketStateCollection.insertOne({
          _id: {
            d: '1',
            b: 'global[]'
          },
          last_op: 5n,
          compacted_state: {
            op_id: 3n,
            count: 3,
            checksum: 0n,
            bytes: 7n
          },
          estimate_since_compact: {
            count: 2,
            bytes: 5n
          }
        });
      } else {
        await factory.db.bucket_state.insertOne({
          _id: {
            g: bucketStorage.group_id,
            b: 'global[]'
          },
          last_op: 5n,
          compacted_state: {
            op_id: 3n,
            count: 3,
            checksum: 0n,
            bytes: 7n
          },
          estimate_since_compact: {
            count: 2,
            bytes: 5n
          }
        });
      }

      // This test uses a couple of "internal" APIs of the compactor.
      const compactor = bucketStorage.createMongoCompactor({ maxOpId: 5n });

      const dirtyBuckets = compactor.dirtyBucketBatches({
        minBucketChanges: 1,
        minChangeRatio: 0.39
      });
      const firstBatch = await dirtyBuckets.next();

      expect(firstBatch.done).toBe(false);
      expect(firstBatch.value).toHaveLength(1);
      expect(firstBatch.value[0].bucket).toBe('global[]');
      expect(firstBatch.value[0].estimatedCount).toBe(5);
      expect(typeof firstBatch.value[0].estimatedCount).toBe('number');
      expect(firstBatch.value[0].dirtyRatio).toBeCloseTo(5 / 12);

      const checksumBuckets = await (compactor as any).dirtyBucketBatchForChecksums({
        minBucketChanges: 1
      });
      expect(checksumBuckets).toEqual([
        {
          bucket: 'global[]',
          definitionId: storageDb.storageConfig.incrementalReprocessing ? '1' : null,
          estimatedCount: 5
        }
      ]);
    });
  });
});

describe('Mongo Sync Parameter Storage Compact', () => {
  register.registerParameterCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY);
});
