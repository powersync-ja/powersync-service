import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

/**
 * Bucket compacting is not yet implemented.
 * This causes the internal compacting test to fail.
 * Other tests have been verified manually.
 */
function registerStorageVersionTests(storageVersion: number) {
  describe(`storage v${storageVersion}`, () => {
    const storageFactory = POSTGRES_STORAGE_FACTORY;

    register.registerSyncTests(storageFactory, { storageVersion });

    test('large batch (2)', async () => {
      // Test syncing a batch of data that is small in count,
      // but large enough in size to be split over multiple returned chunks.
      // Similar to the above test, but splits over 1MB chunks.
      await using factory = await storageFactory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM "%"
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const globalBucket = bucketRequest(syncRules, 'global[]');

      const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
        const sourceTable = TEST_TABLE;

        const largeDescription = '0123456789'.repeat(2_000_00);

        await batch.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 'test1',
            description: 'test1'
          },
          afterReplicaId: test_utils.rid('test1')
        });

        await batch.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 'large1',
            description: largeDescription
          },
          afterReplicaId: test_utils.rid('large1')
        });

        // Large enough to split the returned batch
        await batch.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 'large2',
            description: largeDescription
          },
          afterReplicaId: test_utils.rid('large2')
        });

        await batch.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 'test3',
            description: 'test3'
          },
          afterReplicaId: test_utils.rid('test3')
        });
      });

      const checkpoint = result!.flushed_op;

      const options: storage.BucketDataBatchOptions = {};

      const batch1 = await test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(checkpoint, new Map([[globalBucket, 0n]]), options)
      );
      expect(test_utils.getBatchData(batch1)).toEqual([
        { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 }
      ]);
      expect(test_utils.getBatchMeta(batch1)).toEqual({
        after: '0',
        has_more: true,
        next_after: '1'
      });

      const batch2 = await test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(
          checkpoint,
          new Map([[globalBucket, BigInt(batch1[0].chunkData.next_after)]]),
          options
        )
      );
      expect(test_utils.getBatchData(batch2)).toEqual([
        { op_id: '2', op: 'PUT', object_id: 'large1', checksum: 1178768505 }
      ]);
      expect(test_utils.getBatchMeta(batch2)).toEqual({
        after: '1',
        has_more: true,
        next_after: '2'
      });

      const batch3 = await test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(
          checkpoint,
          new Map([[globalBucket, BigInt(batch2[0].chunkData.next_after)]]),
          options
        )
      );
      expect(test_utils.getBatchData(batch3)).toEqual([
        { op_id: '3', op: 'PUT', object_id: 'large2', checksum: 1607205872 }
      ]);
      expect(test_utils.getBatchMeta(batch3)).toEqual({
        after: '2',
        has_more: true,
        next_after: '3'
      });

      const batch4 = await test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(
          checkpoint,
          new Map([[globalBucket, BigInt(batch3[0].chunkData.next_after)]]),
          options
        )
      );
      expect(test_utils.getBatchData(batch4)).toEqual([
        { op_id: '4', op: 'PUT', object_id: 'test3', checksum: 1359888332 }
      ]);
      expect(test_utils.getBatchMeta(batch4)).toEqual({
        after: '3',
        has_more: false,
        next_after: '4'
      });
    });
  });
}

describe('sync - postgres', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    registerStorageVersionTests(storageVersion);
  }
});
