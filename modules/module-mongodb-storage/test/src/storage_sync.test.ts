import { storage } from '@powersync/service-core';
import { register, TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('sync - mongodb', () => {
  register.registerSyncTests(INITIALIZED_MONGO_STORAGE_FACTORY);

  // The split of returned results can vary depending on storage drivers
  test('large batch (2)', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned chunks.
    // Similar to the above test, but splits over 1MB chunks.
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM "%"
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]), options)
    );
    expect(test_utils.getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'large1', checksum: 1178768505 }
    ]);
    expect(test_utils.getBatchMeta(batch1)).toEqual({
      after: '0',
      has_more: true,
      next_after: '2'
    });

    const batch2 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        new Map([['global[]', BigInt(batch1[0].chunkData.next_after)]]),
        options
      )
    );
    expect(test_utils.getBatchData(batch2)).toEqual([
      { op_id: '3', op: 'PUT', object_id: 'large2', checksum: 1607205872 }
    ]);
    expect(test_utils.getBatchMeta(batch2)).toEqual({
      after: '2',
      has_more: true,
      next_after: '3'
    });

    const batch3 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        new Map([['global[]', BigInt(batch2[0].chunkData.next_after)]]),
        options
      )
    );
    expect(test_utils.getBatchData(batch3)).toEqual([
      { op_id: '4', op: 'PUT', object_id: 'test3', checksum: 1359888332 }
    ]);
    expect(test_utils.getBatchMeta(batch3)).toEqual({
      after: '3',
      has_more: false,
      next_after: '4'
    });

    // Test that the checksum type is correct.
    // Specifically, test that it never persisted as double.
    const checksumTypes = await factory.db.bucket_data
      .aggregate([{ $group: { _id: { $type: '$checksum' }, count: { $sum: 1 } } }])
      .toArray();
    expect(checksumTypes).toEqual([{ _id: 'long', count: 4 }]);
  });
});
