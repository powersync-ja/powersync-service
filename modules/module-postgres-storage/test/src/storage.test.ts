import { storage } from '@powersync/service-core';
import { register, TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Sync Bucket Validation', register.registerBucketValidationTests);

describe('Postgres Sync Bucket Storage', () => {
  register.registerDataStorageTests(POSTGRES_STORAGE_FACTORY);

  /**
   * The split of returned results can vary depending on storage drivers.
   * The large rows here are 2MB large while the default chunk limit is 1mb.
   * The Postgres storage driver will detect if the next row will increase the batch
   * over the limit and separate that row into a new batch (or single row batch) if applicable.
   */
  test('large batch (2)', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned chunks.
    // Similar to the above test, but splits over 1MB chunks.
    const sync_rules = test_utils.testRules(
      `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM "%"
    `
    );
    using factory = await POSTGRES_STORAGE_FACTORY();
    const bucketStorage = factory.getInstance(sync_rules);

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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]), options)
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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', batch1[0].batch.next_after]]), options)
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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', batch2[0].batch.next_after]]), options)
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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', batch3[0].batch.next_after]]), options)
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
