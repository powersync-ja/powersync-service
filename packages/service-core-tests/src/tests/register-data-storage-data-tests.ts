import { BucketDataBatchOptions, getUuidReplicaIdentityBson, OplogEntry, storage } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { TEST_TABLE } from './util.js';

/**
 * Normalize data from OplogEntries for comparison in tests.
 * Tests typically expect the stringified result
 */
const normalizeOplogData = (data: OplogEntry['data']) => {
  if (data != null && typeof data == 'object') {
    return JSON.stringify(data);
  }
  return data;
};

/**
 * @example
 * ```TypeScript
 *
 * describe('store - mongodb', function () {
 *  registerDataStorageDataTests(MONGO_STORAGE_FACTORY);
 * });
 *
 * ```
 */
export function registerDataStorageDataTests(generateStorageFactory: storage.TestStorageFactory) {
  test('removing row', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;
      await batch.markAllSnapshotDone('1/1');

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
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('test1')
      });
      await batch.commit('1/1');
    });

    const { checkpoint } = await bucketStorage.getCheckpoint();

    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]])));
    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        checksum: d.checksum
      };
    });

    const c1 = 2871785649;
    const c2 = 2872534815;

    expect(data).toEqual([
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'REMOVE', object_id: 'test1', checksum: c2 }
    ]);

    const checksums = [...(await bucketStorage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([
      {
        bucket: 'global[]',
        checksum: (c1 + c2) & 0xffffffff,
        count: 2
      }
    ]);
  });

  test('changing client ids', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT client_id as id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    const sourceTable = TEST_TABLE;
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          client_id: 'client1a',
          description: 'test1a'
        },
        afterReplicaId: test_utils.rid('test1')
      });
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          client_id: 'client1b',
          description: 'test1b'
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test2',
          client_id: 'client2',
          description: 'test2'
        },
        afterReplicaId: test_utils.rid('test2')
      });

      await batch.commit('1/1');
    });
    const { checkpoint } = await bucketStorage.getCheckpoint();
    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]])));
    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id
      };
    });

    expect(data).toEqual([
      { op: 'PUT', object_id: 'client1a' },
      { op: 'PUT', object_id: 'client1b' },
      { op: 'REMOVE', object_id: 'client1a' },
      { op: 'PUT', object_id: 'client2' }
    ]);
  });

  test('re-apply delete', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;
      await batch.markAllSnapshotDone('1/1');

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: test_utils.rid('test1')
      });
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/1');
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('test1')
      });
    });

    const { checkpoint } = await bucketStorage.getCheckpoint();

    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]])));
    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        checksum: d.checksum
      };
    });

    const c1 = 2871785649;
    const c2 = 2872534815;

    expect(data).toEqual([
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'REMOVE', object_id: 'test1', checksum: c2 }
    ]);

    const checksums = [...(await bucketStorage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([
      {
        bucket: 'global[]',
        checksum: (c1 + c2) & 0xffffffff,
        count: 2
      }
    ]);
  });

  test('re-apply update + delete', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: test_utils.rid('test1')
      });
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/1');
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('test1')
      });

      await batch.commit('2/1');
    });

    const { checkpoint } = await bucketStorage.getCheckpoint();

    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]])));

    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        checksum: d.checksum
      };
    });

    const c1 = 2871785649;
    const c2 = 2872534815;

    expect(data).toEqual([
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'REMOVE', object_id: 'test1', checksum: c2 }
    ]);

    const checksums = [...(await bucketStorage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([
      {
        bucket: 'global[]',
        checksum: (c1 + c1 + c1 + c2) & 0xffffffff,
        count: 4
      }
    ]);
  });

  test('batch with overlapping replica ids', async () => {
    // This test checks that we get the correct output when processing rows with:
    // 1. changing replica ids
    // 2. overlapping with replica ids of other rows in the same transaction (at different times)
    // If operations are not processing in input order, this breaks easily.
    // It can break at two places:
    // 1. Not getting the correct "current_data" state for each operation.
    // 2. Output order not being correct.

    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    // Pre-setup
    const result1 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test2',
          description: 'test2a'
        },
        afterReplicaId: test_utils.rid('test2')
      });
    });

    const checkpoint1 = result1?.flushed_op ?? 0n;

    // Test batch
    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;
      // b
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1b'
        },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        before: {
          id: 'test1'
        },
        beforeReplicaId: test_utils.rid('test1'),
        after: {
          id: 'test2',
          description: 'test2b'
        },
        afterReplicaId: test_utils.rid('test2')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        before: {
          id: 'test2'
        },
        beforeReplicaId: test_utils.rid('test2'),
        after: {
          id: 'test3',
          description: 'test3b'
        },

        afterReplicaId: test_utils.rid('test3')
      });

      // c
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'test2',
          description: 'test2c'
        },
        afterReplicaId: test_utils.rid('test2')
      });

      // d
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test4',
          description: 'test4d'
        },
        afterReplicaId: test_utils.rid('test4')
      });

      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        before: {
          id: 'test4'
        },
        beforeReplicaId: test_utils.rid('test4'),
        after: {
          id: 'test5',
          description: 'test5d'
        },
        afterReplicaId: test_utils.rid('test5')
      });
    });

    const checkpoint2 = result2!.flushed_op;

    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint2, new Map([['global[]', checkpoint1]]))
    );

    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        data: normalizeOplogData(d.data)
      };
    });

    // Operations must be in this order
    expect(data).toEqual([
      // b
      { op: 'PUT', object_id: 'test1', data: JSON.stringify({ id: 'test1', description: 'test1b' }) },
      { op: 'REMOVE', object_id: 'test1', data: null },
      { op: 'PUT', object_id: 'test2', data: JSON.stringify({ id: 'test2', description: 'test2b' }) },
      { op: 'REMOVE', object_id: 'test2', data: null },
      { op: 'PUT', object_id: 'test3', data: JSON.stringify({ id: 'test3', description: 'test3b' }) },

      // c
      { op: 'PUT', object_id: 'test2', data: JSON.stringify({ id: 'test2', description: 'test2c' }) },

      // d
      { op: 'PUT', object_id: 'test4', data: JSON.stringify({ id: 'test4', description: 'test4d' }) },
      { op: 'REMOVE', object_id: 'test4', data: null },
      { op: 'PUT', object_id: 'test5', data: JSON.stringify({ id: 'test5', description: 'test5d' }) }
    ]);
  });

  test('changed data with replica identity full', async () => {
    function rid2(id: string, description: string) {
      return getUuidReplicaIdentityBson({ id, description }, [
        { name: 'id', type: 'VARCHAR', typeId: 25 },
        { name: 'description', type: 'VARCHAR', typeId: 25 }
      ]);
    }
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    const sourceTable = test_utils.makeTestTable('test', ['id', 'description']);

    // Pre-setup
    const result1 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid2('test1', 'test1a')
      });
    });

    const checkpoint1 = result1?.flushed_op ?? 0n;

    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Unchanged, but has a before id
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        before: {
          id: 'test1',
          description: 'test1a'
        },
        beforeReplicaId: rid2('test1', 'test1a'),
        after: {
          id: 'test1',
          description: 'test1b'
        },
        afterReplicaId: rid2('test1', 'test1b')
      });
    });

    const result3 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Delete
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 'test1',
          description: 'test1b'
        },
        beforeReplicaId: rid2('test1', 'test1b'),
        after: undefined
      });
    });

    const checkpoint3 = result3!.flushed_op;

    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint3, new Map([['global[]', checkpoint1]]))
    );
    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        data: normalizeOplogData(d.data),
        subkey: d.subkey
      };
    });

    // Operations must be in this order
    expect(data).toEqual([
      // 2
      // The REMOVE is expected because the subkey changes
      {
        op: 'REMOVE',
        object_id: 'test1',
        data: null,
        subkey: '6544e3899293153fa7b38331/740ba9f2-8b0f-53e3-bb17-5f38a9616f0e'
      },
      {
        op: 'PUT',
        object_id: 'test1',
        data: JSON.stringify({ id: 'test1', description: 'test1b' }),
        subkey: '6544e3899293153fa7b38331/500e9b68-a2fd-51ff-9c00-313e2fb9f562'
      },
      // 3
      {
        op: 'REMOVE',
        object_id: 'test1',
        data: null,
        subkey: '6544e3899293153fa7b38331/500e9b68-a2fd-51ff-9c00-313e2fb9f562'
      }
    ]);
  });

  test('unchanged data with replica identity full', async () => {
    function rid2(id: string, description: string) {
      return getUuidReplicaIdentityBson({ id, description }, [
        { name: 'id', type: 'VARCHAR', typeId: 25 },
        { name: 'description', type: 'VARCHAR', typeId: 25 }
      ]);
    }

    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    const sourceTable = test_utils.makeTestTable('test', ['id', 'description']);

    // Pre-setup
    const result1 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid2('test1', 'test1a')
      });
    });

    const checkpoint1 = result1?.flushed_op ?? 0n;

    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Unchanged, but has a before id
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        before: {
          id: 'test1',
          description: 'test1a'
        },
        beforeReplicaId: rid2('test1', 'test1a'),
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid2('test1', 'test1a')
      });
    });

    const result3 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Delete
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 'test1',
          description: 'test1a'
        },
        beforeReplicaId: rid2('test1', 'test1a'),
        after: undefined
      });
    });

    const checkpoint3 = result3!.flushed_op;

    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint3, new Map([['global[]', checkpoint1]]))
    );
    const data = batch[0].chunkData.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        data: normalizeOplogData(d.data),
        subkey: d.subkey
      };
    });

    // Operations must be in this order
    expect(data).toEqual([
      // 2
      {
        op: 'PUT',
        object_id: 'test1',
        data: JSON.stringify({ id: 'test1', description: 'test1a' }),
        subkey: '6544e3899293153fa7b38331/740ba9f2-8b0f-53e3-bb17-5f38a9616f0e'
      },
      // 3
      {
        op: 'REMOVE',
        object_id: 'test1',
        data: null,
        subkey: '6544e3899293153fa7b38331/740ba9f2-8b0f-53e3-bb17-5f38a9616f0e'
      }
    ]);
  });

  test('large batch', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned batches.
    // The specific batch splits is an implementation detail of the storage driver,
    // and the test will have to updated when other implementations are added.
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      const sourceTable = TEST_TABLE;

      const largeDescription = '0123456789'.repeat(12_000_00);

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

      await batch.commit('1/1');
    });

    const { checkpoint } = await bucketStorage.getCheckpoint();

    const options: storage.BucketDataBatchOptions = {
      chunkLimitBytes: 16 * 1024 * 1024
    };

    const batch1 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]), options)
    );
    expect(test_utils.getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'large1', checksum: 454746904 }
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
      { op_id: '3', op: 'PUT', object_id: 'large2', checksum: 1795508474 },
      { op_id: '4', op: 'PUT', object_id: 'test3', checksum: 1359888332 }
    ]);
    expect(test_utils.getBatchMeta(batch2)).toEqual({
      after: '2',
      has_more: false,
      next_after: '4'
    });

    const batch3 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        new Map([['global[]', BigInt(batch2[0].chunkData.next_after)]]),
        options
      )
    );
    expect(test_utils.getBatchData(batch3)).toEqual([]);
    expect(test_utils.getBatchMeta(batch3)).toEqual(null);
  });

  test('long batch', async () => {
    // Test syncing a batch of data that is limited by count.
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      const sourceTable = TEST_TABLE;

      for (let i = 1; i <= 6; i++) {
        await batch.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: `test${i}`,
            description: `test${i}`
          },
          afterReplicaId: `test${i}`
        });
      }

      await batch.commit('1/1');
    });

    const { checkpoint } = await bucketStorage.getCheckpoint();

    const batch1 = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]), { limit: 4 })
    );

    expect(test_utils.getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'test2', checksum: 730027011 },
      { op_id: '3', op: 'PUT', object_id: 'test3', checksum: 1359888332 },
      { op_id: '4', op: 'PUT', object_id: 'test4', checksum: 2049153252 }
    ]);

    expect(test_utils.getBatchMeta(batch1)).toEqual({
      after: '0',
      has_more: true,
      next_after: '4'
    });

    const batch2 = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', BigInt(batch1.chunkData.next_after)]]), {
        limit: 4
      })
    );
    expect(test_utils.getBatchData(batch2)).toEqual([
      { op_id: '5', op: 'PUT', object_id: 'test5', checksum: 3686902721 },
      { op_id: '6', op: 'PUT', object_id: 'test6', checksum: 1974820016 }
    ]);

    expect(test_utils.getBatchMeta(batch2)).toEqual({
      after: '4',
      has_more: false,
      next_after: '6'
    });

    const batch3 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', BigInt(batch2.chunkData.next_after)]]), {
        limit: 4
      })
    );
    expect(test_utils.getBatchData(batch3)).toEqual([]);

    expect(test_utils.getBatchMeta(batch3)).toEqual(null);
  });

  describe('batch has_more', () => {
    const setup = async (options: BucketDataBatchOptions) => {
      await using factory = await generateStorageFactory();
      const syncRules = await factory.updateSyncRules({
        content: `
  bucket_definitions:
    global1:
      data:
        - SELECT id, description FROM test WHERE bucket = 'global1'
    global2:
      data:
        - SELECT id, description FROM test WHERE bucket = 'global2'
  `
      });
      const bucketStorage = factory.getInstance(syncRules);

      await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
        await batch.markAllSnapshotDone('1/1');
        const sourceTable = TEST_TABLE;

        for (let i = 1; i <= 10; i++) {
          await batch.save({
            sourceTable,
            tag: storage.SaveOperationTag.INSERT,
            after: {
              id: `test${i}`,
              description: `test${i}`,
              bucket: i == 1 ? 'global1' : 'global2'
            },
            afterReplicaId: `test${i}`
          });
        }

        await batch.commit('1/1');
      });

      const { checkpoint } = await bucketStorage.getCheckpoint();
      return await test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(
          checkpoint,
          new Map([
            ['global1[]', 0n],
            ['global2[]', 0n]
          ]),
          options
        )
      );
    };

    test('batch has_more (1)', async () => {
      const batch = await setup({ limit: 5 });
      expect(batch.length).toEqual(2);

      expect(batch[0].chunkData.bucket).toEqual('global1[]');
      expect(batch[1].chunkData.bucket).toEqual('global2[]');

      expect(test_utils.getBatchData(batch[0])).toEqual([
        { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 }
      ]);

      expect(test_utils.getBatchData(batch[1])).toEqual([
        { op_id: '2', op: 'PUT', object_id: 'test2', checksum: 730027011 },
        { op_id: '3', op: 'PUT', object_id: 'test3', checksum: 1359888332 },
        { op_id: '4', op: 'PUT', object_id: 'test4', checksum: 2049153252 },
        { op_id: '5', op: 'PUT', object_id: 'test5', checksum: 3686902721 }
      ]);

      expect(test_utils.getBatchMeta(batch[0])).toEqual({
        after: '0',
        has_more: false,
        next_after: '1'
      });

      expect(test_utils.getBatchMeta(batch[1])).toEqual({
        after: '0',
        has_more: true,
        next_after: '5'
      });
    });

    test('batch has_more (2)', async () => {
      const batch = await setup({ limit: 11 });
      expect(batch.length).toEqual(2);

      expect(batch[0].chunkData.bucket).toEqual('global1[]');
      expect(batch[1].chunkData.bucket).toEqual('global2[]');

      expect(test_utils.getBatchData(batch[0])).toEqual([
        { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 }
      ]);

      expect(test_utils.getBatchData(batch[1])).toEqual([
        { op_id: '2', op: 'PUT', object_id: 'test2', checksum: 730027011 },
        { op_id: '3', op: 'PUT', object_id: 'test3', checksum: 1359888332 },
        { op_id: '4', op: 'PUT', object_id: 'test4', checksum: 2049153252 },
        { op_id: '5', op: 'PUT', object_id: 'test5', checksum: 3686902721 },
        { op_id: '6', op: 'PUT', object_id: 'test6', checksum: 1974820016 },
        { op_id: '7', op: 'PUT', object_id: 'test7', checksum: 2477637855 },
        { op_id: '8', op: 'PUT', object_id: 'test8', checksum: 3644033632 },
        { op_id: '9', op: 'PUT', object_id: 'test9', checksum: 1011055869 },
        { op_id: '10', op: 'PUT', object_id: 'test10', checksum: 1331456365 }
      ]);

      expect(test_utils.getBatchMeta(batch[0])).toEqual({
        after: '0',
        has_more: false,
        next_after: '1'
      });

      expect(test_utils.getBatchMeta(batch[1])).toEqual({
        after: '0',
        has_more: false,
        next_after: '10'
      });
    });

    test('batch has_more (3)', async () => {
      // 50 bytes is more than 1 row, less than 2 rows
      const batch = await setup({ limit: 3, chunkLimitBytes: 50 });

      expect(batch.length).toEqual(3);
      expect(batch[0].chunkData.bucket).toEqual('global1[]');
      expect(batch[1].chunkData.bucket).toEqual('global2[]');
      expect(batch[2].chunkData.bucket).toEqual('global2[]');

      expect(test_utils.getBatchData(batch[0])).toEqual([
        { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 }
      ]);

      expect(test_utils.getBatchData(batch[1])).toEqual([
        { op_id: '2', op: 'PUT', object_id: 'test2', checksum: 730027011 }
      ]);
      expect(test_utils.getBatchData(batch[2])).toEqual([
        { op_id: '3', op: 'PUT', object_id: 'test3', checksum: 1359888332 }
      ]);

      expect(test_utils.getBatchMeta(batch[0])).toEqual({
        after: '0',
        has_more: false,
        next_after: '1'
      });

      expect(test_utils.getBatchMeta(batch[1])).toEqual({
        after: '0',
        has_more: true,
        next_after: '2'
      });

      expect(test_utils.getBatchMeta(batch[2])).toEqual({
        after: '2',
        has_more: true,
        next_after: '3'
      });
    });
  });

  test('empty storage metrics', async () => {
    await using f = await generateStorageFactory({ dropAll: true });
    const metrics = await f.getStorageMetrics();
    expect(metrics).toEqual({
      operations_size_bytes: 0,
      parameters_size_bytes: 0,
      replication_size_bytes: 0
    });

    const r = await f.configureSyncRules({ content: 'bucket_definitions: {}', validate: false });
    const storage = f.getInstance(r.persisted_sync_rules!);
    await storage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.keepalive('1/0');
    });

    const metrics2 = await f.getStorageMetrics();
    expect(metrics2).toMatchSnapshot();
  });

  test('op_id initialization edge case', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned chunks.
    // Similar to the above test, but splits over 1MB chunks.
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
  bucket_definitions:
    global:
      data:
        - SELECT id FROM test
        - SELECT id FROM test_ignore WHERE false
  `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const sourceTable = test_utils.makeTestTable('test', ['id']);
    const sourceTableIgnore = test_utils.makeTestTable('test_ignore', ['id']);

    const result1 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      // This saves a record to current_data, but not bucket_data.
      // This causes a checkpoint to be created without increasing the op_id sequence.
      await batch.save({
        sourceTable: sourceTableIgnore,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1'
        },
        afterReplicaId: test_utils.rid('test1')
      });
    });

    const checkpoint1 = result1!.flushed_op;

    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test2'
        },
        afterReplicaId: test_utils.rid('test2')
      });
    });

    const checkpoint2 = result2!.flushed_op;
    // we expect 0n and 1n, or 1n and 2n.
    expect(checkpoint2).toBeGreaterThan(checkpoint1);
  });

  test('unchanged checksums', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT client_id as id, description FROM "%"
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    const sourceTable = TEST_TABLE;
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: test_utils.rid('test1')
      });
      await batch.commit('1/1');
    });
    const { checkpoint } = await bucketStorage.getCheckpoint();

    const checksums = [...(await bucketStorage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([{ bucket: 'global[]', checksum: 1917136889, count: 1 }]);
    const checksums2 = [...(await bucketStorage.getChecksums(checkpoint + 1n, ['global[]'])).values()];
    expect(checksums2).toEqual([{ bucket: 'global[]', checksum: 1917136889, count: 1 }]);
  });

  testChecksumBatching(generateStorageFactory);
}

/**
 * This specifically tests an issue we ran into with MongoDB storage.
 *
 * Exposed as a separate test so we can test with more storage parameters.
 */
export function testChecksumBatching(generateStorageFactory: storage.TestStorageFactory) {
  test('checksums for multiple buckets', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  user:
    parameters: select request.user_id() as user_id
    data:
      - select id, description from test where user_id = bucket.user_id
`
    });
    const bucketStorage = factory.getInstance(syncRules);

    const sourceTable = TEST_TABLE;
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      for (let u of ['u1', 'u2', 'u3', 'u4']) {
        for (let t of ['t1', 't2', 't3', 't4']) {
          const id = `${t}_${u}`;
          await batch.save({
            sourceTable,
            tag: storage.SaveOperationTag.INSERT,
            after: {
              id,
              description: `${t} description`,
              user_id: u
            },
            afterReplicaId: test_utils.rid(id)
          });
        }
      }
      await batch.commit('1/1');
    });
    const { checkpoint } = await bucketStorage.getCheckpoint();

    bucketStorage.clearChecksumCache();
    const buckets = ['user["u1"]', 'user["u2"]', 'user["u3"]', 'user["u4"]'];
    const checksums = [...(await bucketStorage.getChecksums(checkpoint, buckets)).values()];
    checksums.sort((a, b) => a.bucket.localeCompare(b.bucket));
    expect(checksums).toEqual([
      { bucket: 'user["u1"]', count: 4, checksum: 346204588 },
      { bucket: 'user["u2"]', count: 4, checksum: 5261081 },
      { bucket: 'user["u3"]', count: 4, checksum: 134760718 },
      { bucket: 'user["u4"]', count: 4, checksum: -302639724 }
    ]);
  });
}
