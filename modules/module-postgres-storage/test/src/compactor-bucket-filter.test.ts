import { storage } from '@powersync/service-core';
import { TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

/**
 * Default compact options used across all bucket filtering tests.
 * These values are intentionally small to make compaction predictable in tests.
 */
const DEFAULT_COMPACT_OPTIONS = {
  clearBatchLimit: 2,
  moveBatchLimit: 1,
  moveBatchQueryLimit: 1,
  minBucketChanges: 1
} as const;

describe('Postgres Compactor - Bucket Filtering', () => {

  test('should compact only the specified exact bucket', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test WHERE bucket = 'global'
  other:
    data:
      - SELECT * FROM test WHERE bucket = 'other'
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g1', bucket: 'global' },
        afterReplicaId: test_utils.rid('g1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'g1', bucket: 'global' },
        afterReplicaId: test_utils.rid('g1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'o1', bucket: 'other' },
        afterReplicaId: test_utils.rid('o1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'o1', bucket: 'other' },
        afterReplicaId: test_utils.rid('o1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const globalBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const otherBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['other[]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['global[]', 'other[]']);

    await bucketStorage.compact({
      ...DEFAULT_COMPACT_OPTIONS,
      compactBuckets: ['global[]']
    });

    const globalAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const otherAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['other[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['global[]', 'other[]']);
    expect(checksumsAfter.get('global[]')?.checksum, 'global[] checksum should be preserved').toEqual(checksumsBefore.get('global[]')?.checksum);
    expect(checksumsAfter.get('other[]')?.checksum, 'other[] checksum should be preserved').toEqual(checksumsBefore.get('other[]')?.checksum);

    test_utils.validateCompactedBucket(globalBefore.chunkData.data, globalAfter.chunkData.data);

    expect(otherAfter.chunkData.data.every(op => op.op === 'PUT'), 'other[] should only have PUT operations (not compacted)').toBe(true);
    expect(otherAfter.chunkData.data, 'other[] data should remain unchanged').toEqual(otherBefore.chunkData.data);
  });

  test('should compact all buckets matching a prefix', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  user:
    parameters: SELECT id FROM users
    data:
      - SELECT * FROM test WHERE user_id = bucket.id
  global:
    data:
      - SELECT * FROM test WHERE bucket = 'global'
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1', user_id: 'u1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't1', user_id: 'u1' },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't2', user_id: 'u2' },
        afterReplicaId: test_utils.rid('t2')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't2', user_id: 'u2' },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g1', bucket: 'global' },
        afterReplicaId: test_utils.rid('g1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'g1', bucket: 'global' },
        afterReplicaId: test_utils.rid('g1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const user1Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const user2Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u2"]', 0n]]))
    );
    const globalBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['user["u1"]', 'user["u2"]', 'global[]']);

    await bucketStorage.compact({
      ...DEFAULT_COMPACT_OPTIONS,
      compactBuckets: ['user']
    });

    const user1After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const user2After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u2"]', 0n]]))
    );
    const globalAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['user["u1"]', 'user["u2"]', 'global[]']);
    expect(checksumsAfter.get('user["u1"]')?.checksum, 'user["u1"] checksum should be preserved').toEqual(checksumsBefore.get('user["u1"]')?.checksum);
    expect(checksumsAfter.get('user["u2"]')?.checksum, 'user["u2"] checksum should be preserved').toEqual(checksumsBefore.get('user["u2"]')?.checksum);
    expect(checksumsAfter.get('global[]')?.checksum, 'global[] checksum should be preserved').toEqual(checksumsBefore.get('global[]')?.checksum);

    test_utils.validateCompactedBucket(user1Before.chunkData.data, user1After.chunkData.data);
    test_utils.validateCompactedBucket(user2Before.chunkData.data, user2After.chunkData.data);

    expect(globalAfter.chunkData.data, 'global[] data should remain unchanged (not matched by prefix)').toEqual(globalBefore.chunkData.data);
  });

  test('should compact a specific parameterized bucket by exact name', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  user:
    parameters: SELECT id FROM users
    data:
      - SELECT * FROM test WHERE user_id = bucket.id
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1', user_id: 'u1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't1', user_id: 'u1' },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't2', user_id: 'u2' },
        afterReplicaId: test_utils.rid('t2')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't2', user_id: 'u2' },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const user1Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const user2Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u2"]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['user["u1"]', 'user["u2"]']);

    await bucketStorage.compact({
      ...DEFAULT_COMPACT_OPTIONS,
      compactBuckets: ['user["u1"]']
    });

    const user1After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const user2After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u2"]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['user["u1"]', 'user["u2"]']);
    expect(checksumsAfter.get('user["u1"]')?.checksum, 'user["u1"] checksum should be preserved').toEqual(checksumsBefore.get('user["u1"]')?.checksum);
    expect(checksumsAfter.get('user["u2"]')?.checksum, 'user["u2"] checksum should be preserved').toEqual(checksumsBefore.get('user["u2"]')?.checksum);

    test_utils.validateCompactedBucket(user1Before.chunkData.data, user1After.chunkData.data);

    expect(user2After.chunkData.data, 'user["u2"] data should remain unchanged (not targeted)').toEqual(user2Before.chunkData.data);
  });

  test('should not compact any buckets when filter matches nothing', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const globalBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    await bucketStorage.compact({
      ...DEFAULT_COMPACT_OPTIONS,
      compactBuckets: ['nonexistent']
    });

    const globalAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    expect(globalAfter.chunkData.data, 'global[] data should remain unchanged (nonexistent prefix)').toEqual(globalBefore.chunkData.data);
  });

  test('should not compact any buckets when compactBuckets is empty array', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const globalBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    await bucketStorage.compact({
      ...DEFAULT_COMPACT_OPTIONS,
      compactBuckets: []
    });

    const globalAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    expect(globalAfter.chunkData.data, 'global[] data should remain unchanged (empty compactBuckets array)').toEqual(globalBefore.chunkData.data);
  });

  test('should compact buckets using both exact match and prefix filters together', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test WHERE bucket = 'global'
  user:
    parameters: SELECT id FROM users
    data:
      - SELECT * FROM test WHERE user_id = bucket.id
  other:
    data:
      - SELECT * FROM test WHERE bucket = 'other'
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g1', bucket: 'global' },
        afterReplicaId: test_utils.rid('g1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'g1', bucket: 'global' },
        afterReplicaId: test_utils.rid('g1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1', user_id: 'u1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't1', user_id: 'u1' },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'o1', bucket: 'other' },
        afterReplicaId: test_utils.rid('o1')
      });
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'o1', bucket: 'other' },
        afterReplicaId: test_utils.rid('o1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const globalBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const user1Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const otherBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['other[]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['global[]', 'user["u1"]', 'other[]']);

    await bucketStorage.compact({
      ...DEFAULT_COMPACT_OPTIONS,
      compactBuckets: ['global[]', 'user']
    });

    const globalAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const user1After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const otherAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['other[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['global[]', 'user["u1"]', 'other[]']);
    expect(checksumsAfter.get('global[]')?.checksum, 'global[] checksum should be preserved').toEqual(checksumsBefore.get('global[]')?.checksum);
    expect(checksumsAfter.get('user["u1"]')?.checksum, 'user["u1"] checksum should be preserved').toEqual(checksumsBefore.get('user["u1"]')?.checksum);
    expect(checksumsAfter.get('other[]')?.checksum, 'other[] checksum should be preserved').toEqual(checksumsBefore.get('other[]')?.checksum);

    test_utils.validateCompactedBucket(globalBefore.chunkData.data, globalAfter.chunkData.data);
    test_utils.validateCompactedBucket(user1Before.chunkData.data, user1After.chunkData.data);

    expect(otherAfter.chunkData.data, 'other[] data should remain unchanged (not in compactBuckets list)').toEqual(otherBefore.chunkData.data);
  });

});
