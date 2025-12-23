import { storage } from '@powersync/service-core';
import { TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

/** Small batch limits to force pagination during compaction. */
const SMALL_BATCH_OPTIONS = {
  clearBatchLimit: 2,
  moveBatchLimit: 1,
  moveBatchQueryLimit: 2,
  minBucketChanges: 1
} as const;

describe('Postgres Compactor - Pagination', () => {
  test('paginates correctly when data exceeds batch size', async () => {
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
      for (let i = 1; i <= 5; i++) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `item-${i}` },
          afterReplicaId: test_utils.rid(`item-${i}`)
        });
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: `item-${i}` },
          afterReplicaId: test_utils.rid(`item-${i}`)
        });
      }
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const dataBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    expect(dataBefore.chunkData.data.length).toBe(10);
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, ['global[]']);

    await bucketStorage.compact(SMALL_BATCH_OPTIONS);

    const dataAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, ['global[]']);
    expect(checksumAfter.get('global[]')?.checksum, 'checksum should be preserved').toEqual(checksumBefore.get('global[]')?.checksum);

    test_utils.validateCompactedBucket(dataBefore.chunkData.data, dataAfter.chunkData.data);
  });

  test('paginates across multiple buckets in "all" mode', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  alpha:
    data:
      - SELECT * FROM test WHERE bucket = 'alpha'
  beta:
    data:
      - SELECT * FROM test WHERE bucket = 'beta'
  gamma:
    data:
      - SELECT * FROM test WHERE bucket = 'gamma'
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      for (const bucketName of ['alpha', 'beta', 'gamma']) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `${bucketName}-1`, bucket: bucketName },
          afterReplicaId: test_utils.rid(`${bucketName}-1`)
        });
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: `${bucketName}-1`, bucket: bucketName },
          afterReplicaId: test_utils.rid(`${bucketName}-1`)
        });
      }
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const alphaBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['alpha[]', 0n]]))
    );
    const betaBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['beta[]', 0n]]))
    );
    const gammaBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['gamma[]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['alpha[]', 'beta[]', 'gamma[]']);

    await bucketStorage.compact(SMALL_BATCH_OPTIONS);

    const alphaAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['alpha[]', 0n]]))
    );
    const betaAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['beta[]', 0n]]))
    );
    const gammaAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['gamma[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['alpha[]', 'beta[]', 'gamma[]']);
    expect(checksumsAfter.get('alpha[]')?.checksum, 'alpha[] checksum should be preserved').toEqual(checksumsBefore.get('alpha[]')?.checksum);
    expect(checksumsAfter.get('beta[]')?.checksum, 'beta[] checksum should be preserved').toEqual(checksumsBefore.get('beta[]')?.checksum);
    expect(checksumsAfter.get('gamma[]')?.checksum, 'gamma[] checksum should be preserved').toEqual(checksumsBefore.get('gamma[]')?.checksum);

    test_utils.validateCompactedBucket(alphaBefore.chunkData.data, alphaAfter.chunkData.data);
    test_utils.validateCompactedBucket(betaBefore.chunkData.data, betaAfter.chunkData.data);
    test_utils.validateCompactedBucket(gammaBefore.chunkData.data, gammaAfter.chunkData.data);
  });

  test('paginates in "prefix" mode across parameterized buckets', async () => {
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
      for (const userId of ['u1', 'u2', 'u3']) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `t-${userId}`, user_id: userId },
          afterReplicaId: test_utils.rid(`t-${userId}`)
        });
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: `t-${userId}`, user_id: userId },
          afterReplicaId: test_utils.rid(`t-${userId}`)
        });
      }
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const user1Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const user2Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u2"]', 0n]]))
    );
    const user3Before = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u3"]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['user["u1"]', 'user["u2"]', 'user["u3"]']);

    await bucketStorage.compact({
      ...SMALL_BATCH_OPTIONS,
      compactBuckets: ['user']
    });

    const user1After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u1"]', 0n]]))
    );
    const user2After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u2"]', 0n]]))
    );
    const user3After = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['user["u3"]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['user["u1"]', 'user["u2"]', 'user["u3"]']);
    expect(checksumsAfter.get('user["u1"]')?.checksum, 'user["u1"] checksum should be preserved').toEqual(checksumsBefore.get('user["u1"]')?.checksum);
    expect(checksumsAfter.get('user["u2"]')?.checksum, 'user["u2"] checksum should be preserved').toEqual(checksumsBefore.get('user["u2"]')?.checksum);
    expect(checksumsAfter.get('user["u3"]')?.checksum, 'user["u3"] checksum should be preserved').toEqual(checksumsBefore.get('user["u3"]')?.checksum);

    test_utils.validateCompactedBucket(user1Before.chunkData.data, user1After.chunkData.data);
    test_utils.validateCompactedBucket(user2Before.chunkData.data, user2After.chunkData.data);
    test_utils.validateCompactedBucket(user3Before.chunkData.data, user3After.chunkData.data);
  });

  test('paginates in "exact" mode within a single bucket', async () => {
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
      for (let i = 1; i <= 4; i++) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `item-${i}` },
          afterReplicaId: test_utils.rid(`item-${i}`)
        });
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: `item-${i}` },
          afterReplicaId: test_utils.rid(`item-${i}`)
        });
      }
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const dataBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    expect(dataBefore.chunkData.data.length).toBe(8);
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, ['global[]']);

    await bucketStorage.compact({
      ...SMALL_BATCH_OPTIONS,
      compactBuckets: ['global[]']
    });

    const dataAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, ['global[]']);
    expect(checksumAfter.get('global[]')?.checksum, 'checksum should be preserved').toEqual(checksumBefore.get('global[]')?.checksum);

    test_utils.validateCompactedBucket(dataBefore.chunkData.data, dataAfter.chunkData.data);
  });

  test('handles empty bucket gracefully', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  empty:
    data:
      - SELECT * FROM test WHERE 1 = 0
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
        after: { id: 'g1' },
        afterReplicaId: test_utils.rid('g1')
      });
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const emptyBefore = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['empty[]', 0n]]))
    );
    expect(emptyBefore.length).toBe(0);

    await bucketStorage.compact({
      ...SMALL_BATCH_OPTIONS,
      compactBuckets: ['empty[]']
    });

    const emptyAfter = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['empty[]', 0n]]))
    );
    expect(emptyAfter.length).toBe(0);

    const globalAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    expect(globalAfter.chunkData.data.length).toBe(1);
    expect(globalAfter.chunkData.data[0].op).toBe('PUT');
  });

  test('compacts multiple exact-match buckets in a single call', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  first:
    data:
      - SELECT * FROM test WHERE bucket = 'first'
  second:
    data:
      - SELECT * FROM test WHERE bucket = 'second'
  third:
    data:
      - SELECT * FROM test WHERE bucket = 'third'
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      for (const bucketName of ['first', 'second', 'third']) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `${bucketName}-1`, bucket: bucketName },
          afterReplicaId: test_utils.rid(`${bucketName}-1`)
        });
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: `${bucketName}-1`, bucket: bucketName },
          afterReplicaId: test_utils.rid(`${bucketName}-1`)
        });
      }
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const firstBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['first[]', 0n]]))
    );
    const secondBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['second[]', 0n]]))
    );
    const thirdBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['third[]', 0n]]))
    );
    const checksumsBefore = await bucketStorage.getChecksums(checkpoint, ['first[]', 'second[]', 'third[]']);

    await bucketStorage.compact({
      ...SMALL_BATCH_OPTIONS,
      compactBuckets: ['first[]', 'second[]']
    });

    const firstAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['first[]', 0n]]))
    );
    const secondAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['second[]', 0n]]))
    );
    const thirdAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['third[]', 0n]]))
    );

    bucketStorage.clearChecksumCache();
    const checksumsAfter = await bucketStorage.getChecksums(checkpoint, ['first[]', 'second[]', 'third[]']);
    expect(checksumsAfter.get('first[]')?.checksum, 'first[] checksum should be preserved').toEqual(checksumsBefore.get('first[]')?.checksum);
    expect(checksumsAfter.get('second[]')?.checksum, 'second[] checksum should be preserved').toEqual(checksumsBefore.get('second[]')?.checksum);
    expect(checksumsAfter.get('third[]')?.checksum, 'third[] checksum should be preserved').toEqual(checksumsBefore.get('third[]')?.checksum);

    test_utils.validateCompactedBucket(firstBefore.chunkData.data, firstAfter.chunkData.data);
    test_utils.validateCompactedBucket(secondBefore.chunkData.data, secondAfter.chunkData.data);

    expect(thirdAfter.chunkData.data, 'third[] data should remain unchanged').toEqual(thirdBefore.chunkData.data);
  });
});
