import { storage } from '@powersync/service-core';
import { register, TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Postgres Sync Bucket Storage Compact', () => register.registerCompactTests(POSTGRES_STORAGE_FACTORY));

/**
 * Regression test: the compactor's pagination query uses U+FFFF as a sentinel
 * upper bound for bucket_name comparisons. Under locale-aware collation
 * (e.g. en_US.UTF-8), alphanumeric strings sort AFTER U+FFFF, causing the
 * initial query to return zero rows and skip compaction entirely.
 * COLLATE "C" on that comparison forces byte-order, where U+FFFF sorts last.
 *
 * This test inserts two PUT ops for the same row (t1) into a single bucket,
 * then compacts without specifying compactBuckets — triggering the
 * bucket == null path where bucketUpper = U+FFFF. After compaction, the older
 * PUT should be converted to a MOVE. Without COLLATE "C", no compaction
 * occurs and both ops remain as PUT.
 */
describe('Postgres Compact - COLLATE regression', () => {
  test('compacts when bucket_name is compared against U+FFFF sentinel', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data: [select * from test]
      `
    });
    const bucketStorage = factory.getInstance(syncRules);

    // Insert two PUTs for the same row — the older one should become a MOVE after compaction
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

    // Compact WITHOUT specifying compactBuckets — this triggers bucketUpper = U+FFFF
    await bucketStorage.compact({
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1
    });

    const batch = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    // Op 1 should be converted to MOVE; op 2 stays as PUT.
    // Without COLLATE "C", both remain as PUT (compaction was skipped).
    expect(batch.chunkData.data).toMatchObject([
      { op_id: '1', op: 'MOVE' },
      { op_id: '2', op: 'PUT', object_id: 't1' }
    ]);
  });
});
