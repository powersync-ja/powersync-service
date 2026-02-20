import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { register, TEST_TABLE, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Postgres Sync Bucket Storage Compact', () => register.registerCompactTests(POSTGRES_STORAGE_FACTORY));

describe('Postgres Compact - explicit bucket name', () => {
  test('compacts a specific bucket by exact name', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
      `)
    );
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(
      test_utils.BATCH_OPTIONS,
      async (batch) => {
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
      }
    );

    const checkpoint = result!.flushed_op;

    // Compact with an explicit bucket name — exercises the this.buckets
    // iteration path, NOT the compactAllBuckets discovery path.
    await bucketStorage.compact({
      compactBuckets: ['global[]'],
      minBucketChanges: 1
    });

    const batch = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    expect(batch.chunkData.data).toMatchObject([
      { op_id: '1', op: 'MOVE' },
      { op_id: '2', op: 'PUT', object_id: 't1' }
    ]);
  });
});
