import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, bucketRequestMap, register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { PostgresCompactor } from '../../src/storage/PostgresCompactor.js';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Postgres Sync Bucket Storage Compact', () => register.registerCompactTests(POSTGRES_STORAGE_FACTORY));

describe('Postgres Compact - explicit bucket name', () => {
  const TEST_TABLE = test_utils.makeTestTable('test', ['id'], POSTGRES_STORAGE_FACTORY);
  test('compacts a specific bucket by exact name', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
      `)
    );
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
      await batch.markAllSnapshotDone('1/1');
      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    // Compact with an explicit bucket name — exercises the this.buckets
    // iteration path, NOT the compactAllBuckets discovery path.
    await bucketStorage.compact({
      compactBuckets: [bucketRequest(syncRules, 'global[]')],
      minBucketChanges: 1
    });

    const batch = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );

    expect(batch.chunkData.data).toMatchObject([
      { op_id: '1', op: 'MOVE' },
      { op_id: '2', op: 'PUT', object_id: 't1' }
    ]);
  });

  test('clearBucket fails fast when prefix includes PUT', async () => {
    // This tests the specific implementation, to check that our operation type guard is working
    // for CLEAR compacting.
    await using factory = await POSTGRES_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
      `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    const bucket = bucketRequest(syncRules, 'global[]');

    await factory.db.query({
      statement: `
        INSERT INTO bucket_data (
          group_id,
          bucket_name,
          op_id,
          op,
          source_table,
          source_key,
          table_name,
          row_id,
          checksum,
          data,
          target_op
        ) VALUES
          ($1, $2, 1, 'REMOVE', NULL, NULL, NULL, NULL, 101, NULL, NULL),
          ($1, $2, 2, 'PUT', NULL, NULL, NULL, NULL, 202, NULL, NULL),
          ($1, $2, 3, 'REMOVE', NULL, NULL, NULL, NULL, 303, NULL, NULL),
          ($1, $2, 4, 'PUT', NULL, NULL, NULL, NULL, 404, NULL, NULL)
      `,
      params: [
        { type: 'int4', value: bucketStorage.group_id },
        { type: 'varchar', value: bucket }
      ]
    });

    const compactor = new PostgresCompactor(factory.db, bucketStorage.group_id, {});
    // Trigger the private method directly
    await expect(compactor.clearBucketForTests(bucket, 3n)).rejects.toThrow(/Unexpected PUT operation/);

    // The method wraps in a transaction; on assertion error the bucket must remain unchanged.
    const rowsAfter = await factory.db.sql`
      SELECT
        op_id,
        op,
        checksum
      FROM
        bucket_data
      WHERE
        group_id = ${{ type: 'int4', value: bucketStorage.group_id }}
        AND bucket_name = ${{ type: 'varchar', value: bucket }}
      ORDER BY
        op_id
    `.rows<{ op_id: bigint; op: string; checksum: bigint }>();

    expect(rowsAfter).toEqual([
      { op_id: 1n, op: 'REMOVE', checksum: 101n },
      { op_id: 2n, op: 'PUT', checksum: 202n },
      { op_id: 3n, op: 'REMOVE', checksum: 303n },
      { op_id: 4n, op: 'PUT', checksum: 404n }
    ]);
  });
});
