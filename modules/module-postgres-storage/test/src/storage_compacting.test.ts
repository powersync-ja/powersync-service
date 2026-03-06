import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { PostgresCompactor } from '../../src/storage/PostgresCompactor.js';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Postgres Sync Bucket Storage Compact', () => register.registerCompactTests(POSTGRES_STORAGE_FACTORY));

describe('Postgres Compact - explicit bucket name', () => {
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

    const result = await (async () => {
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await writer.markAllSnapshotDone('1/1');
      const flushed = await writer.flush();
      await writer.commit('1/1');
      return flushed;
    })();

    const checkpoint = result!.flushed_op;

    // Compact with an explicit bucket name — exercises the this.buckets
    // iteration path, NOT the compactAllBuckets discovery path.
    await bucketStorage.compact({
      compactBuckets: [bucketRequest(syncRules, 'global[]').bucket],
      minBucketChanges: 1
    });

    const batch = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules, 'global[]', 0n)])
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
    const request = bucketRequest(syncRules, 'global[]');

    const result = await (async () => {
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
      await writer.markAllSnapshotDone('1/1');
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't1' },
        afterReplicaId: test_utils.rid('t1')
      });
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.DELETE,
        before: { id: 't1' },
        beforeReplicaId: test_utils.rid('t1')
      });
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 't2' },
        afterReplicaId: test_utils.rid('t2')
      });
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.DELETE,
        before: { id: 't2' },
        beforeReplicaId: test_utils.rid('t2')
      });
      const flushed = await writer.flush();
      await writer.commit('1/1');
      return flushed;
    })();

    const checkpoint = result!.flushed_op;
    const rowsBefore = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = test_utils.getBatchData(rowsBefore);
    const clearToOpId = BigInt(dataBefore[2].op_id);

    const compactor = new PostgresCompactor(factory.db, bucketStorage.group_id, {});
    // Trigger the private method directly
    await expect(compactor.clearBucketForTests(request.bucket, clearToOpId)).rejects.toThrow(
      /Unexpected PUT operation/
    );

    // The method wraps in a transaction; on assertion error the bucket must remain unchanged.
    const rowsAfter = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    expect(test_utils.getBatchData(rowsAfter)).toEqual(dataBefore);
  });
});
