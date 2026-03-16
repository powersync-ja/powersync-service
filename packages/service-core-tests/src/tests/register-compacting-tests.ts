import { addChecksums, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { bucketRequest } from '../test-utils/test-utils-index.js';
import { bucketRequestMap, bucketRequests } from './util.js';

export function registerCompactTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;

  test('compacting (1)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1'
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2'
      },
      afterReplicaId: test_utils.rid('t2')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 't2'
      },
      afterReplicaId: test_utils.rid('t2')
    });

    await writer.commit('1/1');
    await writer.flush();

    const checkpoint = writer.last_flushed_op!;

    const request = bucketRequest(syncRules, 'global[]');

    const batchBefore = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = batchBefore.chunkData.data;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, [request]);

    expect(dataBefore).toMatchObject([
      {
        object_id: 't1',
        op: 'PUT',
        op_id: '1'
      },
      {
        object_id: 't2',
        op: 'PUT',
        op_id: '2'
      },
      {
        object_id: 't2',
        op: 'PUT',
        op_id: '3'
      }
    ]);
    expect(batchBefore.targetOp).toEqual(null);

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batchAfter = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = batchAfter.chunkData.data;
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, [request]);
    bucketStorage.clearChecksumCache();
    const checksumAfter2 = await bucketStorage.getChecksums(checkpoint, [request]);

    expect(batchAfter.targetOp).toEqual(3n);
    expect(dataAfter).toMatchObject([
      dataBefore[0],
      {
        checksum: dataBefore[1].checksum,
        op: 'MOVE',
        op_id: '2'
      },
      {
        checksum: dataBefore[2].checksum,
        object_id: 't2',
        op: 'PUT',
        op_id: '3'
      }
    ]);

    expect(checksumAfter.get(request.bucket)).toEqual(checksumBefore.get(request.bucket));
    expect(checksumAfter2.get(request.bucket)).toEqual(checksumBefore.get(request.bucket));

    test_utils.validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (2)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1'
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2'
      },
      afterReplicaId: test_utils.rid('t2')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't1'
      },
      beforeReplicaId: test_utils.rid('t1')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 't2'
      },
      afterReplicaId: test_utils.rid('t2')
    });

    await writer.commit('1/1');
    await writer.flush();

    const checkpoint = writer.last_flushed_op!;
    const request = bucketRequest(syncRules, 'global[]');

    const batchBefore = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = batchBefore.chunkData.data;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, [request]);

    // op_id sequence depends on the storage implementation
    expect(dataBefore).toMatchObject([
      {
        object_id: 't1',
        op: 'PUT'
      },
      {
        object_id: 't2',
        op: 'PUT'
      },
      {
        object_id: 't1',
        op: 'REMOVE'
      },
      {
        object_id: 't2',
        op: 'PUT'
      }
    ]);

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batchAfter = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = batchAfter.chunkData.data;
    bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, [request]);

    expect(batchAfter.targetOp).toBeLessThanOrEqual(checkpoint);
    expect(dataAfter).toMatchObject([
      {
        checksum: addChecksums(
          addChecksums(dataBefore[0].checksum as number, dataBefore[1].checksum as number),
          dataBefore[2].checksum as number
        ),
        op: 'CLEAR'
      },
      {
        checksum: dataBefore[3].checksum,
        object_id: 't2',
        op: 'PUT'
      }
    ]);
    expect(checksumAfter.get(request.bucket)).toEqual({
      ...checksumBefore.get(request.bucket),
      count: 2
    });

    test_utils.validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (3)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2'
      },
      afterReplicaId: 't2'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't1'
      },
      beforeReplicaId: 't1'
    });

    await writer.commit('1/1');
    await writer.flush();

    const checkpoint1 = writer.last_flushed_op!;
    const request = bucketRequest(syncRules, 'global[]');
    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable2 = await test_utils.resolveTestTable(writer2, 'test', ['id'], config);
    await writer2.save({
      sourceTable: testTable2,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't2'
      },
      beforeReplicaId: 't2'
    });
    await writer2.commit('2/1');
    await writer2.flush();
    const checkpoint2 = writer2.last_flushed_op!;

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batchAfter = await test_utils.oneFromAsync(bucketStorage.getBucketDataBatch(checkpoint2, [request]));
    const dataAfter = batchAfter.chunkData.data;
    await bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, [request]);

    expect(dataAfter).toMatchObject([
      {
        op: 'CLEAR'
      }
    ]);
    expect(checksumAfter.get(request.bucket)).toEqual({
      bucket: request.bucket,
      count: 1,
      checksum: dataAfter[0].checksum
    });
  });

  test('compacting (4)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(` bucket_definitions:
          grouped:
            # The parameter query here is not important
            # We specifically don't want to create bucket_parameter records here
            # since the op_ids for bucket_data could vary between storage implementations.
            parameters: select 'b' as b
            data:
              - select * from test where b = bucket.b`)
    );
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    /**
     * Repeatedly create operations which fall into different buckets.
     * The bucket operations are purposely interleaved as the op_id increases.
     * A large amount of operations are created here.
     * The configured window of compacting operations is 100. This means the initial window will
     * contain operations from multiple buckets.
     */
    for (let count = 0; count < 100; count++) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          b: 'b1',
          value: 'start'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't1',
          b: 'b1',
          value: 'intermediate'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          b: 'b2',
          value: 'start'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't1',
          b: 'b1',
          value: 'final'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't2',
          b: 'b2',
          value: 'final'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await writer.commit('1/1');
    }

    await writer.flush();

    const checkpoint = writer.last_flushed_op!;

    await bucketStorage.compact({
      clearBatchLimit: 100,
      moveBatchLimit: 100,
      moveBatchQueryLimit: 100, // Larger limit for a larger window of operations
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batchAfter = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        bucketRequestMap(syncRules, [
          ['grouped["b1"]', 0n],
          ['grouped["b2"]', 0n]
        ])
      )
    );
    const dataAfter = batchAfter.flatMap((b) => b.chunkData.data);

    // The op_ids will vary between MongoDB and Postgres storage
    expect(dataAfter).toMatchObject(
      expect.arrayContaining([
        { op_id: '497', op: 'CLEAR', checksum: -937074151 },
        {
          op_id: '499',
          op: 'PUT',
          object_type: 'test',
          object_id: 't1',
          checksum: 52221819,
          subkey: '6544e3899293153fa7b38331/117ab485-4b42-58a2-ab32-0053a22c3423',
          data: '{"id":"t1","b":"b1","value":"final"}'
        },
        { op_id: '498', op: 'CLEAR', checksum: -234380197 },
        {
          op_id: '500',
          op: 'PUT',
          object_type: 'test',
          object_id: 't2',
          checksum: 2126669493,
          subkey: '6544e3899293153fa7b38331/ec27c691-b47a-5d92-927a-9944feb89eee',
          data: '{"id":"t2","b":"b2","value":"final"}'
        }
      ])
    );
  });

  test('partial checksums after compacting', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2'
      },
      afterReplicaId: 't2'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't1'
      },
      beforeReplicaId: 't1'
    });

    await writer.commit('1/1');
    await writer.flush();

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable2 = await test_utils.resolveTestTable(writer2, 'test', ['id'], config);
    await writer2.save({
      sourceTable: testTable2,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't2'
      },
      beforeReplicaId: 't2'
    });
    await writer2.commit('2/1');
    await writer2.flush();
    const checkpoint2 = writer2.last_flushed_op!;
    const request = bucketRequest(syncRules, 'global[]');
    await bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, [request]);
    const globalChecksum = checksumAfter.get(request.bucket);
    expect(globalChecksum).toMatchObject({
      bucket: request.bucket,
      count: 4
    });

    // storage-specific checksum - just check that it does not change
    expect(globalChecksum).toMatchSnapshot();
  });

  test('partial checksums after compacting (2)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 't1'
      },
      afterReplicaId: 't1'
    });

    await writer.commit('1/1');
    await writer.flush();

    // Get checksums here just to populate the cache
    await bucketStorage.getChecksums(writer.last_flushed_op!, bucketRequests(syncRules, ['global[]']));
    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable2 = await test_utils.resolveTestTable(writer2, 'test', ['id'], config);
    await writer2.save({
      sourceTable: testTable2,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't1'
      },
      beforeReplicaId: 't1'
    });
    await writer2.commit('2/1');
    await writer2.flush();

    await bucketStorage.compact({
      clearBatchLimit: 20,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const checkpoint2 = writer2.last_flushed_op!;
    const request = bucketRequest(syncRules, 'global[]');
    // Check that the checksum was correctly updated with the clear operation after having a cached checksum
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, [request]);
    const globalChecksum = checksumAfter.get(request.bucket);
    expect(globalChecksum).toMatchObject({
      bucket: request.bucket,
      count: 1
    });
    // storage-specific checksum - just check that it does not change
    expect(globalChecksum).toMatchSnapshot();
  });

  test('defaults maxOpId to current checkpoint', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
      `)
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 't1' },
      afterReplicaId: test_utils.rid('t1')
    });
    await writer.commit('1/1');
    await writer.flush();

    const checkpoint1 = writer.last_flushed_op!;

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable2 = await test_utils.resolveTestTable(writer2, 'test', ['id'], config);
    // This is flushed but not committed (does not advance the checkpoint)
    await writer2.save({
      sourceTable: testTable2,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 't1' },
      afterReplicaId: test_utils.rid('t1')
    });
    await writer2.flush();
    const checkpoint2 = writer2.last_flushed_op!;

    const checkpointBeforeCompact = await bucketStorage.getCheckpoint();
    expect(checkpointBeforeCompact.checkpoint).toEqual(checkpoint1);

    // With default options, Postgres compaction should use the active checkpoint.
    await bucketStorage.compact({
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batchAfterDefaultCompact = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint2, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );

    // Operation 1 should remain a PUT because op_id=2 is above the default maxOpId checkpoint.
    expect(batchAfterDefaultCompact.chunkData.data).toMatchObject([
      { op_id: '1', op: 'PUT', object_id: 't1' },
      { op_id: '2', op: 'PUT', object_id: 't1' }
    ]);
  });
}
