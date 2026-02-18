import { addChecksums, storage } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { bucketRequest, bucketRequestMap, bucketRequests } from './util.js';

export function registerCompactTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;
  const TEST_TABLE = test_utils.makeTestTable('test', ['id'], config);

  test('compacting (1)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data: [select * from test]
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't2'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const batchBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );
    const dataBefore = batchBefore.chunkData.data;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, bucketRequests(syncRules, ['global[]']));

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

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );
    const dataAfter = batchAfter.chunkData.data;
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, bucketRequests(syncRules, ['global[]']));
    bucketStorage.clearChecksumCache();
    const checksumAfter2 = await bucketStorage.getChecksums(checkpoint, bucketRequests(syncRules, ['global[]']));

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

    expect(checksumAfter.get(bucketRequest(syncRules, 'global[]'))).toEqual(
      checksumBefore.get(bucketRequest(syncRules, 'global[]'))
    );
    expect(checksumAfter2.get(bucketRequest(syncRules, 'global[]'))).toEqual(
      checksumBefore.get(bucketRequest(syncRules, 'global[]'))
    );

    test_utils.validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (2)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data: [select * from test]
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't1'
        },
        beforeReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't2'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.commit('1/1');
    });

    const checkpoint = result!.flushed_op;

    const batchBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );
    const dataBefore = batchBefore.chunkData.data;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, bucketRequests(syncRules, ['global[]']));

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

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );
    const dataAfter = batchAfter.chunkData.data;
    bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, bucketRequests(syncRules, ['global[]']));

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
    expect(checksumAfter.get(bucketRequest(syncRules, 'global[]'))).toEqual({
      ...checksumBefore.get(bucketRequest(syncRules, 'global[]')),
      count: 2
    });

    test_utils.validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (3)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data: [select * from test]
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2'
        },
        afterReplicaId: 't2'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't1'
        },
        beforeReplicaId: 't1'
      });

      await batch.commit('1/1');
    });

    const checkpoint1 = result!.flushed_op;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint1, bucketRequests(syncRules, ['global[]']));

    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't2'
        },
        beforeReplicaId: 't2'
      });
      await batch.commit('2/1');
    });
    const checkpoint2 = result2!.flushed_op;

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint2, bucketRequestMap(syncRules, [['global[]', 0n]]))
    );
    const dataAfter = batchAfter.chunkData.data;
    await bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, bucketRequests(syncRules, ['global[]']));

    expect(dataAfter).toMatchObject([
      {
        op: 'CLEAR'
      }
    ]);
    expect(checksumAfter.get(bucketRequest(syncRules, 'global[]'))).toEqual({
      bucket: bucketRequest(syncRules, 'global[]'),
      count: 1,
      checksum: dataAfter[0].checksum
    });
  });

  test('compacting (4)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      /* yaml */ content: ` bucket_definitions:
          grouped:
            # The parameter query here is not important
            # We specifically don't want to create bucket_parameter records here
            # since the op_ids for bucket_data could vary between storage implementations.
            parameters: select 'b' as b
            data:
              - select * from test where b = bucket.b`
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      /**
       * Repeatedly create operations which fall into different buckets.
       * The bucket operations are purposely interleaved as the op_id increases.
       * A large amount of operations are created here.
       * The configured window of compacting operations is 100. This means the initial window will
       * contain operations from multiple buckets.
       */
      for (let count = 0; count < 100; count++) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 't1',
            b: 'b1',
            value: 'start'
          },
          afterReplicaId: test_utils.rid('t1')
        });

        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: {
            id: 't1',
            b: 'b1',
            value: 'intermediate'
          },
          afterReplicaId: test_utils.rid('t1')
        });

        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 't2',
            b: 'b2',
            value: 'start'
          },
          afterReplicaId: test_utils.rid('t2')
        });

        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: {
            id: 't1',
            b: 'b1',
            value: 'final'
          },
          afterReplicaId: test_utils.rid('t1')
        });

        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: {
            id: 't2',
            b: 'b2',
            value: 'final'
          },
          afterReplicaId: test_utils.rid('t2')
        });

        await batch.commit('1/1');
      }
    });

    const checkpoint = result!.flushed_op;

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
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data: [select * from test]
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2'
        },
        afterReplicaId: 't2'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't1'
        },
        beforeReplicaId: 't1'
      });

      await batch.commit('1/1');
    });

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't2'
        },
        beforeReplicaId: 't2'
      });
      await batch.commit('2/1');
    });
    const checkpoint2 = result2!.flushed_op;
    await bucketStorage.clearChecksumCache();
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, bucketRequests(syncRules, ['global[]']));
    const globalChecksum = checksumAfter.get(bucketRequest(syncRules, 'global[]'));
    expect(globalChecksum).toMatchObject({
      bucket: bucketRequest(syncRules, 'global[]'),
      count: 4
    });

    // storage-specific checksum - just check that it does not change
    expect(globalChecksum).toMatchSnapshot();
  });

  test('partial checksums after compacting (2)', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data: [select * from test]
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't1'
        },
        afterReplicaId: 't1'
      });

      await batch.commit('1/1');
    });

    // Get checksums here just to populate the cache
    await bucketStorage.getChecksums(result!.flushed_op, bucketRequests(syncRules, ['global[]']));
    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't1'
        },
        beforeReplicaId: 't1'
      });
      await batch.commit('2/1');
    });

    await bucketStorage.compact({
      clearBatchLimit: 20,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const checkpoint2 = result2!.flushed_op;
    // Check that the checksum was correctly updated with the clear operation after having a cached checksum
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, bucketRequests(syncRules, ['global[]']));
    const globalChecksum = checksumAfter.get(bucketRequest(syncRules, 'global[]'));
    expect(globalChecksum).toMatchObject({
      bucket: bucketRequest(syncRules, 'global[]'),
      count: 1
    });
    // storage-specific checksum - just check that it does not change
    expect(globalChecksum).toMatchSnapshot();
  });
}
