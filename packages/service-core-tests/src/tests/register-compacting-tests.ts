import { storage } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

const TEST_TABLE = test_utils.makeTestTable('test', ['id']);

export function registerCompactTests(generateStorageFactory: storage.TestStorageFactory) {
  test('compacting (1)', async () => {
    const sync_rules = test_utils.testRules(`
bucket_definitions:
  global:
    data: [select * from test]
    `);

    await using factory = await generateStorageFactory();
    const bucketStorage = factory.getInstance(sync_rules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
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
    });

    const checkpoint = result!.flushed_op;

    const batchBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const dataBefore = batchBefore.chunkData.data;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, ['global[]']);

    expect(dataBefore).toMatchObject([
      {
        checksum: 2634521662,
        object_id: 't1',
        op: 'PUT',
        op_id: '1'
      },
      {
        checksum: 4243212114,
        object_id: 't2',
        op: 'PUT',
        op_id: '2'
      },
      {
        checksum: 4243212114,
        object_id: 't2',
        op: 'PUT',
        op_id: '3'
      }
    ]);

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1
    });

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const dataAfter = batchAfter.chunkData.data;
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, ['global[]']);

    expect(batchAfter.targetOp).toEqual(3n);
    expect(dataAfter).toMatchObject([
      {
        checksum: 2634521662,
        object_id: 't1',
        op: 'PUT',
        op_id: '1'
      },
      {
        checksum: 4243212114,
        op: 'MOVE',
        op_id: '2'
      },
      {
        checksum: 4243212114,
        object_id: 't2',
        op: 'PUT',
        op_id: '3'
      }
    ]);

    expect(checksumBefore.get('global[]')).toEqual(checksumAfter.get('global[]'));

    test_utils.validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (2)', async () => {
    const sync_rules = test_utils.testRules(`
bucket_definitions:
  global:
    data: [select * from test]
    `);

    await using factory = await generateStorageFactory();
    const bucketStorage = factory.getInstance(sync_rules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
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
    });

    const checkpoint = result!.flushed_op;

    const batchBefore = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const dataBefore = batchBefore.chunkData.data;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint, ['global[]']);

    expect(dataBefore).toMatchObject([
      {
        checksum: 2634521662,
        object_id: 't1',
        op: 'PUT',
        op_id: '1'
      },
      {
        checksum: 4243212114,
        object_id: 't2',
        op: 'PUT',
        op_id: '2'
      },
      {
        checksum: 4228978084,
        object_id: 't1',
        op: 'REMOVE',
        op_id: '3'
      },
      {
        checksum: 4243212114,
        object_id: 't2',
        op: 'PUT',
        op_id: '4'
      }
    ]);

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1
    });

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );
    const dataAfter = batchAfter.chunkData.data;
    const checksumAfter = await bucketStorage.getChecksums(checkpoint, ['global[]']);

    expect(batchAfter.targetOp).toEqual(4n);
    expect(dataAfter).toMatchObject([
      {
        checksum: -1778190028,
        op: 'CLEAR',
        op_id: '3'
      },
      {
        checksum: 4243212114,
        object_id: 't2',
        op: 'PUT',
        op_id: '4'
      }
    ]);
    expect(checksumBefore.get('global[]')).toEqual(checksumAfter.get('global[]'));

    test_utils.validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (3)', async () => {
    const sync_rules = test_utils.testRules(`
bucket_definitions:
  global:
    data: [select * from test]
    `);

    await using factory = await generateStorageFactory();
    const bucketStorage = factory.getInstance(sync_rules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
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
    });

    const checkpoint1 = result!.flushed_op;
    const checksumBefore = await bucketStorage.getChecksums(checkpoint1, ['global[]']);

    const result2 = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't2'
        },
        beforeReplicaId: 't2'
      });
    });
    const checkpoint2 = result2!.flushed_op;

    await bucketStorage.compact({
      clearBatchLimit: 2,
      moveBatchLimit: 1,
      moveBatchQueryLimit: 1
    });

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint2, new Map([['global[]', 0n]]))
    );
    const dataAfter = batchAfter.chunkData.data;
    const checksumAfter = await bucketStorage.getChecksums(checkpoint2, ['global[]']);

    expect(batchAfter.targetOp).toEqual(4n);
    expect(dataAfter).toMatchObject([
      {
        checksum: 1874612650,
        op: 'CLEAR',
        op_id: '4'
      }
    ]);
    expect(checksumAfter.get('global[]')).toEqual({
      bucket: 'global[]',
      count: 1,
      checksum: 1874612650
    });
  });

  test('compacting (4)', async () => {
    const sync_rules = test_utils.testRules(/* yaml */
    ` bucket_definitions:
        grouped:
          # The parameter query here is not important
          # We specifically don't want to create bucket_parameter records here
          # since the op_ids for bucket_data could vary between storage implementations.
          parameters: select 'b' as b
          data:
            - select * from test where b = bucket.b`);

    await using factory = await generateStorageFactory();
    const bucketStorage = factory.getInstance(sync_rules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
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
      }
    });

    const checkpoint = result!.flushed_op;

    await bucketStorage.compact({
      clearBatchLimit: 100,
      moveBatchLimit: 100,
      moveBatchQueryLimit: 100 // Larger limit for a larger window of operations
    });

    const batchAfter = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        new Map([
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
}
