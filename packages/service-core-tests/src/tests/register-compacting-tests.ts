import { storage } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

const TEST_TABLE = test_utils.makeTestTable('test', ['id']);

/**
 * @example
 * ```TypeScript
 * // Test with the default options - large batch sizes
 * describe('compacting buckets - default options', () => registerCompactTests(() => new MongoStorageFactory(), {}));
 *
 *  // Also test with the miniumum batch sizes, forcing usage of multiple batches internally
 * describe('compacting buckets - batched', () =>
 * compactTests(() => new MongoStorageFactory(), { clearBatchLimit: 2, moveBatchLimit: 1, moveBatchQueryLimit: 1 }));
 * ```
 */
export function registerCompactTests<CompactOptions extends storage.CompactOptions = storage.CompactOptions>(
  generateStorageFactory: storage.TestStorageFactory,
  compactOptions: CompactOptions
) {
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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]))
    );
    const dataBefore = batchBefore.batch.data;
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

    await bucketStorage.compact(compactOptions);

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]))
    );
    const dataAfter = batchAfter.batch.data;
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
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]))
    );
    const dataBefore = batchBefore.batch.data;
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

    await bucketStorage.compact(compactOptions);

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]))
    );
    const dataAfter = batchAfter.batch.data;
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

    await bucketStorage.compact(compactOptions);

    const batchAfter = await test_utils.oneFromAsync(
      bucketStorage.getBucketDataBatch(checkpoint2, new Map([['global[]', '0']]))
    );
    const dataAfter = batchAfter.batch.data;
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

  // TODO: maybe only run this test for Postgres. MongoDB is extremely slow. This might indicate an issue with Postgres.
  test('compacting (3)', { timeout: Infinity }, async () => {
    const sync_rules = test_utils.testRules(/* yaml */
    ` bucket_definitions:
        grouped:
          parameters: select b from test
          data:
            - select * from test where b = bucket.b`);

    await using factory = await generateStorageFactory();
    const bucketStorage = factory.getInstance(sync_rules);

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      /**
       * Repeatedly create operations which fall into different buckets.
       * The bucket operations are purposely interleaved as the op_id increases.
       * A large amount of operations are created here.
       * The default window of compacting operations is 10_000. This means the initial window will
       * contain operations from multiple buckets.
       */
      for (let count = 0; count < 8_000; count++) {
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

    await bucketStorage.compact(compactOptions);

    const batchAfter = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        new Map([
          ['grouped["b1"]', '0'],
          ['grouped["b2"]', '0']
        ])
      )
    );
    const dataAfter = batchAfter.flatMap((b) => b.batch.data);

    // The op_ids will vary between MongoDB and Postgres storage
    expect(dataAfter).toMatchObject(
      expect.arrayContaining([
        expect.objectContaining({ op: 'CLEAR', checksum: -2120931643 }),
        expect.objectContaining({
          op: 'PUT',
          object_type: 'test',
          object_id: 't1',
          checksum: 52221819,
          subkey: '6544e3899293153fa7b38331/117ab485-4b42-58a2-ab32-0053a22c3423',
          data: '{"id":"t1","b":"b1","value":"final"}'
        }),
        expect.objectContaining({ op: 'CLEAR', checksum: -1067381173 }),
        expect.objectContaining({
          op: 'PUT',
          object_type: 'test',
          object_id: 't2',
          checksum: 2126669493,
          subkey: '6544e3899293153fa7b38331/ec27c691-b47a-5d92-927a-9944feb89eee',
          data: '{"id":"t2","b":"b2","value":"final"}'
        })
      ])
    );
  });
}
