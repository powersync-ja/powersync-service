import { SaveOperationTag } from '@/storage/BucketStorage.js';
import { MongoCompactOptions } from '@/storage/mongo/MongoCompactor.js';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { validateCompactedBucket } from './bucket_validation.js';
import { oneFromAsync } from './stream_utils.js';
import { makeTestTable, MONGO_STORAGE_FACTORY, ZERO_LSN } from './util.js';

const TEST_TABLE = makeTestTable('test', ['id']);

// Test with the default options - large batch sizes
describe('compacting buckets - default options', () => compactTests({}));

// Also test with the miniumum batch sizes, forcing usage of multiple batches internally
describe('compacting buckets - batched', () =>
  compactTests({ clearBatchLimit: 2, moveBatchLimit: 1, moveBatchQueryLimit: 1 }));

function compactTests(compactOptions: MongoCompactOptions) {
  const factory = MONGO_STORAGE_FACTORY;

  test('compacting (1)', async () => {
    const sync_rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `);

    const storage = (await factory()).getInstance({ id: 1, sync_rules, slot_name: 'test' });

    const result = await storage.startBatch({ zeroLSN: ZERO_LSN }, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't1'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't2'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 't2'
        }
      });
    });

    const checkpoint = result!.flushed_op;

    const batchBefore = await oneFromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const dataBefore = batchBefore.batch.data;

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

    await storage.compact(compactOptions);

    const batchAfter = await oneFromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const dataAfter = batchAfter.batch.data;

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

    validateCompactedBucket(dataBefore, dataAfter);
  });

  test('compacting (2)', async () => {
    const sync_rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `);

    const storage = (await factory()).getInstance({ id: 1, sync_rules, slot_name: 'test' });

    const result = await storage.startBatch({ zeroLSN: ZERO_LSN }, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't1'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't2'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.DELETE,
        before: {
          id: 't1'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 't2'
        }
      });
    });

    const checkpoint = result!.flushed_op;

    const batchBefore = await oneFromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const dataBefore = batchBefore.batch.data;

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

    await storage.compact(compactOptions);

    const batchAfter = await oneFromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const dataAfter = batchAfter.batch.data;

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

    validateCompactedBucket(dataBefore, dataAfter);
  });
}
