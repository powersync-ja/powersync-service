import { putOp, removeOp } from '@core-tests/stream_utils.js';
import { MONGO_STORAGE_FACTORY } from '@core-tests/util.js';
import { BucketStorageFactory } from '@powersync/service-core';
import * as crypto from 'crypto';
import { describe, expect, test } from 'vitest';
import { walStreamTest } from './change_stream_utils.js';

type StorageFactory = () => Promise<BucketStorageFactory>;

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
`;

describe(
  'change stream - mongodb',
  function () {
    defineWalStreamTests(MONGO_STORAGE_FACTORY);
  },
  { timeout: 20_000 }
);

function defineWalStreamTests(factory: StorageFactory) {
  test(
    'replicating basic values',
    walStreamTest(factory, async (context) => {
      const { db } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description, num FROM "test_data"`);

      await context.replicateSnapshot();

      context.startStreaming();

      const collection = db.collection('test_data');
      const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n });
      const test_id = result.insertedId;

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        putOp('test_data', { id: test_id.toHexString(), description: 'test1', num: 1152921504606846976n })
      ]);
    })
  );

  test(
    'replicating case sensitive table',
    walStreamTest(factory, async (context) => {
      const { db } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT _id as id, description FROM "test_DATA"
      `);

      await context.replicateSnapshot();

      context.startStreaming();

      const collection = db.collection('test_DATA');
      const result = await collection.insertOne({ description: 'test1' });
      const test_id = result.insertedId.toHexString();

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([putOp('test_DATA', { id: test_id, description: 'test1' })]);
    })
  );

  test(
    'replicating large values',
    walStreamTest(factory, async (context) => {
      const { db } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT _id as id, name, description FROM "test_data"
      `);

      await context.replicateSnapshot();
      context.startStreaming();

      const largeDescription = crypto.randomBytes(20_000).toString('hex');

      const collection = db.collection('test_data');
      const result = await collection.insertOne({ name: 'test1', description: largeDescription });
      const test_id = result.insertedId;

      await collection.updateOne({ _id: test_id }, { $set: { name: 'test2' } });

      const data = await context.getBucketData('global[]');
      expect(data.slice(0, 1)).toMatchObject([
        putOp('test_data', { id: test_id.toHexString(), name: 'test1', description: largeDescription })
      ]);
      expect(data.slice(1)).toMatchObject([
        putOp('test_data', { id: test_id.toHexString(), name: 'test2', description: largeDescription })
      ]);
    })
  );

  // Not implemented yet
  test.skip(
    'replicating dropCollection',
    walStreamTest(factory, async (context) => {
      const { db } = context;
      const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
  by_test_data:
    parameters: SELECT _id as id FROM test_data WHERE id = token_parameters.user_id
    data: []
`;
      await context.updateSyncRules(syncRuleContent);
      await context.replicateSnapshot();
      context.startStreaming();

      const collection = db.collection('test_data');
      const result = await collection.insertOne({ description: 'test1' });
      const test_id = result.insertedId.toHexString();

      await collection.drop();

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        putOp('test_data', { id: test_id, description: 'test1' }),
        removeOp('test_data', test_id)
      ]);
    })
  );

  test(
    'initial sync',
    walStreamTest(factory, async (context) => {
      const { db } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      const collection = db.collection('test_data');
      const result = await collection.insertOne({ description: 'test1' });
      const test_id = result.insertedId.toHexString();

      await context.replicateSnapshot();
      context.startStreaming();

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([putOp('test_data', { id: test_id, description: 'test1' })]);
    })
  );

  // Not correctly implemented yet
  test.skip(
    'large record',
    walStreamTest(factory, async (context) => {
      await context.updateSyncRules(`bucket_definitions:
      global:
        data:
          - SELECT _id as id, description, other FROM "test_data"`);
      const { db } = context;

      await context.replicateSnapshot();

      // 16MB
      const largeDescription = crypto.randomBytes(8_000_000 - 100).toString('hex');

      const collection = db.collection('test_data');
      const result = await collection.insertOne({ description: largeDescription });
      const test_id = result.insertedId;

      await collection.updateOne({ _id: test_id }, { $set: { name: 't2' } });
      context.startStreaming();

      const data = await context.getBucketData('global[]');
      expect(data.length).toEqual(2);
      const row = JSON.parse(data[0].data as string);
      delete row.description;
      expect(row).toEqual({ id: test_id.toHexString() });
      delete data[0].data;
      expect(data[0]).toMatchObject({
        object_id: test_id.toHexString(),
        object_type: 'test_data',
        op: 'PUT',
        op_id: '1'
      });
    })
  );

  test(
    'table not in sync rules',
    walStreamTest(factory, async (context) => {
      const { db } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await context.replicateSnapshot();

      context.startStreaming();

      const collection = db.collection('test_donotsync');
      const result = await collection.insertOne({ description: 'test' });

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([]);
    })
  );
}