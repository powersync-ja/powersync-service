import { putOp, removeOp } from '@core-tests/stream_utils.js';
import { MONGO_STORAGE_FACTORY } from '@core-tests/util.js';
import { BucketStorageFactory } from '@powersync/service-core';
import * as crypto from 'crypto';
import * as mongo from 'mongodb';
import { setTimeout } from 'node:timers/promises';
import { describe, expect, test } from 'vitest';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { PostImagesOption } from '@module/types/types.js';

type StorageFactory = () => Promise<BucketStorageFactory>;

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
`;

describe('change stream - mongodb', { timeout: 20_000 }, function () {
  defineChangeStreamTests(MONGO_STORAGE_FACTORY);
});

function defineChangeStreamTests(factory: StorageFactory) {
  test('replicating basic values', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description, num FROM "test_data"`);

    await db.createCollection('test_data', {
      changeStreamPreAndPostImages: { enabled: false }
    });
    const collection = db.collection('test_data');

    await context.replicateSnapshot();

    context.startStreaming();

    const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n });
    const test_id = result.insertedId;
    await setTimeout(30);
    await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } });
    await setTimeout(30);
    await collection.replaceOne({ _id: test_id }, { description: 'test3' });
    await setTimeout(30);
    await collection.deleteOne({ _id: test_id });

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data', { id: test_id.toHexString(), description: 'test1', num: 1152921504606846976n }),
      putOp('test_data', { id: test_id.toHexString(), description: 'test2', num: 1152921504606846976n }),
      putOp('test_data', { id: test_id.toHexString(), description: 'test3' }),
      removeOp('test_data', test_id.toHexString())
    ]);
  });

  test('replicating wildcard', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description, num FROM "test_%"`);

    await db.createCollection('test_data', {
      changeStreamPreAndPostImages: { enabled: false }
    });
    const collection = db.collection('test_data');

    const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n });
    const test_id = result.insertedId;

    await context.replicateSnapshot();

    context.startStreaming();

    await setTimeout(30);
    await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } });

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data', { id: test_id.toHexString(), description: 'test1', num: 1152921504606846976n }),
      putOp('test_data', { id: test_id.toHexString(), description: 'test2', num: 1152921504606846976n })
    ]);
  });

  test('updateLookup - no fullDocument available', async () => {
    await using context = await ChangeStreamTestContext.open(factory, { postImages: PostImagesOption.OFF });
    const { db, client } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description, num FROM "test_data"`);

    await db.createCollection('test_data', {
      changeStreamPreAndPostImages: { enabled: false }
    });
    const collection = db.collection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    const session = client.startSession();
    let test_id: mongo.ObjectId | undefined;
    try {
      await session.withTransaction(async () => {
        const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n }, { session });
        test_id = result.insertedId;
        await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } }, { session });
        await collection.replaceOne({ _id: test_id }, { description: 'test3' }, { session });
        await collection.deleteOne({ _id: test_id }, { session });
      });
    } finally {
      await session.endSession();
    }

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data', { id: test_id!.toHexString(), description: 'test1', num: 1152921504606846976n }),
      // fullDocument is not available at the point this is replicated, resulting in it treated as a remove
      removeOp('test_data', test_id!.toHexString()),
      putOp('test_data', { id: test_id!.toHexString(), description: 'test3' }),
      removeOp('test_data', test_id!.toHexString())
    ]);
  });

  test('postImages - autoConfigure', async () => {
    // Similar to the above test, but with postImages enabled.
    // This resolves the consistency issue.
    await using context = await ChangeStreamTestContext.open(factory, { postImages: PostImagesOption.AUTO_CONFIGURE });
    const { db, client } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description, num FROM "test_data"`);

    await db.createCollection('test_data', {
      // enabled: false here, but autoConfigure will enable it.
      changeStreamPreAndPostImages: { enabled: false }
    });
    const collection = db.collection('test_data');

    await context.replicateSnapshot();

    context.startStreaming();

    const session = client.startSession();
    let test_id: mongo.ObjectId | undefined;
    try {
      await session.withTransaction(async () => {
        const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n }, { session });
        test_id = result.insertedId;
        await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } }, { session });
        await collection.replaceOne({ _id: test_id }, { description: 'test3' }, { session });
        await collection.deleteOne({ _id: test_id }, { session });
      });
    } finally {
      await session.endSession();
    }

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data', { id: test_id!.toHexString(), description: 'test1', num: 1152921504606846976n }),
      // The postImage helps us get this data
      putOp('test_data', { id: test_id!.toHexString(), description: 'test2', num: 1152921504606846976n }),
      putOp('test_data', { id: test_id!.toHexString(), description: 'test3' }),
      removeOp('test_data', test_id!.toHexString())
    ]);
  });

  test('postImages - on', async () => {
    // Similar to postImages - autoConfigure, but does not auto-configure.
    // changeStreamPreAndPostImages must be manually configured.
    await using context = await ChangeStreamTestContext.open(factory, { postImages: PostImagesOption.READ_ONLY });
    const { db, client } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description, num FROM "test_data"`);

    await db.createCollection('test_data', {
      changeStreamPreAndPostImages: { enabled: true }
    });
    const collection = db.collection('test_data');

    await context.replicateSnapshot();

    context.startStreaming();

    const session = client.startSession();
    let test_id: mongo.ObjectId | undefined;
    try {
      await session.withTransaction(async () => {
        const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n }, { session });
        test_id = result.insertedId;
        await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } }, { session });
        await collection.replaceOne({ _id: test_id }, { description: 'test3' }, { session });
        await collection.deleteOne({ _id: test_id }, { session });
      });
    } finally {
      await session.endSession();
    }

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data', { id: test_id!.toHexString(), description: 'test1', num: 1152921504606846976n }),
      // The postImage helps us get this data
      putOp('test_data', { id: test_id!.toHexString(), description: 'test2', num: 1152921504606846976n }),
      putOp('test_data', { id: test_id!.toHexString(), description: 'test3' }),
      removeOp('test_data', test_id!.toHexString())
    ]);
  });

  test('replicating case sensitive table', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
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
  });

  test('replicating large values', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
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
  });

  test('replicating dropCollection', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
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
  });

  test('replicating renameCollection', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
    const { db } = context;
    const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data1"
      - SELECT _id as id, description FROM "test_data2"
`;
    await context.updateSyncRules(syncRuleContent);
    await context.replicateSnapshot();
    context.startStreaming();

    const collection = db.collection('test_data1');
    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId.toHexString();

    await collection.rename('test_data2');

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data1', { id: test_id, description: 'test1' }),
      removeOp('test_data1', test_id),
      putOp('test_data2', { id: test_id, description: 'test1' })
    ]);
  });

  test('initial sync', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const collection = db.collection('test_data');
    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId.toHexString();

    await context.replicateSnapshot();
    context.startStreaming();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', { id: test_id, description: 'test1' })]);
  });

  // Not correctly implemented yet
  test.skip('large record', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
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
  });

  test('collection not in sync rules', async () => {
    await using context = await ChangeStreamTestContext.open(factory);
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();

    context.startStreaming();

    const collection = db.collection('test_donotsync');
    const result = await collection.insertOne({ description: 'test' });

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
  });

  test('postImages - new collection with postImages enabled', async () => {
    await using context = await ChangeStreamTestContext.open(factory, { postImages: PostImagesOption.AUTO_CONFIGURE });
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_%"`);

    await context.replicateSnapshot();

    await db.createCollection('test_data', {
      // enabled: true here - everything should work
      changeStreamPreAndPostImages: { enabled: true }
    });
    const collection = db.collection('test_data');
    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId;
    await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } });

    context.startStreaming();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      putOp('test_data', { id: test_id!.toHexString(), description: 'test1' }),
      putOp('test_data', { id: test_id!.toHexString(), description: 'test2' })
    ]);
  });

  test('postImages - new collection with postImages disabled', async () => {
    await using context = await ChangeStreamTestContext.open(factory, { postImages: PostImagesOption.AUTO_CONFIGURE });
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data%"`);

    await context.replicateSnapshot();

    await db.createCollection('test_data', {
      // enabled: false here, but autoConfigure will enable it.
      // Unfortunately, that is too late, and replication must be restarted.
      changeStreamPreAndPostImages: { enabled: false }
    });
    const collection = db.collection('test_data');
    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId;
    await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } });

    context.startStreaming();

    await expect(() => context.getBucketData('global[]')).rejects.toMatchObject({
      message: expect.stringContaining('stream was configured to require a post-image for all update events')
    });
  });
}
