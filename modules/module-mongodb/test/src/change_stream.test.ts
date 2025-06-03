import * as crypto from 'crypto';
import { setTimeout } from 'node:timers/promises';
import { describe, expect, test, vi } from 'vitest';

import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';

import { PostImagesOption } from '@module/types/types.js';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { describeWithStorage } from './util.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
`;

describe('change stream', () => {
  describeWithStorage({ timeout: 20_000 }, defineChangeStreamTests);
});

function defineChangeStreamTests(factory: storage.TestStorageFactory) {
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
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test1', num: 1152921504606846976n }),
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test2', num: 1152921504606846976n }),
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test3' }),
      test_utils.removeOp('test_data', test_id.toHexString())
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
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test1', num: 1152921504606846976n }),
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test2', num: 1152921504606846976n })
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
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test1', num: 1152921504606846976n }),
      // fullDocument is not available at the point this is replicated, resulting in it treated as a remove
      test_utils.removeOp('test_data', test_id!.toHexString()),
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test3' }),
      test_utils.removeOp('test_data', test_id!.toHexString())
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
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test1', num: 1152921504606846976n }),
      // The postImage helps us get this data
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2', num: 1152921504606846976n }),
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test3' }),
      test_utils.removeOp('test_data', test_id!.toHexString())
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
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test1', num: 1152921504606846976n }),
      // The postImage helps us get this data
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2', num: 1152921504606846976n }),
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test3' }),
      test_utils.removeOp('test_data', test_id!.toHexString())
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

    await db.createCollection('test_DATA');
    await context.replicateSnapshot();

    context.startStreaming();

    const collection = db.collection('test_DATA');
    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId.toHexString();

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([test_utils.putOp('test_DATA', { id: test_id, description: 'test1' })]);
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
    await db.createCollection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    const largeDescription = crypto.randomBytes(20_000).toString('hex');

    const collection = db.collection('test_data');
    const result = await collection.insertOne({ name: 'test1', description: largeDescription });
    const test_id = result.insertedId;

    await collection.updateOne({ _id: test_id }, { $set: { name: 'test2' } });

    const data = await context.getBucketData('global[]');
    expect(data.slice(0, 1)).toMatchObject([
      test_utils.putOp('test_data', { id: test_id.toHexString(), name: 'test1', description: largeDescription })
    ]);
    expect(data.slice(1)).toMatchObject([
      test_utils.putOp('test_data', { id: test_id.toHexString(), name: 'test2', description: largeDescription })
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
      test_utils.putOp('test_data', { id: test_id, description: 'test1' }),
      test_utils.removeOp('test_data', test_id)
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
      test_utils.putOp('test_data1', { id: test_id, description: 'test1' }),
      test_utils.removeOp('test_data1', test_id),
      test_utils.putOp('test_data2', { id: test_id, description: 'test1' })
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
    expect(data).toMatchObject([test_utils.putOp('test_data', { id: test_id, description: 'test1' })]);
  });

  test('large record', async () => {
    // Test a large update.

    // Without $changeStreamSplitLargeEvent, we get this error:
    // MongoServerError: PlanExecutor error during aggregation :: caused by :: BSONObj size: 33554925 (0x20001ED) is invalid.
    // Size must be between 0 and 16793600(16MB)

    await using context = await ChangeStreamTestContext.open(factory);
    await context.updateSyncRules(`bucket_definitions:
      global:
        data:
          - SELECT _id as id, name, other FROM "test_data"`);
    const { db } = context;

    await db.createCollection('test_data');

    await context.replicateSnapshot();

    const collection = db.collection('test_data');
    const result = await collection.insertOne({ name: 't1' });
    const test_id = result.insertedId;

    // 12MB field.
    // The field appears twice in the ChangeStream event, so the total size
    // is > 16MB.

    // We don't actually have this description field in the sync rules,
    // That causes other issues, not relevant for this specific test.
    const largeDescription = crypto.randomBytes(12000000 / 2).toString('hex');

    await collection.updateOne({ _id: test_id }, { $set: { description: largeDescription } });
    context.startStreaming();

    const data = await context.getBucketData('global[]');
    expect(data.length).toEqual(2);
    const row1 = JSON.parse(data[0].data as string);
    expect(row1).toEqual({ id: test_id.toHexString(), name: 't1' });
    delete data[0].data;
    expect(data[0]).toMatchObject({
      object_id: test_id.toHexString(),
      object_type: 'test_data',
      op: 'PUT',
      op_id: '1'
    });
    const row2 = JSON.parse(data[1].data as string);
    expect(row2).toEqual({ id: test_id.toHexString(), name: 't1' });
    delete data[1].data;
    expect(data[1]).toMatchObject({
      object_id: test_id.toHexString(),
      object_type: 'test_data',
      op: 'PUT',
      op_id: '2'
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
      // An extra op here, since this triggers a snapshot in addition to getting the event.
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2' }),
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test1' }),
      test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2' })
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

  test('recover from error', async () => {
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
    await collection.insertOne({ description: 'test1', num: 1152921504606846976n });

    await context.replicateSnapshot();

    // Simulate an error
    await context.storage!.reportError(new Error('simulated error'));
    expect((await context.factory.getActiveSyncRulesContent())?.last_fatal_error).toEqual('simulated error');

    // startStreaming() should automatically clear the error.
    context.startStreaming();

    // getBucketData() creates a checkpoint that clears the error, so we don't do that
    // Just wait, and check that the error is cleared automatically.
    await vi.waitUntil(
      async () => {
        const error = (await context.factory.getActiveSyncRulesContent())?.last_fatal_error;
        return error == null;
      },
      { timeout: 2_000 }
    );
  });
}
