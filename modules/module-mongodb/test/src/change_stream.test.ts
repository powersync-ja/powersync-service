import * as crypto from 'crypto';
import { setTimeout } from 'node:timers/promises';
import { describe, expect, test, vi } from 'vitest';

import { mongo } from '@powersync/lib-service-mongodb';
import { createWriteCheckpoint, storage } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';

import { MongoRouteAPIAdapter } from '@module/api/MongoRouteAPIAdapter.js';
import { PostImagesOption } from '@module/types/types.js';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { describeWithStorage, StorageVersionTestContext, TEST_CONNECTION_OPTIONS } from './util.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
`;

describe('change stream', () => {
  describeWithStorage({ timeout: 20_000 }, defineChangeStreamTests);
});

function defineChangeStreamTests({ factory, storageVersion }: StorageVersionTestContext) {
  const supportsConcurrentSnapshots = storageVersion >= 3;

  const openContext = (options?: Parameters<typeof ChangeStreamTestContext.open>[1]) => {
    return ChangeStreamTestContext.open(factory, { ...options, storageVersion });
  };
  test('replicating basic values', async () => {
    await using context = await openContext({
      mongoOptions: { postImages: PostImagesOption.READ_ONLY }
    });
    const { db } = context;
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

    const result = await collection.insertOne({ description: 'test1', num: 1152921504606846976n });
    const test_id = result.insertedId;
    await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } });
    await collection.replaceOne({ _id: test_id }, { description: 'test3' });
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
    await using context = await openContext();
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

  test('does not resurrect rows deleted while snapshotting', async () => {
    // Special case for testing:
    // 1. Row A exists.
    // 2. Start a snapshot and streaming.
    // 3. Row A is deleted.
    // 4. Streaming sees the delete, writes the delete.
    // 5. Snapshot still sees row A and writes it.
    // The streaming delete should win, using skipExistingRows. For that to work, we rely on soft deletes.
    // To trigger this case, we need the delete to trigger and process the delete in between the snapshot read and the snapshot write, which we can do using beforeBatchFlush.

    let collection!: mongo.Collection;
    let testId!: mongo.ObjectId;
    let interceptedSnapshotFlush = false;
    let sourceDeleteWritten = false;
    let sourceDeleteFlushBatch: storage.BucketStorageBatch | undefined;
    const streamDeleteFlushed = Promise.withResolvers<void>();

    await using context = await openContext({
      streamOptions: {
        snapshotChunkLength: 1,
        storageHooks: {
          beforeBatchFlush: async (batch) => {
            if (!batch.skipExistingRows && sourceDeleteWritten && sourceDeleteFlushBatch == null) {
              sourceDeleteFlushBatch = batch;
            }

            // skipExistingRows means we're busy with snapshotting.
            if (!batch.skipExistingRows || interceptedSnapshotFlush) {
              return;
            }

            interceptedSnapshotFlush = true;
            await collection.deleteOne({ _id: testId });
            sourceDeleteWritten = true;
            if (!supportsConcurrentSnapshots) {
              return;
            }
            await Promise.race([
              streamDeleteFlushed.promise,
              setTimeout(10_000).then(() => {
                throw new Error('Timed out waiting for streamed delete to flush');
              })
            ]);
          },
          afterBatchFlush: async (batch) => {
            if (batch == sourceDeleteFlushBatch) {
              streamDeleteFlushed.resolve();
            }
          }
        }
      }
    });
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await db.createCollection('test_data', {
      changeStreamPreAndPostImages: { enabled: false }
    });
    testId = new mongo.ObjectId();
    collection = db.collection('test_data');
    await collection.insertOne({ _id: testId, description: 'stale snapshot row' });

    await context.replicateSnapshot();

    expect(interceptedSnapshotFlush).toBe(true);
    await context.getCheckpoint();

    const data = await context.getBucketData('global[]');
    expect(test_utils.reduceBucket(data).slice(1)).toEqual([]);
  });

  test('updateLookup - no fullDocument available', async () => {
    await using context = await openContext({
      mongoOptions: { postImages: PostImagesOption.OFF }
    });
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
    await using context = await openContext({
      mongoOptions: { postImages: PostImagesOption.AUTO_CONFIGURE }
    });
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
    await using context = await openContext({
      mongoOptions: { postImages: PostImagesOption.READ_ONLY }
    });
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
    await using context = await openContext();
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

  test('replicating from multiple databases in the same cluster', async () => {
    await using context = await openContext();
    const { client, db } = context;
    const otherDb = client.db(`${db.databaseName}_other_${storageVersion}`);
    await otherDb.dropDatabase();
    await using _ = {
      [Symbol.asyncDispose]: async () => {
        await otherDb.dropDatabase();
      }
    };

    await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT _id as id, description FROM "${db.databaseName}"."test_data_default"
            - SELECT _id as id, description FROM "${otherDb.databaseName}"."test_data_other"
      `);

    await db.createCollection('test_data_default');
    await otherDb.createCollection('test_data_other');
    await context.replicateSnapshot();
    context.startStreaming();

    const defaultResult = await db.collection('test_data_default').insertOne({ description: 'default db' });
    const otherResult = await otherDb.collection('test_data_other').insertOne({ description: 'other db' });

    const data = await context.getBucketData('global[]');

    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining(
          test_utils.putOp('test_data_default', {
            id: defaultResult.insertedId.toHexString(),
            description: 'default db'
          })
        ),
        expect.objectContaining(
          test_utils.putOp('test_data_other', {
            id: otherResult.insertedId.toHexString(),
            description: 'other db'
          })
        )
      ])
    );
  });

  test('replicating large values', async () => {
    await using context = await openContext();
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
    await using context = await openContext();
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
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced).toEqual([]);

    expect(data).toMatchObject([
      test_utils.putOp('test_data', { id: test_id, description: 'test1' }),
      test_utils.removeOp('test_data', test_id)
    ]);
  });

  test('replicating renameCollection', async () => {
    await using context = await openContext();
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
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced).toMatchObject([test_utils.putOp('test_data2', { id: test_id, description: 'test1' })]);

    expect(data).toMatchObject([
      test_utils.putOp('test_data1', { id: test_id, description: 'test1' }),
      test_utils.removeOp('test_data1', test_id),
      test_utils.putOp('test_data2', { id: test_id, description: 'test1' })
    ]);
  });

  test.runIf(supportsConcurrentSnapshots)(
    'collection recreated while queued snapshot is waiting does not stall checkpoints',
    async () => {
      // This is a regression test for a specific timing issue in concurrent snapshot logic.
      // Regression flow:
      // 1. Create test_b and wait until streaming has queued a concurrent snapshot for its source table.
      // 2. Pause that queued snapshot before it reads storage state.
      // 3. Drop test_b and wait until streaming has deleted the queued source table row from storage.
      // 4. Recreate the test_b collection, but do not insert replacement data yet. This leaves the collection
      //    visible to namespace-based resolution without giving normal streaming an insert event that could
      //    discover and queue the replacement source table.
      // 5. Release the stale queued snapshot and wait for a checkpoint. The old implementation re-resolved by
      //    namespace here, created a new source table row, then skipped it because its id did not match the
      //    queued id. That left snapshot_done=false with no queue item to finish it, stalling checkpoints.
      // 6. After the checkpoint proves the stale snapshot did not orphan a source table, insert replacement data
      //    and verify normal streaming still replicates it.
      const testBSnapshotStarted = Promise.withResolvers<void>();
      const releaseTestBSnapshot = Promise.withResolvers<void>();
      let pausedTestBSnapshot = false;
      let queuedTestBTable: storage.SourceTable | null = null;
      let waitForStreamingFlush: PromiseWithResolvers<void> | null = null;
      const waitFor = async (promise: Promise<void>, description: string) => {
        await Promise.race([
          promise,
          setTimeout(10_000).then(() => {
            throw new Error(`Timed out waiting for ${description}`);
          })
        ]);
      };

      await using context = await openContext({
        streamOptions: {
          snapshotChunkLength: 1,
          snapshotHooks: {
            beforeSnapshotStarted: async (table) => {
              if (table.name != 'test_b' || pausedTestBSnapshot) {
                return;
              }

              queuedTestBTable = table;
              pausedTestBSnapshot = true;
              testBSnapshotStarted.resolve();
              await releaseTestBSnapshot.promise;
            }
          },
          storageHooks: {
            afterBatchFlush: async (batch) => {
              if (batch.skipExistingRows) {
                return;
              }

              waitForStreamingFlush?.resolve();
            }
          }
        }
      });
      const { db } = context;
      const waitForSourceTableDropped = async (table: storage.SourceTable) => {
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          await using writer = await context.storage!.createWriter(test_utils.BATCH_OPTIONS);
          if ((await writer.getSourceTableStatus(table)) == null) {
            return;
          }
          await setTimeout(50);
        }
        throw new Error('Timed out waiting for test_b source table to be dropped');
      };
      const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_%"
`;
      await context.updateSyncRules(syncRuleContent);
      await context.replicateSnapshot();
      context.startStreaming();

      try {
        await db.createCollection('test_a');
        const testA = await db.collection('test_a').insertOne({ description: 'test a' });

        await db.createCollection('test_b');
        waitForStreamingFlush = Promise.withResolvers<void>();
        await db.collection('test_b').insertOne({ description: 'old test b' });
        await waitFor(waitForStreamingFlush.promise, 'old test_b streaming flush');
        await waitFor(testBSnapshotStarted.promise, 'test_b snapshot to start');

        if (queuedTestBTable == null) {
          throw new Error('test_b snapshot started without a queued source table');
        }

        await db.collection('test_b').drop();
        await waitForSourceTableDropped(queuedTestBTable);

        await db.createCollection('test_b');
        releaseTestBSnapshot.resolve();
        await context.getCheckpoint({ timeout: 10_000 });

        const result = await db.collection('test_b').insertOne({ description: 'new test b' });

        const data = await context.getBucketData('global[]', undefined, { timeout: 10_000 });
        const reduced = test_utils.reduceBucket(data).slice(1);
        expect(reduced).toMatchObject([
          test_utils.putOp('test_a', { id: testA.insertedId.toHexString(), description: 'test a' }),
          test_utils.putOp('test_b', { id: result.insertedId.toHexString(), description: 'new test b' })
        ]);
      } finally {
        releaseTestBSnapshot.resolve();
      }
    }
  );

  test('initial sync', async () => {
    await using context = await openContext();
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const collection = db.collection('test_data');
    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId.toHexString();

    await context.replicateSnapshot();
    // Note: snapshot is only consistent some time into the streaming request.
    // At the point that we get the first acknowledged checkpoint, as is required
    // for getBucketData(), the data should be consistent.
    context.startStreaming();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([test_utils.putOp('test_data', { id: test_id, description: 'test1' })]);
  });

  test('coalesces standalone checkpoints when backlog is buffered', async () => {
    await using context = await openContext();
    await context.updateSyncRules(BASIC_SYNC_RULES);

    let commitCount = 0;
    // This relies on internals to count how often checkpoints are committed
    context.storage!.registerListener({
      batchStarted: (batch) => {
        const originalCommit = batch.commit.bind(batch);
        batch.commit = async (...args) => {
          commitCount += 1;
          return await originalCommit(...args);
        };
      }
    });

    await context.replicateSnapshot();
    await context.markSnapshotConsistent();
    await using api = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS
    });

    context.startStreaming();

    // Wait until the stream is active and caught up, then start counting from zero.
    await context.getCheckpoint();
    commitCount = 0;

    // Create a large number of write checkpoints together.
    // We could alternatively use createCheckpoint() directly, but this gives more of
    // an end-to-end test of the checkpointing behavior under load.
    const checkpointCount = 30;
    await Promise.all(
      Array.from({ length: checkpointCount }, (i) =>
        createWriteCheckpoint({
          userId: 'test_user',
          clientId: 'test_client' + i,
          api,
          storage: context.factory
        })
      )
    );

    // Wait for the checkpoints to be processed.
    await context.getCheckpoint();

    // We need at least 1 commit.
    expect(commitCount).toBeGreaterThan(0);
    // The previous implementation greated 1 commit per checkpoint, which is bad for performance.
    // We expect a small number here - typically 2-10, but allow for anything less than the total number of checkpoints.
    expect(commitCount).toBeLessThan(checkpointCount + 1);
  });

  test('large record', async () => {
    // Test a large update.

    // Without $changeStreamSplitLargeEvent, we get this error:
    // MongoServerError: PlanExecutor error during aggregation :: caused by :: BSONObj size: 33554925 (0x20001ED) is invalid.
    // Size must be between 0 and 16793600(16MB)

    await using context = await openContext();
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

    // We don't actually have this description field in the sync config,
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

  test('collection not in sync config', async () => {
    await using context = await openContext();
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
    await using context = await openContext({
      mongoOptions: { postImages: PostImagesOption.AUTO_CONFIGURE }
    });
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
    if (data.length == 3) {
      expect(data).toMatchObject([
        // An extra op here, since this triggers a snapshot in addition to getting the event.
        test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2' }),
        test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test1' }),
        test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2' })
      ]);
    } else {
      expect(data).toMatchObject([
        test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test1' }),
        test_utils.putOp('test_data', { id: test_id!.toHexString(), description: 'test2' })
      ]);
    }
  });

  test('postImages - new collection with postImages disabled', async () => {
    await using context = await openContext({
      mongoOptions: { postImages: PostImagesOption.AUTO_CONFIGURE }
    });
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
    await using context = await openContext();
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
    await context.getCheckpoint();

    // Simulate an error
    await context.storage!.reportError(new Error('simulated error'));
    const syncRules = await context.factory.getActiveSyncRulesContent();
    expect(syncRules).toBeTruthy();
    expect(syncRules?.last_fatal_error).toEqual('simulated error');

    // The next checkpoint should clear the error.
    await context.getCheckpoint();

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
