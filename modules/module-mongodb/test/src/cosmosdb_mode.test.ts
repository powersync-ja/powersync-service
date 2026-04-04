import { setTimeout } from 'node:timers/promises';
import { describe, expect, test } from 'vitest';

import { createWriteCheckpoint } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';

import { MongoRouteAPIAdapter } from '@module/api/MongoRouteAPIAdapter.js';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { describeWithStorage, StorageVersionTestContext, TEST_CONNECTION_OPTIONS } from './util.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
`;

// These tests require a real Cosmos DB cluster. On standard MongoDB,
// the Cosmos DB code paths (wallTime timestamps, sentinel checkpoints,
// client.watch()) are not exercised because isCosmosDb is only set
// by server detection. Running these against standard MongoDB would
// test the standard code path, which is already covered by change_stream.test.ts.
//
// Why not a cosmosDbMode test flag? The Cosmos DB workarounds involve
// different change stream initialization ordering (lazy ChangeStream +
// no startAtOperationTime) and wall-clock LSN precision (increment 0
// instead of operationTime's real increments). These produce LSN
// comparison failures when mixed with standard MongoDB's operationTime-based
// checkpoints. A test flag that partially simulates Cosmos DB creates
// more problems than it solves.
const isCosmosDb = process.env.COSMOS_DB_TEST === 'true';
describe.skipIf(!isCosmosDb)('cosmosDbMode', () => {
  describeWithStorage({ timeout: 30_000 }, defineCosmosDbModeTests);
});

function defineCosmosDbModeTests({ factory, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof ChangeStreamTestContext.open>[1]) => {
    return ChangeStreamTestContext.open(factory, {
      ...options,
      storageVersion,
      streamOptions: {
        ...options?.streamOptions
      }
    });
  };

  test('basic replication in cosmosDbMode', async () => {
    await using context = await openContext();
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"`);

    await db.createCollection('test_data');
    const collection = db.collection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    const result = await collection.insertOne({ description: 'test1' });
    const test_id = result.insertedId;
    await collection.updateOne({ _id: test_id }, { $set: { description: 'test2' } });
    await collection.deleteOne({ _id: test_id });

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test1' }),
      test_utils.putOp('test_data', { id: test_id.toHexString(), description: 'test2' }),
      test_utils.removeOp('test_data', test_id.toHexString())
    ]);
  });

  test('sentinel checkpoint resolution', async () => {
    await using context = await openContext();
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"`);

    await db.createCollection('test_data');
    const collection = db.collection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    const insertResult = await collection.insertOne({ description: 'sentinel_test' });
    const insertedId = insertResult.insertedId.toHexString();

    // getCheckpoint() internally calls createCheckpoint, which should return a sentinel
    // format on Cosmos DB. The streaming loop must resolve it by matching the sentinel event.
    const checkpoint = await context.getCheckpoint();
    expect(checkpoint).toBeTruthy();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([test_utils.putOp('test_data', { id: insertedId, description: 'sentinel_test' })]);
  });

  test('keepalive in cosmosDbMode', async () => {
    await using context = await openContext({
      streamOptions: {
        keepaliveIntervalMs: 2000
      }
    });
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await db.createCollection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    // Wait for the initial checkpoint to be processed
    await context.getCheckpoint();

    // Wait past the keepalive interval so the idle keepalive path fires.
    // On Cosmos DB, this must NOT crash from parseResumeTokenTimestamp
    // (Cosmos DB resume tokens are base64, not hex).
    await setTimeout(3000);

    // Insert data after the keepalive interval to verify the stream is still alive
    const collection = db.collection('test_data');
    await collection.insertOne({ description: 'after_keepalive' });

    const data = await context.getBucketData('global[]');
    expect(data.length).toBeGreaterThanOrEqual(1);
    const lastOp = data[data.length - 1];
    expect(JSON.parse(lastOp.data as string)).toMatchObject({ description: 'after_keepalive' });
  });

  test('write checkpoint flow in cosmosDbMode', async () => {
    await using context = await openContext();
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await db.createCollection('test_data');
    const collection = db.collection('test_data');

    await context.replicateSnapshot();
    await context.markSnapshotConsistent();

    await using api = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS
    });

    context.startStreaming();

    // Wait until stream is active
    await context.getCheckpoint();

    // Insert data so the stream has something to process
    await collection.insertOne({ description: 'write_cp_test' });

    // Exercise the write checkpoint flow: createReplicationHead → createWriteCheckpoint polling.
    // On Cosmos DB, createReplicationHead passes null HEAD to the callback, and
    // createWriteCheckpoint must poll storage for the HEAD LSN.
    const result = await createWriteCheckpoint({
      userId: 'test_user',
      clientId: 'test_client',
      api,
      storage: context.factory
    });

    // The write checkpoint should resolve with a valid result
    expect(result).toBeTruthy();
    expect(result.writeCheckpoint).toBeTruthy();
    expect(result.replicationHead).toBeTruthy();
  });

  test('resume after restart in cosmosDbMode', async () => {
    // Phase 1: replicate some data, then stop
    await using context = await openContext();
    const { db } = context;
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"`);

    await db.createCollection('test_data');
    const collection = db.collection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    const result1 = await collection.insertOne({ description: 'before_restart' });
    const id1 = result1.insertedId;

    // Wait for the data to be replicated and checkpoint to advance
    await context.getCheckpoint();

    const dataBefore = await context.getBucketData('global[]');
    expect(dataBefore).toMatchObject([
      test_utils.putOp('test_data', { id: id1.toHexString(), description: 'before_restart' })
    ]);

    // Stop streaming (simulates a restart)
    context.abort();
    await context.dispose();

    // Phase 2: reopen without clearing and resume
    await using context2 = await openContext({ doNotClear: true });
    const db2 = context2.db;

    // Load the existing sync rules — must set both syncRulesContent and storage
    // for getBucketData() to work (it needs syncRulesContent for bucket versioning)
    const activeContent = await context2.factory.getActiveSyncRulesContent();
    if (!activeContent) throw new Error('Active sync rules not found after restart');
    (context2 as any).syncRulesContent = activeContent;
    context2.storage = context2.factory.getInstance(activeContent);

    context2.startStreaming();

    const collection2 = db2.collection('test_data');
    const result2 = await collection2.insertOne({ description: 'after_restart' });
    const id2 = result2.insertedId;

    const dataAfter = await context2.getBucketData('global[]');

    // Should contain the new insert after resume
    const afterRestartOps = dataAfter.filter((op) => op.object_id === id2.toHexString() && op.op === 'PUT');
    expect(afterRestartOps.length).toBeGreaterThanOrEqual(1);
    expect(JSON.parse(afterRestartOps[0].data as string)).toMatchObject({ description: 'after_restart' });
  });
}
