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

// These tests require a real Cosmos DB cluster. See test/COSMOS_DB_TESTING.md for setup.
//
// Why these can't run against standard MongoDB: the Cosmos DB workarounds involve
// different change stream initialization ordering (lazy ChangeStream + no
// startAtOperationTime) and wall-clock LSN precision (increment 0 instead of
// operationTime's real increments). These produce LSN comparison failures when
// mixed with standard MongoDB's operationTime-based checkpoints. A test flag that
// partially simulates Cosmos DB creates more problems than it solves — see the
// commit history on the cosmos branch for the full investigation.
const isCosmosDb = process.env.COSMOS_DB_TEST === 'true';
describe.skipIf(!isCosmosDb)('cosmosDbMode', () => {
  // 120s timeout — remote Cosmos DB clusters can have 10-30s latency spikes
  // for change stream delivery. Tests that poll for data need headroom.
  describeWithStorage({ timeout: 120_000 }, defineCosmosDbModeTests);
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

  test('data events not dropped after restart (lte guard)', async () => {
    // Verifies that the .lte() dedup guard in streamChangesInternal does NOT
    // drop data events on Cosmos DB after restart. On Cosmos DB, wallTime has
    // second precision (increment 0). Without the isCosmosDb guard, events
    // within the same wall-clock second as the last checkpoint would be silently
    // dropped — causing data loss.
    //
    // This test avoids getClientCheckpoint (which has its own timing issues)
    // and instead polls the storage directly until the data appears.

    // Phase 1: initial sync + streaming + checkpoint
    await using context = await openContext();
    const { db } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await db.createCollection('test_data');
    const collection = db.collection('test_data');

    await context.replicateSnapshot();
    context.startStreaming();

    await collection.insertOne({ description: 'phase1_data' });
    await context.getCheckpoint();

    // Stop streaming
    context.abort();
    await context.dispose();

    // Phase 2: restart and insert immediately (same second as last checkpoint)
    await using context2 = await openContext({ doNotClear: true });
    const db2 = context2.db;

    const activeContent = await context2.factory.getActiveSyncRulesContent();
    if (!activeContent) throw new Error('Active sync rules not found');
    (context2 as any).syncRulesContent = activeContent;
    context2.storage = context2.factory.getInstance(activeContent);

    context2.startStreaming();

    // Wait for the stream to be fully initialized — the streaming loop must
    // process its initial checkpoint before we insert test data. Without this,
    // the insert can happen before the ChangeStream's lazy aggregate command
    // is sent, causing the event to be missed entirely (not a .lte() issue).
    await context2.getCheckpoint({ timeout: 10_000 });

    // Insert — if .lte() drops same-second events, this data will never appear.
    const collection2 = db2.collection('test_data');
    const result2 = await collection2.insertOne({ description: 'post_restart_data' });
    const id2 = result2.insertedId;

    // Poll for the data by repeatedly calling getBucketData with a longer timeout.
    // We bypass the flaky getClientCheckpoint timing by polling until the data appears
    // or the timeout expires. If the .lte() guard drops same-second events, the data
    // will never appear — deterministic failure.
    // 50s timeout — remote Cosmos DB clusters can have 10-30s latency spikes.
    const deadline = Date.now() + 50_000;
    let found = false;
    while (Date.now() < deadline) {
      try {
        const data = await context2.getBucketData('global[]', undefined, { timeout: 2_000 });
        const match = data.find((op) => op.object_id === id2.toHexString() && op.op === 'PUT');
        if (match) {
          const parsed = JSON.parse(match.data as string);
          expect(parsed).toMatchObject({ description: 'post_restart_data' });
          found = true;
          break;
        }
      } catch {
        // getCheckpoint may timeout on first attempts — retry
      }
      await setTimeout(200);
    }

    expect(
      found,
      'Data event after restart was dropped — .lte() guard may be incorrectly filtering same-second events'
    ).toBe(true);
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

    // Wait for the stream to fully initialize and process the initial checkpoint
    // before inserting new data.
    await context2.getCheckpoint({ timeout: 10_000 });

    const collection2 = db2.collection('test_data');
    const result2 = await collection2.insertOne({ description: 'after_restart' });
    const id2 = result2.insertedId;

    // On Cosmos DB, wall-clock LSNs have second precision. The getClientCheckpoint
    // poll can resolve before data events are committed if the checkpoint LSN
    // matches the storage LSN (same second). This mirrors production behavior
    // where write checkpoints may take up to ~1s to resolve on a quiet system.
    // Use a polling approach with retries to handle this latency.
     // 50s timeout — remote Cosmos DB clusters can have 10-30s latency spikes.
    const deadline = Date.now() + 50_000;
    let found = false;
    while (Date.now() < deadline) {
      try {
        const dataAfter = await context2.getBucketData('global[]', undefined, { timeout: 2_000 });
        const afterRestartOps = dataAfter.filter((op) => op.object_id === id2.toHexString() && op.op === 'PUT');
        if (afterRestartOps.length >= 1) {
          expect(JSON.parse(afterRestartOps[0].data as string)).toMatchObject({ description: 'after_restart' });
          found = true;
          break;
        }
      } catch {
        // getCheckpoint may timeout — retry
      }
      await setTimeout(200);
    }
    expect(found, 'Data event after restart was not replicated within timeout').toBe(true);
  });
}
