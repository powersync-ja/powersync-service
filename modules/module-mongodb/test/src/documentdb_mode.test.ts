import { setTimeout } from 'node:timers/promises';
import { describe, expect, test, vi } from 'vitest';

import { createWriteCheckpoint } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';

import { MongoRouteAPIAdapter } from '@module/api/MongoRouteAPIAdapter.js';
import { SentinelLSN } from '@module/common/SentinelLSN.js';
import { createDocumentDbCheckpointLsn, STANDALONE_CHECKPOINT_ID } from '@module/replication/MongoRelation.js';
import { CHECKPOINTS_COLLECTION } from '@module/replication/replication-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { connectMongoData, describeWithStorage, StorageVersionTestContext, TEST_CONNECTION_OPTIONS } from './util.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM "test_data"
`;

// These tests require a real DocumentDB cluster. See test/DOCUMENTDB_TESTING.md for setup.
//
// Why these can't run against standard MongoDB: the DocumentDB workarounds involve
// different change stream initialization ordering (lazy ChangeStream + no
// startAtOperationTime) and wall-clock LSN precision (increment 0 instead of
// operationTime's real increments). These produce LSN comparison failures when
// mixed with standard MongoDB's operationTime-based checkpoints. A test flag that
// partially simulates DocumentDB creates more problems than it solves — see the
// commit history on the cosmos branch for the full investigation.
describe.skipIf(DATABASE_TYPE != DatabaseType.DOCUMENTDB)('documentDbMode', () => {
  test('prints hello response and detects DocumentDB', async () => {
    const { client, db } = await connectMongoData();
    try {
      const hello = await db.command({ hello: 1 });
      console.dir({ hello }, { depth: null });
      expect(hello.internal?.cosmos_versions != null || hello.internal?.documentdb_versions != null).toBe(true);
    } finally {
      await client.close();
    }
  });

  // Verifies a core assumption of the DocumentDB sentinel LSN design: that
  // `fullDocument` on update events is the write-time post-image, not a
  // read-time lookup of the document's current state.
  //
  // SentinelCheckpointImplementation tracks its position from `fullDocument.i` on
  // updates to the standalone checkpoint document, and commits LSNs based on
  // that value. With read-time (updateLookup) semantics, a backlogged stream
  // reading an old event would see a *future* counter value, letting the
  // committed LSN run ahead of unprocessed data events — which would allow
  // write checkpoints to resolve before the caller's write has replicated.
  //
  // The test bursts increments while no cursor is reading, then replays the
  // events. Write-time semantics yield one event per increment, each carrying
  // its own value. Read-time semantics yield events all reporting the final
  // value. Collapsed/missing events would indicate DocumentDB coalesces rapid
  // same-document updates, which would be its own significant finding.
  test('fullDocument on update events is the write-time post-image', { timeout: 120_000 }, async () => {
    const { client, db } = await connectMongoData();
    try {
      const collection = db.collection<{ _id: string; i: number }>(CHECKPOINTS_COLLECTION);
      const docId = '_test_fulldoc_semantics';

      const pipeline = [
        {
          $match: {
            operationType: 'update',
            'ns.db': db.databaseName,
            'ns.coll': CHECKPOINTS_COLLECTION,
            'documentKey._id': docId
          }
        }
      ];

      // Establish a resume position past the priming writes, so the replay
      // below starts cleanly before the burst.
      //
      // The stream only delivers events written after the cursor is
      // established (DocumentDB has no startAtOperationTime), and the driver opens
      // the cursor lazily on the first read. So keep writing priming
      // increments until the first event comes through — the same approach the
      // production retry loop uses during initial LSN acquisition.
      let resumeToken: unknown;
      const before = client.watch(pipeline, { fullDocument: 'updateLookup', maxAwaitTimeMS: 500 });
      try {
        const deadline = Date.now() + 60_000;
        let lastPrimedI = 0;
        let drainedTo = -1;
        while (drainedTo < 0 && Date.now() < deadline) {
          const result = await collection.findOneAndUpdate(
            { _id: docId },
            { $inc: { i: 1 } },
            { upsert: true, returnDocument: 'after' }
          );
          lastPrimedI = result!.i;
          const event = await before.tryNext();
          if (event != null && 'fullDocument' in event) {
            drainedTo = event.fullDocument?.i ?? -1;
          }
        }
        expect(drainedTo, 'change stream never delivered the priming events').toBeGreaterThan(0);

        // Drain the remaining priming events so they don't leak into the
        // replay below.
        while (drainedTo < lastPrimedI && Date.now() < deadline) {
          const event = await before.tryNext();
          if (event != null && 'fullDocument' in event) {
            drainedTo = event.fullDocument?.i ?? drainedTo;
          }
        }
        expect(drainedTo, 'timed out draining priming events').toEqual(lastPrimedI);

        resumeToken = before.resumeToken;
        expect(resumeToken).toBeTruthy();
      } finally {
        await before.close();
      }

      // Burst increments with no cursor reading. All writes complete before
      // any event is fetched, so a read-time lookup cannot accidentally
      // return event-time values.
      const expected: number[] = [];
      for (let k = 0; k < 5; k++) {
        const result = await collection.findOneAndUpdate(
          { _id: docId },
          { $inc: { i: 1 } },
          { upsert: true, returnDocument: 'after' }
        );
        expected.push(result!.i);
      }

      // Replay the burst and record the value each event carries.
      const seen: number[] = [];
      const after = client.watch(pipeline, {
        fullDocument: 'updateLookup',
        maxAwaitTimeMS: 500,
        resumeAfter: resumeToken
      });
      try {
        const start = Date.now();
        const deadline = start + 60_000;
        while (seen.length < expected.length && Date.now() < deadline) {
          const event = await after.tryNext();
          if (event != null && 'fullDocument' in event) {
            seen.push(event.fullDocument?.i);
          }
        }
      } finally {
        await after.close();
      }

      expect(seen).toEqual(expected);
    } finally {
      await client.close();
    }
  });

  // Empirically probe whether DocumentDB delivers change events for *different*
  // documents in the order they were written.
  //
  // DocumentDB only guarantees change order per shard key (RU docs) / nothing
  // documented (vCore) — not globally. The sentinel write-checkpoint design
  // assumes no cross-document order: a write checkpoint head is the
  // `_standalone_checkpoint` sentinel, and the caller's data write lives in a
  // different document. If the sentinel event can be delivered *before* the
  // data write that preceded it, a write checkpoint can resolve before its data
  // has replicated (see docs/documentdb/documentdb-outstanding-items.md).
  //
  // This reproduces that exact scenario using the real checkpointing write path
  // (createDocumentDbCheckpointLsn): each round writes a data document, then bumps
  // the standalone sentinel, and we record the delivery order off a single
  // cluster-level change stream.
  //
  // We do NOT assert on cross-document order (delivery order is not contractual
  // — a single-shard cluster may happen to preserve it); the finding is logged
  // so we can document the cluster's observed behaviour. We DO assert that
  // same-document order is preserved, since sentinel `i` matching relies on it.
  test('change stream delivery order across documents', { timeout: 180_000 }, async () => {
    const { client, db } = await connectMongoData();
    const ROUNDS = 20;
    const dataColl = 'order_probe_data';
    const dataId = '_order_probe_doc';
    try {
      const data = db.collection<{ _id: string; seq: number }>(dataColl);
      await data.deleteMany({});

      // Watch the whole database; classify events by documentKey in code.
      const pipeline = [
        {
          $match: {
            operationType: { $in: ['insert', 'update', 'replace'] },
            'ns.db': db.databaseName
          }
        }
      ];

      const cursor = client.watch(pipeline, { fullDocument: 'updateLookup', maxAwaitTimeMS: 500 });
      try {
        // Prime the cursor (DocumentDB has no startAtOperationTime, and the driver
        // opens the cursor lazily) by writing the data doc with descending
        // negative seqs until an event comes through, then drain the backlog.
        const primeDeadline = Date.now() + 60_000;
        let primed = false;
        let primeSeq = 0;
        while (!primed && Date.now() < primeDeadline) {
          primeSeq -= 1;
          await data.updateOne({ _id: dataId }, { $set: { seq: primeSeq } }, { upsert: true });
          if ((await cursor.tryNext()) != null) {
            primed = true;
          }
        }
        expect(primed, 'change stream cursor never went live').toBe(true);
        while ((await cursor.tryNext()) != null) {
          // drain remaining priming events
        }

        // Issue interleaved writes: data(round) then sentinel bump, per round.
        type Issued = { label: string; kind: 'data' | 'sentinel'; round: number; sentinel?: bigint };
        const issued: Issued[] = [];
        const sentinelValueToRound = new Map<string, number>();
        for (let round = 1; round <= ROUNDS; round++) {
          await data.updateOne({ _id: dataId }, { $set: { seq: round } });
          issued.push({ label: `D${round}`, kind: 'data', round });

          const sentinel = SentinelLSN.fromSerialized(await createDocumentDbCheckpointLsn(client, db)).sentinel;
          issued.push({ label: `S${round}`, kind: 'sentinel', round, sentinel });
          sentinelValueToRound.set(sentinel.toString(), round);
        }

        // Collect the delivered events in arrival order.
        const arrivals: { label: string; kind: 'data' | 'sentinel'; value: number | bigint }[] = [];
        const collectDeadline = Date.now() + 90_000;
        while (arrivals.length < issued.length && Date.now() < collectDeadline) {
          const ev = await cursor.tryNext();
          if (ev == null || !('documentKey' in ev) || !('fullDocument' in ev) || ev.fullDocument == null) {
            continue;
          }
          const id = String(ev.documentKey._id);
          if (id === dataId) {
            const seq = (ev.fullDocument as any).seq as number;
            if (seq >= 1) {
              arrivals.push({ label: `D${seq}`, kind: 'data', value: seq });
            }
          } else if (id === STANDALONE_CHECKPOINT_ID) {
            const value = BigInt((ev.fullDocument as any).i);
            const round = sentinelValueToRound.get(value.toString());
            if (round != null) {
              arrivals.push({ label: `S${round}`, kind: 'sentinel', value });
            }
          }
        }

        // --- Analysis ---
        const dataArrivals = arrivals.filter((a) => a.kind === 'data').map((a) => a.value as number);
        const sentinelArrivals = arrivals.filter((a) => a.kind === 'sentinel').map((a) => a.value as bigint);
        const dataOrderPreserved = dataArrivals.every((v, i) => i === 0 || v > dataArrivals[i - 1]);
        const sentinelOrderPreserved = sentinelArrivals.every((v, i) => i === 0 || v > sentinelArrivals[i - 1]);

        // General cross-document inversions: any event delivered out of global issue order.
        const issueIndex = new Map(issued.map((it, i) => [it.label, i] as const));
        const crossDocumentInversions: { afterDelivering: string; received: string }[] = [];
        for (let i = 1; i < arrivals.length; i++) {
          if (issueIndex.get(arrivals[i].label)! < issueIndex.get(arrivals[i - 1].label)!) {
            crossDocumentInversions.push({ afterDelivering: arrivals[i - 1].label, received: arrivals[i].label });
          }
        }

        // Bug-specific signal: a sentinel bump overtaking the data write that
        // preceded it in the same round (write checkpoint resolves too early).
        const sentinelOvertakingItsDataWrite: number[] = [];
        for (let round = 1; round <= ROUNDS; round++) {
          const dIdx = arrivals.findIndex((a) => a.label === `D${round}`);
          const sIdx = arrivals.findIndex((a) => a.label === `S${round}`);
          if (dIdx >= 0 && sIdx >= 0 && sIdx < dIdx) {
            sentinelOvertakingItsDataWrite.push(round);
          }
        }

        console.dir(
          {
            crossDocumentOrderingProbe: {
              rounds: ROUNDS,
              issued: issued.length,
              arrived: arrivals.length,
              deliveryOrder: arrivals.map((a) => a.label),
              dataOrderPreserved,
              sentinelOrderPreserved,
              crossDocumentInversions,
              sentinelOvertakingItsDataWrite
            }
          },
          { depth: null }
        );

        // We expect to receive every issued event within the deadline.
        expect(arrivals.length, 'did not receive all issued events before the deadline').toBe(issued.length);
        // Per-document ordering is the one guarantee DocumentDB documents — assert it.
        expect(dataOrderPreserved, 'data-document events arrived out of order').toBe(true);
        expect(sentinelOrderPreserved, 'sentinel-document events arrived out of order').toBe(true);
        // Cross-document order is intentionally NOT asserted; inversions (if any)
        // are logged above as the finding.
      } finally {
        await cursor.close();
      }
      await data.deleteMany({});
    } finally {
      await client.close();
    }
  });

  // Follow-up to the probe above. The sequential probe preserves order on a
  // single-shard cluster, where there is effectively one ordered feed. This
  // variant adds a concurrent write storm across many other documents and a much
  // higher round count, to test whether a single-shard cluster has *internal*
  // sub-partitions whose feeds can interleave out of order under load.
  //
  // The measured D→S pairs are still issued strictly sequentially (so they keep a
  // well-defined happens-before order to violate); the background writers only
  // generate feed pressure. Same reporting; cross-document order is logged, not
  // asserted.
  test('change stream delivery order across documents — under concurrent load', { timeout: 240_000 }, async () => {
    const { client, db } = await connectMongoData();
    const ROUNDS = 100;
    const BACKGROUND_WRITERS = 6;
    const dataColl = 'order_probe_data';
    const dataId = '_order_probe_doc';
    const backgroundCollections = Array.from({ length: BACKGROUND_WRITERS }, (_, w) => `order_probe_bg_${w}`);
    try {
      const data = db.collection<{ _id: string; seq: number }>(dataColl);
      await data.deleteMany({});

      // Only deliver the two measured namespaces, so background churn does not
      // flood the cursor we are measuring.
      const pipeline = [
        {
          $match: {
            operationType: { $in: ['insert', 'update', 'replace'] },
            'ns.db': db.databaseName,
            'ns.coll': { $in: [dataColl, CHECKPOINTS_COLLECTION] }
          }
        }
      ];
      const cursor = client.watch(pipeline, { fullDocument: 'updateLookup', maxAwaitTimeMS: 500 });

      // Start the background write storm: 6 loops, each with one write in flight,
      // churning 100 rotating documents per collection.
      let stopBackground = false;
      const background = backgroundCollections.map((name) =>
        (async () => {
          const coll = db.collection<{ _id: string; n: number }>(name);
          let n = 0;
          while (!stopBackground) {
            n += 1;
            await coll.updateOne({ _id: `bg_${n % 100}` }, { $set: { n } }, { upsert: true }).catch(() => {});
          }
        })()
      );

      try {
        // Prime the cursor.
        const primeDeadline = Date.now() + 60_000;
        let primed = false;
        let primeSeq = 0;
        while (!primed && Date.now() < primeDeadline) {
          primeSeq -= 1;
          await data.updateOne({ _id: dataId }, { $set: { seq: primeSeq } }, { upsert: true });
          if ((await cursor.tryNext()) != null) {
            primed = true;
          }
        }
        expect(primed, 'change stream cursor never went live').toBe(true);
        while ((await cursor.tryNext()) != null) {
          // drain priming events
        }

        // Measured sequential D→S pairs.
        const issued: { label: string; round: number }[] = [];
        const sentinelValueToRound = new Map<string, number>();
        for (let round = 1; round <= ROUNDS; round++) {
          await data.updateOne({ _id: dataId }, { $set: { seq: round } });
          issued.push({ label: `D${round}`, round });
          const sentinel = SentinelLSN.fromSerialized(await createDocumentDbCheckpointLsn(client, db)).sentinel;
          issued.push({ label: `S${round}`, round });
          sentinelValueToRound.set(sentinel.toString(), round);
        }

        // Collect.
        const arrivals: string[] = [];
        const expectedLabels = new Set(issued.map((i) => i.label));
        const collectDeadline = Date.now() + 120_000;
        while (arrivals.length < issued.length && Date.now() < collectDeadline) {
          const ev = await cursor.tryNext();
          if (ev == null || !('documentKey' in ev) || !('fullDocument' in ev) || ev.fullDocument == null) {
            continue;
          }
          const id = String(ev.documentKey._id);
          let label: string | null = null;
          if (id === dataId) {
            const seq = (ev.fullDocument as any).seq as number;
            if (seq >= 1) {
              label = `D${seq}`;
            }
          } else if (id === STANDALONE_CHECKPOINT_ID) {
            const round = sentinelValueToRound.get(BigInt((ev.fullDocument as any).i).toString());
            if (round != null) {
              label = `S${round}`;
            }
          }
          if (label != null && expectedLabels.has(label)) {
            arrivals.push(label);
          }
        }

        // Analysis (same as the sequential probe).
        const issueIndex = new Map(issued.map((it, i) => [it.label, i] as const));
        const crossDocumentInversions: { afterDelivering: string; received: string }[] = [];
        for (let i = 1; i < arrivals.length; i++) {
          if (issueIndex.get(arrivals[i])! < issueIndex.get(arrivals[i - 1])!) {
            crossDocumentInversions.push({ afterDelivering: arrivals[i - 1], received: arrivals[i] });
          }
        }
        const sentinelOvertakingItsDataWrite: number[] = [];
        for (let round = 1; round <= ROUNDS; round++) {
          const dIdx = arrivals.indexOf(`D${round}`);
          const sIdx = arrivals.indexOf(`S${round}`);
          if (dIdx >= 0 && sIdx >= 0 && sIdx < dIdx) {
            sentinelOvertakingItsDataWrite.push(round);
          }
        }

        console.dir(
          {
            crossDocumentOrderingUnderLoad: {
              rounds: ROUNDS,
              backgroundWriters: BACKGROUND_WRITERS,
              issued: issued.length,
              arrived: arrivals.length,
              crossDocumentInversions,
              sentinelOvertakingItsDataWrite
            }
          },
          { depth: null }
        );

        expect(arrivals.length, 'did not receive all issued events before the deadline').toBe(issued.length);
      } finally {
        stopBackground = true;
        await Promise.allSettled(background);
        await cursor.close();
      }

      await data.deleteMany({});
      for (const name of backgroundCollections) {
        await db
          .collection(name)
          .drop()
          .catch(() => {});
      }
    } finally {
      await client.close();
    }
  });

  // 120s timeout — remote DocumentDB clusters can have 10-30s latency spikes
  // for change stream delivery. Tests that poll for data need headroom.
  describeWithStorage({ timeout: 120_000 }, defineDocumentDBDbModeTests);
});

function defineDocumentDBDbModeTests({ factory, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof ChangeStreamTestContext.open>[1]) => {
    return ChangeStreamTestContext.open(factory, {
      ...options,
      storageVersion,
      streamOptions: {
        ...options?.streamOptions
      }
    });
  };

  test('basic replication in documentDbMode', async () => {
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
    // format on DocumentDB. The streaming loop must resolve it by matching the sentinel event.
    const checkpoint = await context.getCheckpoint();
    expect(checkpoint).toBeTruthy();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([test_utils.putOp('test_data', { id: insertedId, description: 'sentinel_test' })]);
  });

  // DocumentDB does not support $changeStreamSplitLargeEvent, so large change
  // events cannot be split into fragments the way the standard MongoDB path
  // does. This verifies that a large document still replicates end-to-end —
  // both on insert (the event carries the full document) and on update
  // (updateLookup fetches the full document into the event) — and that the
  // large value is persisted through to bucket storage, not just delivered on
  // the change stream.
  //
  // The payload is sized just under PowerSync's MAX_ROW_SIZE (15 MiB). Rows at
  // or above that limit are dropped by bucket storage regardless of source DB
  // ("Row too large ... Removing"), so the persisted value cannot approach the
  // 16 MiB BSON document limit. 14 MiB still forces a large change event
  // through the DocumentDB stream while keeping the projected row under the limit.
  // Assertions check the payload length rather than inlining a 14MB string.
  // Note: DocumentDB delivers large change events very slowly (observed ~18s to fetch a
  // single ~14 MiB event), so this test allows generous checkpoint timeouts. See
  // docs/documentdb/documentdb-limitations.md.
  test(
    'replicates a large document near the row size limit',
    // Fetching the large row takes very long in DocumentDBDB cloud
    { timeout: 120_000 },
    async () => {
      await using context = await openContext();
      const { db } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, marker, payload FROM "test_data"`);

      await db.createCollection('test_data');
      const collection = db.collection('test_data');

      await context.replicateSnapshot();
      context.startStreaming();

      // 14 MiB payload — large enough to exercise the large-event path, but the
      // projected row (payload + small envelope) stays under MAX_ROW_SIZE so it
      // is not dropped by bucket storage.
      const largePayload = 'x'.repeat(14 * 1024 * 1024);

      const insertResult = await collection.insertOne({ marker: 'big_insert', payload: largePayload });
      const id = insertResult.insertedId.toHexString();

      // Large events are slow to fetch on DocumentDB, so allow well beyond the 15s default.
      const afterInsert = await context.getBucketData('global[]', undefined, { timeout: 50_000 });
      expect(afterInsert.length).toEqual(1);
      const insertData = JSON.parse(afterInsert[0].data as string);
      expect(insertData).toMatchObject({ id, marker: 'big_insert' });
      expect(insertData.payload.length).toEqual(largePayload.length);

      // Update a small field; the payload stays large, so the updateLookup
      // fullDocument on the change event is still ~15MB.
      await collection.updateOne({ _id: insertResult.insertedId }, { $set: { marker: 'big_update' } });

      // Fetching the large row takes very long in DocumentDBDB cloud
      const afterUpdate = await context.getBucketData('global[]', undefined, { timeout: 50_000 });
      expect(afterUpdate.length).toEqual(2);
      const updateData = JSON.parse(afterUpdate[1].data as string);
      expect(updateData).toMatchObject({ id, marker: 'big_update' });
      expect(updateData.payload.length).toEqual(largePayload.length);
    }
  );

  test('keepalive in documentDbMode', async () => {
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
    await context.getCheckpoint({ timeout: 50_000 });

    // Wait past the keepalive interval so the idle keepalive path fires.
    // On DocumentDB, this must NOT crash from parseResumeTokenTimestamp
    // (DocumentDB resume tokens are base64, not hex).
    await setTimeout(3000);

    // Insert data after the keepalive interval to verify the stream is still alive
    const collection = db.collection('test_data');
    await collection.insertOne({ description: 'after_keepalive' });

    const data = await context.getBucketData('global[]');
    expect(data.length).toBeGreaterThanOrEqual(1);
    const lastOp = data[data.length - 1];
    expect(JSON.parse(lastOp.data as string)).toMatchObject({ description: 'after_keepalive' });
  });

  test('respects maxAwaitTimeMS for idle getMore calls in documentDbMode', async () => {
    const maxAwaitTimeMS = 2_000;

    await using context = await openContext({
      streamOptions: {
        maxAwaitTimeMS
      }
    });

    // DocumentDB uses a cluster-level change stream through client.db('admin'), so
    // spying on only context.db.command would miss the getMore calls. Spying on
    // the Db prototype captures command calls from both the test DB and admin DB
    // while still delegating to the real MongoDB driver implementation.
    const dbPrototype = Object.getPrototypeOf(context.db);
    const originalCommand = dbPrototype.command;
    const getMoreTimings: {
      collection: unknown;
      maxTimeMS: unknown;
      durationMS: number;
      nextBatchLength: number | undefined;
    }[] = [];
    const commandSpy = vi.spyOn(dbPrototype, 'command').mockImplementation(async function (
      this: unknown,
      command: any,
      options?: any
    ) {
      if (command?.getMore == null) {
        return originalCommand.call(this, command, options);
      }

      // Measure the actual round-trip duration of the driver's getMore command.
      // This verifies the server waits for maxTimeMS when the change stream is
      // idle, rather than returning empty batches immediately.
      const start = performance.now();
      let result: any;
      try {
        result = await originalCommand.call(this, command, options);
        return result;
      } finally {
        const cursor = Buffer.isBuffer(result?.cursor)
          ? mongo.BSON.deserialize(result.cursor, {
              useBigInt64: true,
              fieldsAsRaw: { nextBatch: true },
              validation: { utf8: false }
            })
          : result?.cursor;

        getMoreTimings.push({
          collection: command.collection,
          maxTimeMS: command.maxTimeMS,
          durationMS: Math.round(performance.now() - start),
          nextBatchLength: cursor?.nextBatch?.length
        });
      }
    });

    try {
      const { db } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await db.createCollection('test_data');
      const collection = db.collection('test_data');

      await context.replicateSnapshot();
      context.startStreaming();

      const result = await collection.insertOne({ description: 'maxAwaitTimeMS_test' });
      await context.getBucketData('global[]');

      // Once the stream has caught up, the latest getMore call should eventually
      // be an idle poll. That idle poll should wait for maxTimeMS instead of
      // returning immediately, and the response should contain an empty batch.
      await vi.waitFor(
        () => {
          const lastGetMore = getMoreTimings.at(-1);
          expect(lastGetMore?.durationMS).toBeGreaterThanOrEqual(maxAwaitTimeMS);
          expect(lastGetMore?.nextBatchLength).toEqual(0);
        },
        {
          timeout: maxAwaitTimeMS + 2_000,
          interval: 100
        }
      );

      console.dir({ getMoreMaxAwaitTimeMSTimings: getMoreTimings }, { depth: null });

      expect(result.insertedId).toBeTruthy();
      expect(getMoreTimings.length).toBeGreaterThan(0);
    } finally {
      commandSpy.mockRestore();
    }
  });

  test('write checkpoint flow in documentDbMode', async () => {
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

    // Exercise the write checkpoint flow. On DocumentDB, createReplicationHead
    // advances the standalone checkpoint sentinel and uses the resulting
    // sentinel-based LSN as the source-side head. The sentinel write also
    // nudges replication forward on an idle stream.
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
    // drop data events on DocumentDB after restart. On DocumentDB, wallTime has
    // second precision (increment 0). Without the isDocumentDb guard, events
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

    await context2.loadActiveSyncRules();

    context2.startStreaming();

    // Wait for the stream to be fully initialized — the streaming loop must
    // process its initial checkpoint before we insert test data. Without this,
    // the insert can happen before the ChangeStream's lazy aggregate command
    // is sent, causing the event to be missed entirely (not a .lte() issue).
    await context2.getCheckpoint({ timeout: 50_000 });

    // Insert — if .lte() drops same-second events, this data will never appear.
    const collection2 = db2.collection('test_data');
    const result2 = await collection2.insertOne({ description: 'post_restart_data' });
    const id2 = result2.insertedId;

    // Poll for the data by repeatedly calling getBucketData with a longer timeout.
    // We bypass the flaky getClientCheckpoint timing by polling until the data appears
    // or the timeout expires. If the .lte() guard drops same-second events, the data
    // will never appear — deterministic failure.
    // 50s timeout — remote DocumentDB clusters can have 10-30s latency spikes.
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

  test('resume after restart in documentDbMode', async () => {
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

    const dataBefore = await context.getBucketDataAtLatestCheckpoint('global[]');
    expect(dataBefore).toMatchObject([
      test_utils.putOp('test_data', { id: id1.toHexString(), description: 'before_restart' })
    ]);

    // Stop streaming (simulates a restart)
    context.abort();
    await context.dispose();

    // Phase 2: reopen without clearing and resume
    await using context2 = await openContext({ doNotClear: true });
    const db2 = context2.db;

    await context2.loadActiveSyncRules();

    context2.startStreaming();

    // Wait for the stream to fully initialize and process the initial checkpoint
    // before inserting new data.
    await context2.getCheckpoint({ timeout: 10_000 });

    const collection2 = db2.collection('test_data');
    const result2 = await collection2.insertOne({ description: 'after_restart' });
    const id2 = result2.insertedId;

    // On DocumentDB, wall-clock LSNs have second precision and stored LSNs may
    // include resume-token suffixes. Avoid creating a fresh sentinel checkpoint
    // on every poll; read at the latest persisted checkpoint instead.
    // 50s timeout — remote DocumentDB clusters can have 10-30s latency spikes.
    const deadline = Date.now() + 50_000;
    let found = false;
    while (Date.now() < deadline) {
      const dataAfter = await context2.getBucketDataAtLatestCheckpoint('global[]');
      const afterRestartOps = dataAfter.filter((op) => op.object_id === id2.toHexString() && op.op === 'PUT');
      if (afterRestartOps.length >= 1) {
        expect(JSON.parse(afterRestartOps[0].data as string)).toMatchObject({ description: 'after_restart' });
        found = true;
        break;
      }
      await setTimeout(200);
    }
    expect(found, 'Data event after restart was not replicated within timeout').toBe(true);
  });
}
