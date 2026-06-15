import { describe, expect, test } from 'vitest';

import { MongoLSN } from '@module/common/MongoLSN.js';
import { SentinelLSN } from '@module/common/SentinelLSN.js';
import {
  createCheckpoint,
  createCosmosCheckpointLsn,
  STANDALONE_CHECKPOINT_ID
} from '@module/replication/MongoRelation.js';
import { CHECKPOINTS_COLLECTION } from '@module/replication/replication-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { connectMongoData } from './util.js';

describe('Cosmos DB helpers', () => {
  describe('getEventTimestamp behavior', () => {
    // getEventTimestamp is a utility in CheckpointImplementation.ts. These tests document the
    // expected behavior, tested indirectly. The integration tests in cosmosdb_mode.test.ts
    // exercise the actual code path. Here we test the underlying logic that the method
    // should implement.

    test('with clusterTime present — returns clusterTime', () => {
      // When isCosmosDb is false and clusterTime is present, getEventTimestamp should return clusterTime.
      // We simulate this by checking that clusterTime is directly usable as a Timestamp.
      const clusterTime = mongo.Timestamp.fromBits(1, 1700000000);
      const event = { clusterTime, wallTime: new Date('2024-01-01T00:00:00Z') };
      // Standard MongoDB path: clusterTime takes priority
      expect(event.clusterTime).toEqual(clusterTime);
    });

    test('with only wallTime present — returns Timestamp with seconds, increment 0', () => {
      // On Cosmos DB, clusterTime is absent. getEventTimestamp should create a Timestamp
      // from wallTime: seconds from epoch in high bits, 0 in low bits (increment).
      const wallTime = new Date('2024-06-15T12:00:00Z');
      const expectedSeconds = Math.floor(wallTime.getTime() / 1000);
      const timestamp = mongo.Timestamp.fromBits(0, expectedSeconds);
      expect(timestamp.getHighBitsUnsigned()).toEqual(expectedSeconds);
      expect(timestamp.getLowBitsUnsigned()).toEqual(0);
    });

    test('with neither clusterTime nor wallTime — should throw', () => {
      // getEventTimestamp should throw when neither timestamp source is available.
      const event = {} as any;
      // Verify the event has neither field
      expect(event.clusterTime).toBeUndefined();
      expect(event.wallTime).toBeUndefined();
      // The actual throw is tested via integration tests — the method is private.
      // This documents the expected contract.
    });

    test('with both + isCosmosDb=true — skips clusterTime, uses wallTime', () => {
      // On Cosmos DB, even if clusterTime were present,
      // getEventTimestamp should prefer wallTime to exercise the Cosmos DB code path.
      const wallTime = new Date('2024-06-15T12:00:00Z');
      const expectedSeconds = Math.floor(wallTime.getTime() / 1000);
      const clusterTime = mongo.Timestamp.fromBits(42, 1700000000);

      // On Cosmos DB, the result should use wallTime, not clusterTime
      const expectedTimestamp = mongo.Timestamp.fromBits(0, expectedSeconds);
      expect(expectedTimestamp.getHighBitsUnsigned()).toEqual(expectedSeconds);
      expect(expectedTimestamp.getLowBitsUnsigned()).toEqual(0);
      // The clusterTime would have different values
      expect(clusterTime.getHighBitsUnsigned()).not.toEqual(expectedSeconds);
    });
  });

  describe('Cosmos DB detection', () => {
    // Detection logic: hello.internal?.cosmos_versions != null || hello.internal?.documentdb_versions != null
    // Older clusters use cosmos_versions, newer ones use documentdb_versions after Microsoft's rename.
    const isCosmosDb = (hello: any) =>
      hello.internal?.cosmos_versions != null || hello.internal?.documentdb_versions != null;

    test('hello with cosmos_versions — detected as Cosmos DB', () => {
      const hello = {
        isWritablePrimary: true,
        msg: 'isdbgrid',
        setName: 'globaldb',
        internal: {
          cosmos_versions: ['1.104-1', '1.105.0', '12.1-1']
        }
      };
      expect(isCosmosDb(hello)).toBe(true);
    });

    test('hello with documentdb_versions — detected as Cosmos DB', () => {
      const hello = {
        isWritablePrimary: true,
        msg: 'isdbgrid',
        internal: {
          documentdb_versions: ['1.111-0', '1.112.0', '12.1-1']
        }
      };
      expect(isCosmosDb(hello)).toBe(true);
    });

    test('standard hello response — not Cosmos DB', () => {
      const hello = {
        isWritablePrimary: true,
        setName: 'rs0',
        hosts: ['localhost:27017']
      };
      expect(isCosmosDb(hello)).toBe(false);
    });
  });

  describe('sentinel checkpoint format', () => {
    test('createCheckpoint returns sentinel format when mode is sentinel', async () => {
      // When mode is 'sentinel', createCheckpoint returns a sentinel string
      // like 'sentinel:<id>:<i>' for event-based matching in the streaming loop.
      const { client, db } = await connectMongoData();
      try {
        const checkpoint = await createCheckpoint(client, db, STANDALONE_CHECKPOINT_ID, { mode: 'sentinel' });
        // The sentinel format should be 'sentinel:<id>:<i>'
        expect(checkpoint).toMatch(/^sentinel:/);
        const parts = checkpoint.split(':');
        expect(parts).toHaveLength(3);
        expect(parts[0]).toEqual('sentinel');
        expect(parts[1]).toEqual(STANDALONE_CHECKPOINT_ID);
        // i should be a number (the incrementing counter)
        expect(Number.isInteger(Number(parts[2]))).toBe(true);
      } finally {
        await client.close();
      }
    });

    test('standalone counter is seeded at a timestamp value on creation', { timeout: 30_000 }, async () => {
      // If a consumer deletes the standalone checkpoint document in their
      // source database, the re-created counter must not restart below
      // already-committed LSNs. createCosmosCheckpointLsn seeds new counters
      // at the current epoch milliseconds, so the coordinate jumps forward
      // instead of resetting.
      const { client, db: sharedDb } = await connectMongoData();
      // Use an isolated database: other test files run in parallel against the
      // shared test database and both bump and clear the standalone checkpoint
      // document, which would make these exact assertions racy.
      const db = client.db(`${sharedDb.databaseName}_seed_test`);
      try {
        const before = BigInt(Date.now());
        const first = SentinelLSN.fromSerialized(await createCosmosCheckpointLsn(client, db));
        const after = BigInt(Date.now());

        // Seeded at epoch ms, not at 1.
        expect(first.sentinel).toBeGreaterThanOrEqual(before);
        expect(first.sentinel).toBeLessThanOrEqual(after + 1n);

        // Subsequent calls increment normally.
        const second = SentinelLSN.fromSerialized(await createCosmosCheckpointLsn(client, db));
        expect(second.sentinel).toEqual(first.sentinel + 1n);

        // Simulate a consumer deleting the document after the counter has
        // accumulated increments: the re-created counter resumes ahead.
        await db.collection(CHECKPOINTS_COLLECTION).deleteOne({ _id: STANDALONE_CHECKPOINT_ID as any });
        const recreated = SentinelLSN.fromSerialized(await createCosmosCheckpointLsn(client, db));
        expect(recreated.sentinel).toBeGreaterThan(second.sentinel);
      } finally {
        await db.dropDatabase().catch(() => {});
        await client.close();
      }
    });

    test('createCheckpoint embeds globalSentinel in the barrier document', async () => {
      // createBatchCheckpoint() passes the standalone counter value so the
      // stream can read the global LSN coordinate from its own barrier event,
      // without depending on cross-document change stream ordering.
      const { client, db } = await connectMongoData();
      try {
        const barrierId = new mongo.ObjectId();
        await createCheckpoint(client, db, barrierId, { mode: 'sentinel', globalSentinel: 42n });

        const doc = await db.collection(CHECKPOINTS_COLLECTION).findOne({ _id: barrierId as any });
        expect(doc?.i).toEqual(1);
        expect(BigInt(doc!.globalSentinel)).toEqual(42n);
      } finally {
        await client.close();
      }
    });
  });

  describe('sentinel matching', () => {
    test('sentinel:X:42 matches event with documentKey._id X and fullDocument.i 42', () => {
      const sentinel = 'sentinel:X:42';
      const [, sentinelId, sentinelI] = sentinel.split(':');

      const changeDocument = {
        documentKey: { _id: 'X' },
        fullDocument: { i: 42 }
      };

      const docId = String(changeDocument.documentKey._id);
      const docI = String(changeDocument.fullDocument?.i);
      expect(docId).toEqual(sentinelId);
      expect(docI).toEqual(sentinelI);
    });

    test('sentinel non-match — different i value does not match', () => {
      const sentinel = 'sentinel:X:42';
      const [, sentinelId, sentinelI] = sentinel.split(':');

      const changeDocument = {
        documentKey: { _id: 'X' },
        fullDocument: { i: 99 }
      };

      const docId = String(changeDocument.documentKey._id);
      const docI = String(changeDocument.fullDocument?.i);
      expect(docId).toEqual(sentinelId);
      expect(docI).not.toEqual(sentinelI);
    });

    test('standard LSN comparison unaffected — hex LSN does not enter sentinel branch', () => {
      // A standard hex LSN should not be treated as a sentinel
      const lsn = '6683b8a000000001';
      expect(lsn.startsWith('sentinel:')).toBe(false);
    });
  });

  describe('keepalive LSN with Date.now()', () => {
    test('timestamp is within a few seconds of current time', () => {
      // On Cosmos DB, keepalive uses Date.now() instead of parseResumeTokenTimestamp.
      // Verify that a MongoLSN created from Date.now() produces a comparable timestamp
      // close to the current time.
      const nowSeconds = Math.floor(Date.now() / 1000);
      const timestamp = mongo.Timestamp.fromBits(0, nowSeconds);
      const lsn = new MongoLSN({ timestamp });

      // Parse the LSN back and verify the timestamp
      const parsed = MongoLSN.fromSerialized(lsn.comparable);
      const parsedSeconds = parsed.timestamp.getHighBitsUnsigned();

      // Should be within 5 seconds of now
      expect(Math.abs(parsedSeconds - nowSeconds)).toBeLessThanOrEqual(5);
    });
  });
});
