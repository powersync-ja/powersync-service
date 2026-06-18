import { describe, expect, test } from 'vitest';

import { SentinelLSN } from '@module/common/SentinelLSN.js';
import { getEventTimestamp } from '@module/replication/checkpoints/CheckpointImplementation.js';
import {
  createCheckpoint,
  createDocumentDbCheckpointLsn,
  STANDALONE_CHECKPOINT_ID
} from '@module/replication/MongoRelation.js';
import { CHECKPOINTS_COLLECTION } from '@module/replication/replication-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { connectMongoData } from './util.js';

describe('DocumentDB helpers', () => {
  describe('getEventTimestamp', () => {
    // getEventTimestamp is used by the timestamp checkpoint implementation (standard
    // MongoDB): it prefers clusterTime and falls back to second-precision wallTime.
    // The DocumentDB sentinel implementation does not use it — DocumentDB LSNs are
    // sentinel-counter based, not timestamp based.

    test('returns clusterTime when present', () => {
      const clusterTime = mongo.Timestamp.fromBits(1, 1700000000);
      const event = { clusterTime, wallTime: new Date('2024-01-01T00:00:00Z') } as any;
      expect(getEventTimestamp(event)).toEqual(clusterTime);
    });

    test('falls back to second-precision wallTime when clusterTime is absent', () => {
      const wallTime = new Date('2024-06-15T12:00:00.789Z');
      const result = getEventTimestamp({ wallTime } as any);
      expect(result.getHighBitsUnsigned()).toEqual(Math.floor(wallTime.getTime() / 1000));
      // Increment is always 0 — wallTime only has second resolution in this conversion.
      expect(result.getLowBitsUnsigned()).toEqual(0);
    });

    test('throws when neither clusterTime nor wallTime is present', () => {
      expect(() => getEventTimestamp({} as any)).toThrow();
    });
  });

  describe('DocumentDB detection', () => {
    // Detection logic: hello.internal?.cosmos_versions != null || hello.internal?.documentdb_versions != null
    // Older clusters use cosmos_versions, newer ones use documentdb_versions after Microsoft's rename.
    const isDocumentDb = (hello: any) =>
      hello.internal?.cosmos_versions != null || hello.internal?.documentdb_versions != null;

    test('hello with cosmos_versions — detected as DocumentDB', () => {
      const hello = {
        isWritablePrimary: true,
        msg: 'isdbgrid',
        setName: 'globaldb',
        internal: {
          cosmos_versions: ['1.104-1', '1.105.0', '12.1-1']
        }
      };
      expect(isDocumentDb(hello)).toBe(true);
    });

    test('hello with documentdb_versions — detected as DocumentDB', () => {
      const hello = {
        isWritablePrimary: true,
        msg: 'isdbgrid',
        internal: {
          documentdb_versions: ['1.111-0', '1.112.0', '12.1-1']
        }
      };
      expect(isDocumentDb(hello)).toBe(true);
    });

    test('standard hello response — not DocumentDB', () => {
      const hello = {
        isWritablePrimary: true,
        setName: 'rs0',
        hosts: ['localhost:27017']
      };
      expect(isDocumentDb(hello)).toBe(false);
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
      // already-committed LSNs. createDocumentDbCheckpointLsn seeds new counters at
      // the current epoch seconds shifted into the high 32 bits (resembling a
      // MongoDB timestamp), so the coordinate jumps forward instead of resetting.
      const { client, db: sharedDb } = await connectMongoData();
      // Use an isolated database: other test files run in parallel against the
      // shared test database and both bump and clear the standalone checkpoint
      // document, which would make these exact assertions racy.
      const db = client.db(`${sharedDb.databaseName}_seed_test`);
      const seed = (epochMs: number) => (BigInt(Math.floor(epochMs / 1000)) << 32n) + 1n;
      try {
        const before = Date.now();
        const first = SentinelLSN.fromSerialized(await createDocumentDbCheckpointLsn(client, db));
        const after = Date.now();

        // Seeded at (epoch_seconds << 32) + 1, not at 1.
        expect(first.sentinel).toBeGreaterThanOrEqual(seed(before));
        expect(first.sentinel).toBeLessThanOrEqual(seed(after));

        // Subsequent calls increment normally.
        const second = SentinelLSN.fromSerialized(await createDocumentDbCheckpointLsn(client, db));
        expect(second.sentinel).toEqual(first.sentinel + 1n);

        // Simulate a consumer deleting the document after the counter has
        // accumulated increments: the re-created counter resumes ahead. The seed
        // has second granularity, and in production the document lives from
        // initial sync onward, so re-creation always lands in a later second;
        // wait for the clock to advance to reproduce that here.
        await new Promise((resolve) => setTimeout(resolve, 1_100));
        await db.collection(CHECKPOINTS_COLLECTION).deleteOne({ _id: STANDALONE_CHECKPOINT_ID as any });
        const recreated = SentinelLSN.fromSerialized(await createDocumentDbCheckpointLsn(client, db));
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

  // Note: the DocumentDB keepalive does not build an LSN from Date.now() — it bumps
  // the shared sentinel via createDocumentDbCheckpointLsn and commits when the bump's
  // own event is observed. The only Date.now()-derived value is the standalone
  // counter seed, which is covered by the 'standalone counter is seeded at a
  // timestamp value on creation' test above.
});
