import { BucketDataDoc } from '@module/storage/implementation/common/BucketDataDoc.js';
import { MongoSyncBucketStorage } from '@module/storage/implementation/createMongoSyncBucketStorage.js';
import { loadBucketDataDocument, serializeBucketData } from '@module/storage/implementation/v3/bucket-format.js';
import { chunkBucketData, DEFAULT_MAX_DOC_SIZE_BYTES } from '@module/storage/implementation/v3/chunking.js';
import { DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS } from '@module/storage/implementation/v3/compaction-constants.js';
import { CompactionLease } from '@module/storage/implementation/v3/CompactionLease.js';
import { BucketDataDocumentV3 } from '@module/storage/implementation/v3/models.js';
import { ObjectStorageError } from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';
import { VersionedPowerSyncMongoV3 } from '@module/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import {
  addChecksums,
  CheckpointChecksumInvalidatedError,
  storage,
  SyncRulesBucketStorage,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
import { bucketRequest, compactActive, register, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe('Mongo Sync Bucket Storage Compact', () => {
  register.registerCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY);

  describe('with blank bucket_state', () => {
    // This can happen when migrating from older service versions, that did not populate bucket_state yet.
    const populate = async (bucketStorage: SyncRulesBucketStorage, sourceTableIndex: number) => {
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

      const sourceTable = await test_utils.resolveTestTable(
        writer,
        'test',
        ['id'],
        INITIALIZED_MONGO_STORAGE_FACTORY,
        sourceTableIndex
      );
      await writer.markAllSnapshotDone('1/1');

      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          owner_id: 'u1'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          owner_id: 'u2'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await writer.commit('1/1');

      return bucketStorage.getCheckpoint();
    };

    const setup = async (storageVersion?: number) => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
bucket_definitions:
  by_user:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules);
      const syncRulesContent = syncRules.syncConfigContent[0];
      const { checkpoint } = await populate(bucketStorage, 1);

      return { bucketStorage, checkpoint, factory, syncRules: syncRulesContent };
    };

    test('V1 full compact with blank bucket_state', async () => {
      const { bucketStorage, checkpoint, factory, syncRules } = await setup();
      const storageDb = bucketStorage.db;

      if (storageDb.storageConfig.incrementalReprocessing) {
        return;
      }
      // Simulate a V1 deployment which pre-dates bucket-state population.
      await factory.db.bucket_state.deleteMany({});

      await compactActive(factory, {
        clearBatchLimit: 200,
        moveBatchLimit: 10,
        moveBatchQueryLimit: 10,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId: checkpoint
      });

      const users = ['u1', 'u2'];
      const userRequests = users.map((user) => bucketRequest(syncRules, `by_user["${user}"]`));
      const [u1Request, u2Request] = userRequests;
      const checksumAfter = await bucketStorage.getChecksums(test_utils.testCheckpoint(checkpoint), userRequests);
      expect(checksumAfter.get(u1Request.bucket)).toEqual({
        bucket: u1Request.bucket,
        checksum: -659469718,
        count: 1
      });
      expect(checksumAfter.get(u2Request.bucket)).toEqual({
        bucket: u2Request.bucket,
        checksum: 430217650,
        count: 1
      });
    });

    test.each(TEST_STORAGE_VERSIONS)('compactInitialReplication (storage v%s)', async (storageVersion) => {
      // Populate old replication stream
      const { factory } = await setup(storageVersion);

      // Now populate another replication stream (bucket definition name changed)
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
bucket_definitions:
  by_user2:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules);
      const syncRulesContent = syncRules.syncConfigContent[0];

      await populate(bucketStorage, 2);
      const { checkpoint } = await bucketStorage.getCheckpoint();

      // V3's initial lite pass processes every bucket with no prior compact state.
      // Earlier storage versions use the default minimum-change threshold.
      const result0 = await bucketStorage.compactInitialReplication({
        maxOpId: checkpoint
      });
      expect(result0.buckets).toEqual(storageVersion >= storage.STORAGE_VERSION_3 ? 2 : 0);

      // For V1/V2, lower the threshold to populate the checksum cache. V3 has
      // already updated its compacted state, so another initial pass is a no-op.
      const result1 = await bucketStorage.compactInitialReplication({
        maxOpId: checkpoint,
        minBucketChanges: 1
      });
      expect(result1.buckets).toEqual(storageVersion >= storage.STORAGE_VERSION_3 ? 0 : 2);

      // Repeating it stays a no-op.
      const result2 = await bucketStorage.compactInitialReplication({
        maxOpId: checkpoint,
        minBucketChanges: 1
      });
      expect(result2.buckets).toEqual(0);

      const users = ['u1', 'u2'];
      const userRequests = users.map((user) => bucketRequest(syncRulesContent, `by_user2["${user}"]`));
      const [u1Request, u2Request] = userRequests;
      const checksumAfter = await bucketStorage.getChecksums(test_utils.testCheckpoint(checkpoint), userRequests);
      expect(checksumAfter.get(u1Request.bucket)).toEqual({
        bucket: u1Request.bucket,
        checksum: -659469718,
        count: 1
      });
      expect(checksumAfter.get(u2Request.bucket)).toEqual({
        bucket: u2Request.bucket,
        checksum: 430217650,
        count: 1
      });
    });

    test('v3 initial chunk compaction includes writer-scheduled work with a custom chunk interval', async () => {
      const { bucketStorage, checkpoint } = await setup(storage.STORAGE_VERSION_3);
      const result = await (bucketStorage as MongoSyncBucketStorage)
        .createMongoCompactor({
          maxOpId: checkpoint,
          compactChunksOnly: true,
          minCompactChunkIntervalMs: 1
        })
        .compact();

      expect(result).toBe(2);
    });

    test('v3 repeated initial chunk compaction reschedules overdue full work beyond the current run', async () => {
      const { bucketStorage, checkpoint } = await setup(storage.STORAGE_VERSION_3);
      const firstResult = await bucketStorage.compactInitialReplication({ maxOpId: checkpoint });
      expect(firstResult.buckets).toBe(2);

      const bucketStateCollection = (bucketStorage.db as VersionedPowerSyncMongoV3).bucketState(
        bucketStorage.replicationStreamId
      );
      await bucketStateCollection.updateMany(
        {},
        {
          $set: {
            first_uncompacted_write: new Date(0),
            next_compact_check: new Date(0)
          }
        }
      );

      const [{ now }] = await (bucketStorage.db as VersionedPowerSyncMongoV3).db
        .aggregate<{ now: Date }>([{ $documents: [{}] }, { $project: { _id: 0, now: '$$NOW' } }])
        .toArray();
      const result = await bucketStorage.compactInitialReplication({
        maxOpId: checkpoint,
        signal: AbortSignal.timeout(2_000)
      });

      expect(result.buckets).toBe(0);
      const states = await bucketStateCollection.find({}).toArray();
      expect(states).toHaveLength(2);
      for (const state of states) {
        expect(state.next_compact_check!.getTime()).toBeGreaterThan(
          now.getTime() + DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS
        );
      }
    });

    test('v3 replication writes initialize scheduled compaction state', async () => {
      const { bucketStorage } = await setup();
      const storageDb = bucketStorage.db;

      if (!storageDb.storageConfig.incrementalReprocessing) {
        return;
      }
      const bucketStateCollection = (storageDb as VersionedPowerSyncMongoV3).bucketState(
        bucketStorage.replicationStreamId
      );
      const states = await bucketStateCollection.find({}).toArray();
      expect(states).toHaveLength(2);
      for (const state of states) {
        expect(state.last_op).toBeGreaterThan(0n);
        expect(state.bucket_stats.count).toBe(1);
        expect(state.bucket_stats.chunks).toBe(1);
        expect(state.first_uncompacted_write).toBeInstanceOf(Date);
        expect(state.next_compact_check).toBeInstanceOf(Date);
        expect(state.compacted_state).toBeUndefined();
        expect(state).not.toHaveProperty('estimate_since_compact');

        const docs = await (storageDb as VersionedPowerSyncMongoV3)
          .bucketData(bucketStorage.replicationStreamId, state._id.d)
          .find({ '_id.b': state._id.b })
          .toArray();
        expect(state.bucket_stats.bytes).toBe(BigInt(docs.reduce((total, document) => total + document.size, 0)));
      }
    });
  });
});

describe('Mongo Sync Parameter Storage Compact', () => {
  register.registerParameterCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY);
});

/**
 * V3 Invariant Verification
 *
 * Tests in this block exercise two levels:
 *
 * Unit tests (no MongoDB): call serializeBucketData(), chunkBucketData(), or
 * loadBucketDataDocument() directly. These use makeBucketDataDoc() with fake
 * bucket keys and source table IDs.
 *
 * Integration tests (full MongoDB): provision V3 storage, insert pre-serialized
 * documents directly into collections, trigger compaction via
 * bucketStorage.compact(), then read back and verify. Surfaces exercised:
 *
 *   - collection.insertMany (direct bucket_data writes)
 *   - bucketStateCollection.insertOne (so compactor discovers buckets)
 *   - bucketStorage.compact() (full pipeline: dirtyBucketBatches →
 *     compactSingleBucket → delete-all + rechunk in transaction →
 *     writeBucketStateUpdates)
 *   - collection.find with _id.b filters (verify post-compaction state)
 *
 * The initial write path (MongoBucketBatchV3 → flushBucketDataShared →
 * chunkBucketData → serializeBucketData → bulk write) is NOT exercised here —
 * that path is covered by the existing shared tests in register-compacting-tests.ts.
 */
describe('V3 invariant verification', () => {
  const BUCKET = 'global[]';
  const TABLE = 'items';

  function makeBucketDataDoc(overrides: Partial<BucketDataDoc> & { o: bigint }): BucketDataDoc {
    return {
      bucketKey: { replicationStreamId: 1, definitionId: '1', bucket: 'test[]' },
      op: 'PUT',
      source_table: new bson.ObjectId(),
      source_key: 'key',
      table: 'test',
      row_id: 'row1',
      checksum: 1n,
      data: '{"id":"row1"}',
      ...overrides
    };
  }

  async function setupV3Storage() {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data: [SELECT id as id, description FROM items]
`,
        { storageVersion: 3 }
      )
    );
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const mapping = syncRules.syncConfigContent[0].mapping;
    const definitionId = mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.replicationStreamId,
      definitionId,
      bucket: BUCKET
    };

    return { bucketStorage, syncRules, db, collection, bucketStateCollection, sourceTableId, ctx, definitionId };
  }

  function makeOp(
    opId: number,
    rowId: string,
    data: string,
    ctx: { replicationStreamId: number; definitionId: string; bucket: string },
    sourceTableId: bson.ObjectId,
    overrides?: { op?: 'PUT' | 'REMOVE' }
  ): BucketDataDoc {
    return {
      bucketKey: {
        replicationStreamId: ctx.replicationStreamId,
        definitionId: ctx.definitionId,
        bucket: ctx.bucket
      },
      o: BigInt(opId),
      op: overrides?.op ?? 'PUT',
      source_table: sourceTableId,
      source_key: test_utils.rid(rowId),
      table: TABLE,
      row_id: rowId,
      checksum: BigInt(opId * 7),
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data })
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocumentV3[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: { count: 10, bytes: 100n, chunks: 1 }
    });
  }

  async function compact(bucketStorage: MongoSyncBucketStorage, maxOpId: bigint) {
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      maxOpId
    });
  }

  test('a successful lease renewal clears a transient renewal error', async () => {
    const { bucketStateCollection, ctx } = await setupV3Storage();
    await insertBucketState(bucketStateCollection, ctx.definitionId, 1n);
    const lease = await CompactionLease.claim(
      bucketStateCollection,
      { _id: { d: ctx.definitionId, b: BUCKET } },
      undefined,
      10 * 60 * 1000
    );
    expect(lease).not.toBeNull();

    try {
      const transientError = new Error('temporary MongoDB error');
      const renew = (lease as any).renew.bind(lease);
      const updateOne = vi.spyOn(bucketStateCollection, 'updateOne').mockRejectedValueOnce(transientError);
      await renew().catch((error: unknown) => {
        (lease as any).renewalError = error;
      });
      await expect(lease!.throwIfLost()).rejects.toBe(transientError);

      updateOne.mockRestore();
      await renew();
      await expect(lease!.throwIfLost()).resolves.toBeUndefined();
    } finally {
      await lease?.[Symbol.asyncDispose]();
    }
  });

  test('a failed scheduled bucket does not block other scheduled buckets', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const badBucket = 'bad[]';
    const goodBucket = 'good[]';
    const goodDocument = serializeBucketData(goodBucket, [
      makeOp(2, 'good', 'good', { ...ctx, bucket: goodBucket }, sourceTableId)
    ]);
    await insertDocs(collection, [goodDocument]);
    await bucketStateCollection.insertMany([
      {
        _id: { d: ctx.definitionId, b: badBucket },
        last_op: 2n,
        next_compact_check: new Date(0),
        // A malformed scheduled bucket with an expired lease must be
        // rescheduled without preventing valid buckets from compacting.
        first_uncompacted_write: new Date(0),
        bucket_stats: { count: 1, bytes: 1n, chunks: 1 },
        compact_lease: { id: new bson.ObjectId(), expires_at: new Date(0) }
      },
      {
        _id: { d: ctx.definitionId, b: goodBucket },
        last_op: 2n,
        next_compact_check: new Date(0),
        first_uncompacted_write: new Date(0),
        bucket_stats: { count: 1, bytes: BigInt(goodDocument.size), chunks: 1 }
      }
    ]);
    await bucketStateCollection.updateOne(
      { _id: { d: ctx.definitionId, b: badBucket } },
      { $unset: { first_uncompacted_write: '' } }
    );

    await (bucketStorage as MongoSyncBucketStorage)
      .createMongoCompactor({ maxOpId: 2n, compactChunksOnly: true })
      .compact();

    const [badState, goodState] = await Promise.all([
      bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: badBucket } }),
      bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: goodBucket } })
    ]);
    expect(badState?.next_compact_check).toBeInstanceOf(Date);
    expect(badState!.next_compact_check!.getTime()).toBeGreaterThan(0);
    expect(goodState?.compacted_state?.op_id).toBe(2n);
  });

  test('1. ops[] ordering - preserves caller ordering (no implicit sort)', () => {
    const ops = [
      makeBucketDataDoc({ o: 5n, data: '{"id":"c"}' }),
      makeBucketDataDoc({ o: 3n, data: '{"id":"a"}' }),
      makeBucketDataDoc({ o: 7n, data: '{"id":"b"}' })
    ];

    const doc = serializeBucketData('test[]', ops);

    expect(doc.ops!.map((op) => op.o)).toEqual([5n, 3n, 7n]);
  });

  test('1. ops[] ordering - preserved after compaction', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'A', 'a2', ctx, sourceTableId),
      makeOp(30, 'A', 'a3', ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 30n);

    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    for (const doc of docs) {
      for (let i = 1; i < doc.ops!.length; i++) {
        expect(doc.ops![i].o).toBeGreaterThanOrEqual(doc.ops![i - 1].o);
      }
    }
  });

  test('2. range metadata consistency - serializeBucketData fields', () => {
    const ops = [
      makeBucketDataDoc({ o: 3n, checksum: 10n, data: 'aaaa' }),
      makeBucketDataDoc({ o: 5n, checksum: 20n, data: 'bbbbb' }),
      makeBucketDataDoc({ o: 8n, checksum: 30n, data: 'cccccccc' })
    ];

    const doc = serializeBucketData('test[]', ops);

    expect(doc._id.o).toBe(8n);
    expect(doc.min_op).toBe(3n);
    expect(doc.count).toBe(3);
    expect(doc.checksum).toBe(60n);
    expect(doc.size).toBe(bson.calculateObjectSize(doc.ops!));
  });

  test('2. has_clear_op is only stored when true', () => {
    const putDoc = serializeBucketData('test[]', [makeBucketDataDoc({ o: 1n })]);
    const clearDoc = serializeBucketData('test[]', [makeBucketDataDoc({ o: 2n, op: 'CLEAR', data: null })]);

    expect(putDoc).not.toHaveProperty('has_clear_op');
    expect(clearDoc.has_clear_op).toBe(true);
  });

  test('2. range metadata consistency - after compaction', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 20n);

    await compact(bucketStorage, 20n);

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    for (const doc of docs) {
      expect(doc._id.o).toBe(doc.ops!.reduce((max, op) => (op.o > max ? op.o : max), 0n));
      expect(doc.min_op).toBe(doc.ops!.reduce((min, op) => (op.o < min ? op.o : min), doc.ops![0].o));
      expect(doc.count).toBe(doc.ops!.length);
      expect(doc.checksum).toBe(doc.ops!.reduce((sum, op) => sum + op.checksum, 0n));
      expect(doc.size).toBe(bson.calculateObjectSize(doc.ops!));
    }
  });

  test('4. no overlapping ranges - multiple documents', () => {
    const opsA = [makeBucketDataDoc({ o: 1n }), makeBucketDataDoc({ o: 3n })];
    const opsB = [makeBucketDataDoc({ o: 5n }), makeBucketDataDoc({ o: 8n })];
    const opsC = [makeBucketDataDoc({ o: 10n }), makeBucketDataDoc({ o: 15n })];

    const docA = serializeBucketData('test[]', opsA);
    const docB = serializeBucketData('test[]', opsB);
    const docC = serializeBucketData('test[]', opsC);

    const docs = [docA, docB, docC];

    for (let i = 0; i < docs.length; i++) {
      for (let j = i + 1; j < docs.length; j++) {
        const a = docs[i];
        const b = docs[j];
        const noOverlap = a.min_op > b._id.o || b.min_op > a._id.o;
        expect(noOverlap).toBe(true);
      }
    }
  });

  test('4. no overlapping ranges - after compaction', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'C', 'c1', ctx, sourceTableId), makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const ops3 = [makeOp(50, 'D', 'd1', ctx, sourceTableId), makeOp(60, 'E', 'e1', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    const doc3 = serializeBucketData(BUCKET, ops3);
    await insertDocs(collection, [doc1, doc2, doc3]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 60n);

    await compact(bucketStorage, 60n);

    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    for (let i = 0; i < docs.length; i++) {
      for (let j = i + 1; j < docs.length; j++) {
        const a = docs[i];
        const b = docs[j];
        const noOverlap = a.min_op > b._id.o || b.min_op > a._id.o;
        expect(noOverlap).toBe(true);
      }
    }
  });

  test('6. compaction survivor integrity - superseded ops become MOVE tombstones', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'A', 'a2', ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 30n);

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    const allOps = docs.flatMap((d) => d.ops!);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(1);
    expect(moveOps[0].o).toBe(10n);
    expect(moveOps[0].checksum).toBe(ops[0].checksum);
    expect(moveOps[0].data).toBeNull();
    const putOps = allOps.filter((op) => op.op === 'PUT');
    expect(putOps.length).toBe(2);
    expect(docs[0].target_op).toBe(30n);
  });

  test('6. compaction survivor integrity - MOVE ops preserved', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      { ...makeOp(20, 'A', 'a2', ctx, sourceTableId), op: 'MOVE' as const, data: null },
      makeOp(30, 'A', 'a3', ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 30n);

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    const allOps = docs.flatMap((d) => d.ops!);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    // Pre-existing MOVE@20 + new MOVE@10 are collapsed into CLEAR
    expect(moveOps.length).toBe(0);
    const clearOps = allOps.filter((op) => op.op === 'CLEAR');
    expect(clearOps.length).toBe(1);
    expect(clearOps[0].o).toBe(20n);
    const putOps = allOps.filter((op) => op.op === 'PUT');
    expect(putOps.length).toBe(1);
    expect(putOps[0].o).toBe(30n);
  });

  test('6. compaction survivor integrity - CLEAR ops preserved', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      { ...makeOp(25, 'A', '', ctx, sourceTableId), op: 'CLEAR' as const, data: null, row_id: undefined }
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 25n);

    await compact(bucketStorage, 25n);

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    const allOps = docs.flatMap((d) => d.ops!);
    const clearOps = allOps.filter((op) => op.op === 'CLEAR');
    expect(clearOps.length).toBe(1);
  });

  test('7. superseded PUT becomes MOVE tombstone, REMOVE survives', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'A', 'a2', ctx, sourceTableId, { op: 'REMOVE' })
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 20n);

    await compact(bucketStorage, 20n);

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    const allOps = docs.flatMap((d) => d.ops!);
    // MOVE@10 + REMOVE@20 collapsed into CLEAR@20
    const clearOps = allOps.filter((op) => op.op === 'CLEAR');
    expect(clearOps.length).toBe(1);
    expect(clearOps[0].o).toBe(20n);
    expect(allOps.filter((op) => op.op === 'REMOVE').length).toBe(0);
    expect(allOps.filter((op) => op.op === 'MOVE').length).toBe(0);
  });

  test('7. empty bucket compact is a no-op', async () => {
    const { bucketStorage, collection } = await setupV3Storage();

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 1n
    });

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    expect(docs.length).toBe(0);
  });

  test('8. BSON limit safety - two large ops get split', () => {
    const halfMB = 600_000;
    const ops = [
      makeBucketDataDoc({ o: 1n, data: 'a'.repeat(halfMB) }),
      makeBucketDataDoc({ o: 2n, data: 'b'.repeat(halfMB) })
    ];

    const chunks = chunkBucketData(ops, DEFAULT_MAX_DOC_SIZE_BYTES);
    expect(chunks.length).toBe(2);
  });

  test('8. BSON limit safety - single oversized op gets own chunk', () => {
    const oversized = DEFAULT_MAX_DOC_SIZE_BYTES + 100_000;
    const ops = [makeBucketDataDoc({ o: 1n, data: 'x'.repeat(oversized) })];

    const chunks = chunkBucketData(ops, DEFAULT_MAX_DOC_SIZE_BYTES);
    expect(chunks.length).toBe(1);
    expect(chunks[0]).toHaveLength(1);
  });

  test('9. serialization fidelity - null data preserved', () => {
    const ops = [makeBucketDataDoc({ o: 1n, data: null })];
    const doc = serializeBucketData('test[]', ops);
    expect(doc.ops![0].data).toBeNull();

    const context = { replicationStreamId: 1, definitionId: '1' };
    const deserialized = [...loadBucketDataDocument(context, doc)];
    expect(deserialized[0].data).toBeNull();
  });

  test('9. serialization fidelity - empty string data preserved', () => {
    const ops = [makeBucketDataDoc({ o: 1n, data: '' })];
    const doc = serializeBucketData('test[]', ops);
    expect(doc.ops![0].data).toBe('');

    const context = { replicationStreamId: 1, definitionId: '1' };
    const deserialized = [...loadBucketDataDocument(context, doc)];
    expect(deserialized[0].data).toBe('');
  });

  test('9. serialization fidelity - unicode characters preserved', () => {
    const unicodeData = '{"name":"日本語テスト","emoji":"🎉"}';
    const ops = [makeBucketDataDoc({ o: 1n, data: unicodeData })];
    const doc = serializeBucketData('test[]', ops);
    expect(doc.ops![0].data).toBe(unicodeData);

    const context = { replicationStreamId: 1, definitionId: '1' };
    const deserialized = [...loadBucketDataDocument(context, doc)];
    expect(deserialized[0].data).toBe(unicodeData);
  });

  test('10. document _id.o invariant - equals last ops[*].o (caller must sort)', () => {
    const ops = [makeBucketDataDoc({ o: 10n }), makeBucketDataDoc({ o: 25n }), makeBucketDataDoc({ o: 7n })];

    const doc = serializeBucketData('test[]', ops);
    expect(doc._id.o).toBe(7n);
  });

  test('10. document _id.o invariant - equals max when pre-sorted', () => {
    const ops = [makeBucketDataDoc({ o: 3n }), makeBucketDataDoc({ o: 10n }), makeBucketDataDoc({ o: 25n })];

    const doc = serializeBucketData('test[]', ops);
    const maxO = ops.reduce((max, op) => (op.o > max ? op.o : max), 0n);
    expect(doc._id.o).toBe(maxO);
  });

  test('10. document _id.o invariant - every document after compaction', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'C', 'c1', ctx, sourceTableId), makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

    await compact(bucketStorage, 40n);

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    for (const doc of docs) {
      const maxO = doc.ops!.reduce((max, op) => (op.o > max ? op.o : max), 0n);
      expect(doc._id.o).toBe(maxO);
    }
  });

  test('compaction with maxOpId filtering excludes a straddling document', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'C', 'c1', ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 15n);

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const allOpsAfter = docsAfter.flatMap((d) => d.ops!);

    // The document ends above maxOpId, so the database query excludes it as a unit.
    expect(allOpsAfter.length).toBe(3);
    const opsBelow = allOpsAfter.filter((op) => op.o <= 15n);
    expect(opsBelow.length).toBe(1);
    expect(opsBelow[0].op).toBe('PUT');
    const opsAbove = allOpsAfter.filter((op) => op.o > 15n);
    expect(opsAbove.length).toBe(2);
    expect(opsAbove.every((op) => op.op === 'PUT')).toBe(true);

    // Checksum preserved
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(Number(doc1.checksum));
  });

  test('checksum consistency - aggregation pipeline matches JavaScript addChecksums', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3Storage();
    const ops = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 20n);

    const allOps = ops;
    let jsChecksum = 0;
    for (const op of allOps) {
      jsChecksum = addChecksums(jsChecksum, Number(op.checksum));
    }

    const docs = await collection.find({ '_id.b': BUCKET }).toArray();
    const storedOps = docs.flatMap((d) => d.ops!);
    let storedChecksum = 0;
    for (const op of storedOps) {
      storedChecksum = addChecksums(storedChecksum, Number(op.checksum));
    }

    expect(storedChecksum).toBe(jsChecksum);
  });
});

describe('V3 checksum pipeline straddling', () => {
  const BUCKET = 'global[]';
  const TABLE = 'items';

  async function setup() {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data: [SELECT id as id, description FROM items]
`,
        { storageVersion: 3 }
      )
    );
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const mapping = syncRules.syncConfigContent[0].mapping;
    const definitionId = mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    const sourceTableId = new bson.ObjectId();
    const ctx = {
      replicationStreamId: bucketStorage.replicationStreamId,
      definitionId,
      bucket: BUCKET
    };

    return { bucketStorage, syncRules, db, collection, bucketStateCollection, definitionId, sourceTableId, ctx };
  }

  function makeOp(
    opId: number,
    rowId: string,
    data: string,
    ctx: { replicationStreamId: number; definitionId: string; bucket: string },
    sourceTableId: bson.ObjectId
  ): BucketDataDoc {
    return {
      bucketKey: {
        replicationStreamId: ctx.replicationStreamId,
        definitionId: ctx.definitionId,
        bucket: ctx.bucket
      },
      o: BigInt(opId),
      op: 'PUT',
      source_table: sourceTableId,
      source_key: test_utils.rid(rowId),
      table: TABLE,
      row_id: rowId,
      checksum: BigInt(opId * 7),
      data: JSON.stringify({ id: rowId, description: data })
    };
  }

  function checksumRequest(): storage.BucketChecksumRequest {
    return {
      bucket: BUCKET,
      source: {
        uniqueName: 'global',
        bucketParameters: [],
        getSourceTables: () => new Set(),
        tableSyncsData: () => false,
        evaluateRow: () => [],
        inferSchema: () => ({ objects: {} }),
        bucketQuery: () => ({ ast: {} as any, parameters: [] })
      } as any
    };
  }

  test('start straddle recalculates a cached checksum from the beginning', async () => {
    const { bucketStorage, collection, bucketStateCollection, definitionId, sourceTableId, ctx } = await setup();

    // Single document with ops 10-60, min_op=10, _id.o=60
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'C', 'c1', ctx, sourceTableId),
      makeOp(40, 'D', 'd1', ctx, sourceTableId),
      makeOp(50, 'E', 'e1', ctx, sourceTableId),
      makeOp(60, 'F', 'f1', ctx, sourceTableId)
    ];
    // Cache the first half before compaction changes the document boundary.
    const firstDoc = serializeBucketData(BUCKET, ops.slice(0, 3));
    await collection.insertOne(firstDoc);

    const fullChecksum = ops.reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0);
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: 30n,
      next_compact_check: undefined,
      first_uncompacted_write: undefined,
      bucket_stats: { count: 3, bytes: 100n, chunks: 1 }
    });

    const request = checksumRequest();

    const cached = await bucketStorage.getChecksums(test_utils.testCheckpoint(30n), [request]);
    expect(cached.get(BUCKET)).toMatchObject({ count: 3 });

    // Compaction rechunks the bucket into one document spanning the cached
    // checkpoint. The requested endpoint is the end of the new document.
    await collection.deleteMany({});
    await collection.insertOne(serializeBucketData(BUCKET, ops, { targetOp: 60n }));

    const result = await bucketStorage.getChecksums(test_utils.testCheckpoint(60n), [request]);
    const checksumResult = result.get(BUCKET)!;

    // The cached checkpoint at 30 is inside the new document. The full result
    // must replace it rather than add the document checksum to it.
    expect(checksumResult.checksum).toBe(fullChecksum);
    expect(checksumResult.count).toBe(6);
  });

  test('end straddle invalidates the old checkpoint and a later checkpoint succeeds', async () => {
    const { bucketStorage, collection, bucketStateCollection, definitionId, sourceTableId, ctx } = await setup();

    // Document with ops 40-60, _id.o=60, min_op=40
    const ops = [
      makeOp(40, 'D', 'd1', ctx, sourceTableId),
      makeOp(50, 'E', 'e1', ctx, sourceTableId),
      makeOp(60, 'F', 'f1', ctx, sourceTableId)
    ];
    const doc = serializeBucketData(BUCKET, ops, { targetOp: 60n });
    await collection.insertMany([doc]);

    const checksumAllOps = ops.reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0);

    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: 0n,
      next_compact_check: undefined,
      first_uncompacted_write: undefined,
      bucket_stats: { count: 0, bytes: 0n, chunks: 0 }
    });

    const request = checksumRequest();

    await expect(bucketStorage.getChecksums(test_utils.testCheckpoint(45n), [request])).rejects.toBeInstanceOf(
      CheckpointChecksumInvalidatedError
    );

    const result = await bucketStorage.getChecksums(test_utils.testCheckpoint(60n), [request]);
    expect(result.get(BUCKET)).toEqual({ bucket: BUCKET, checksum: checksumAllOps, count: 3 });
  });

  test('end straddle without target_op also invalidates the old checkpoint', async () => {
    const { bucketStorage, collection, bucketStateCollection, definitionId, sourceTableId, ctx } = await setup();
    const ops = [makeOp(40, 'D', 'd1', ctx, sourceTableId), makeOp(60, 'F', 'f1', ctx, sourceTableId)];
    await collection.insertOne(serializeBucketData(BUCKET, ops));
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: 0n,
      next_compact_check: undefined,
      first_uncompacted_write: undefined,
      bucket_stats: { count: 0, bytes: 0n, chunks: 0 }
    });

    const request = checksumRequest();
    await expect(bucketStorage.getChecksums(test_utils.testCheckpoint(45n), [request])).rejects.toBeInstanceOf(
      CheckpointChecksumInvalidatedError
    );
  });

  test('has_clear_op replaces a cached checksum', async () => {
    const { bucketStorage, collection, bucketStateCollection, definitionId, sourceTableId, ctx } = await setup();
    const beforeClear = makeOp(10, 'A', 'a1', ctx, sourceTableId);
    const clear = { ...makeOp(20, 'clear', '', ctx, sourceTableId), op: 'CLEAR' as const, checksum: 101n, data: null };
    const afterClear = makeOp(30, 'B', 'b1', ctx, sourceTableId);

    await collection.insertOne(serializeBucketData(BUCKET, [beforeClear]));
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: 10n,
      next_compact_check: undefined,
      first_uncompacted_write: undefined,
      bucket_stats: { count: 1, bytes: 100n, chunks: 1 }
    });

    const request = checksumRequest();
    const before = await bucketStorage.getChecksums(test_utils.testCheckpoint(10n), [request]);
    expect(before.get(BUCKET)).toEqual({ bucket: BUCKET, checksum: 70, count: 1 });

    // Compaction replaces everything preceding CLEAR, while the checksum cache
    // still contains the old checkpoint.
    await collection.deleteMany({});
    await collection.insertOne(serializeBucketData(BUCKET, [clear, afterClear]));
    await bucketStateCollection.updateOne(
      { _id: { d: definitionId, b: BUCKET } },
      { $set: { last_op: 30n, 'bucket_stats.count': 2 } }
    );

    const after = await bucketStorage.getChecksums(test_utils.testCheckpoint(30n), [request]);
    expect(after.get(BUCKET)).toEqual({
      bucket: BUCKET,
      checksum: addChecksums(Number(clear.checksum), Number(afterClear.checksum)),
      count: 2
    });
  });
});

describe('V3 compaction boundaries', () => {
  const BUCKET = 'global[]';
  const TABLE = 'items';

  async function setupV3() {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data: [SELECT id as id, description FROM items]
`,
        { storageVersion: 3 }
      )
    );
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const mapping = syncRules.syncConfigContent[0].mapping;
    const definitionId = mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.replicationStreamId,
      definitionId,
      bucket: BUCKET
    };

    return { bucketStorage, syncRules, db, collection, bucketStateCollection, sourceTableId, ctx };
  }

  function makeOp(
    opId: number,
    rowId: string,
    data: string,
    ctx: { replicationStreamId: number; definitionId: string; bucket: string },
    sourceTableId: bson.ObjectId,
    overrides?: { op?: 'PUT' | 'REMOVE' }
  ): BucketDataDoc {
    return {
      bucketKey: {
        replicationStreamId: ctx.replicationStreamId,
        definitionId: ctx.definitionId,
        bucket: ctx.bucket
      },
      o: BigInt(opId),
      op: overrides?.op ?? 'PUT',
      source_table: sourceTableId,
      source_key: test_utils.rid(rowId),
      table: TABLE,
      row_id: rowId,
      checksum: BigInt(opId * 7),
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data })
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocumentV3[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: { count: 10, bytes: 100n, chunks: 1 }
    });
  }

  async function compact(bucketStorage: MongoSyncBucketStorage, maxOpId: bigint) {
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId
    });
  }

  async function readAllOps(collection: any): Promise<{ row_id: string; o: bigint; op: string }[]> {
    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    return docs.flatMap((d: any) =>
      d.ops!.map((op: any) => ({ row_id: op.row_id!, o: op.o, op: op.op, target_op: op.target_op ?? undefined }))
    );
  }

  async function readAllDocs(collection: any): Promise<BucketDataDocumentV3[]> {
    return collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
  }

  test('scheduled compaction claims only due buckets and clears completed full work', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const docs = [
      serializeBucketData(BUCKET, [makeOp(1, 'A', 'old', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(2, 'A', 'new', ctx, sourceTableId)])
    ];
    await insertDocs(collection, docs);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 2n,
      next_compact_check: new Date(Date.now() + 60_000),
      first_uncompacted_write: new Date(Date.now() - 60_000),
      bucket_stats: {
        count: 2,
        bytes: BigInt(docs[0].size + docs[1].size),
        chunks: 2
      }
    });

    await bucketStorage.compact({ maxOpId: 2n, maxCompactFullIntervalMs: 0 });
    expect(
      (await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } }))?.compacted_state
    ).toBeUndefined();

    await bucketStateCollection.updateOne(
      { _id: { d: ctx.definitionId, b: BUCKET } },
      { $set: { next_compact_check: new Date(Date.now() - 1) } }
    );
    await bucketStorage.compact({ maxOpId: 2n, maxCompactFullIntervalMs: 0 });

    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.last_full_compact?.op_id).toBe(2n);
    expect(state?.first_uncompacted_write).toBeUndefined();
    expect(state?.next_compact_check).toBeUndefined();
    expect(state?.compact_lease).toBeUndefined();
  });

  test('capped full compaction records its prefix and starts a fresh scheduling window', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const prefix = serializeBucketData(BUCKET, [makeOp(1, 'A', 'a', ctx, sourceTableId)]);
    const untouchedTail = serializeBucketData(BUCKET, [makeOp(2, 'B', 'b', ctx, sourceTableId)]);
    await insertDocs(collection, [prefix, untouchedTail]);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 2n,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: {
        count: prefix.count + untouchedTail.count,
        bytes: BigInt(prefix.size + untouchedTail.size),
        chunks: 2
      }
    });

    await bucketStorage.compact({ maxOpId: 1n });

    const partialState = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(partialState?.last_full_compact).toMatchObject({
      op_id: 1n,
      count: prefix.count,
      puts: 1
    });
    expect(partialState?.first_uncompacted_write).toBeInstanceOf(Date);
    expect(partialState!.first_uncompacted_write!.getTime()).toBeGreaterThan(0);
    expect(partialState!.next_compact_check!.getTime()).toBeGreaterThanOrEqual(
      partialState!.first_uncompacted_write!.getTime() + DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS
    );

    const lastFullCompactAt = partialState!.last_full_compact!.at;
    const freshFirstUncompactedWrite = partialState!.first_uncompacted_write!;
    await bucketStateCollection.updateOne(
      { _id: { d: ctx.definitionId, b: BUCKET } },
      { $set: { next_compact_check: new Date(0) } }
    );

    await bucketStorage.compact({ maxOpId: 2n });

    const deferredState = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(deferredState?.last_full_compact?.at).toEqual(lastFullCompactAt);
    expect(deferredState?.first_uncompacted_write).toEqual(freshFirstUncompactedWrite);
    expect(deferredState!.next_compact_check!.getTime()).toBeGreaterThan(Date.now());

    // Even after the scheduling window has elapsed, a run with the old cap
    // must not scan and record the same full-compaction prefix again.
    await bucketStateCollection.updateOne(
      { _id: { d: ctx.definitionId, b: BUCKET } },
      { $set: { first_uncompacted_write: new Date(0), next_compact_check: new Date(0) } }
    );

    await bucketStorage.compact({ maxOpId: 1n });

    const rescheduledState = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(rescheduledState?.last_full_compact?.at).toEqual(lastFullCompactAt);
    expect(rescheduledState?.first_uncompacted_write).toEqual(new Date(0));
    expect(rescheduledState!.next_compact_check!.getTime()).toBeGreaterThan(0);
  });

  test('explicit compaction skips a bucket with no outstanding full-compaction work', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const document = serializeBucketData(BUCKET, [makeOp(1, 'A', 'value', ctx, sourceTableId)]);
    await insertDocs(collection, [document]);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 1n,
      next_compact_check: undefined,
      first_uncompacted_write: undefined,
      compacted_state: {
        op_id: 1n,
        checksum: document.checksum,
        count: document.count,
        bytes: BigInt(document.size),
        chunks: 1,
        at: new Date()
      },
      last_full_compact: {
        op_id: 1n,
        count: document.count,
        puts: 1,
        at: new Date()
      },
      bucket_stats: { count: document.count, bytes: BigInt(document.size), chunks: 1 }
    });

    await expect(bucketStorage.compact({ compactBuckets: [BUCKET], maxOpId: 1n })).resolves.toBeUndefined();

    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.compact_lease).toBeUndefined();
    expect(state?.next_compact_check).toBeNull();
    expect(state?.last_full_compact?.op_id).toBe(1n);
  });

  test('concurrent scheduled compactors lease a bucket to one worker', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const document = serializeBucketData(BUCKET, [makeOp(1, 'A', 'value', ctx, sourceTableId)]);
    await insertDocs(collection, [document]);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 1n,
      next_compact_check: new Date(Date.now() - 1),
      first_uncompacted_write: new Date(0),
      bucket_stats: { count: 1, bytes: BigInt(document.size), chunks: 1 }
    });

    await Promise.all([bucketStorage.compact({ maxOpId: 1n }), bucketStorage.compact({ maxOpId: 1n })]);

    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.last_full_compact?.op_id).toBe(1n);
    expect(state?.compact_lease).toBeUndefined();
  });

  test('1. superseded ops become MOVE tombstones', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, B@20, A@30] — A@10 superseded by A@30, becomes MOVE tombstone
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'A', 'a2', ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 30n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(3);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'A', o: 30n, op: 'PUT' });
  });

  test('2. first op in document superseded becomes MOVE tombstone', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, B@20, C@30], Doc2: [A@40] — A@10 superseded by A@40
    const ops1 = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'C', 'c1', ctx, sourceTableId)
    ];
    const ops2 = [makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

    await compact(bucketStorage, 40n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(4);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'C', o: 30n, op: 'PUT' });
    expect(surviving[3]).toMatchObject({ row_id: 'A', o: 40n, op: 'PUT' });
  });

  test('3. last op in document superseded becomes MOVE tombstone', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, B@20], Doc2: [C@30, A@40] — A@10 superseded by A@40
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'C', 'c1', ctx, sourceTableId), makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

    await compact(bucketStorage, 40n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(4);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'C', o: 30n, op: 'PUT' });
    expect(surviving[3]).toMatchObject({ row_id: 'A', o: 40n, op: 'PUT' });
  });

  test('4. cascading superseded ops become MOVE tombstones', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, A@20] — A@10 superseded by A@20; A@20 superseded by A@30 REMOVE
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'A', 'a2', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'A', 'a3', ctx, sourceTableId, { op: 'REMOVE' })];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 30n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(1);
    expect(surviving[0]).toMatchObject({ op: 'CLEAR', o: 30n });
  });

  test('5. one surviving PUT per document plus MOVE tombstones', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, B@20], Doc2: [C@30, A@40] — A@10 superseded, becomes MOVE
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'C', 'c1', ctx, sourceTableId), makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

    await compact(bucketStorage, 40n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(4);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'C', o: 30n, op: 'PUT' });
    expect(surviving[3]).toMatchObject({ row_id: 'A', o: 40n, op: 'PUT' });
  });

  test('6. multiple superseded ops from different documents become MOVE tombstones', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // 3 small docs, each with 2 ops, A@10 superseded by A@40
    // Doc1: [A@10, B@20] — A@10 becomes MOVE
    // Doc2: [C@30, A@40] — C@30 survives (A@40 is latest A)
    // Doc3: [D@50, E@60] — D@50, E@60 survive
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'C', 'c1', ctx, sourceTableId), makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const ops3 = [makeOp(50, 'D', 'd1', ctx, sourceTableId), makeOp(60, 'E', 'e1', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    const doc3 = serializeBucketData(BUCKET, ops3);
    await insertDocs(collection, [doc1, doc2, doc3]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 60n);

    await compact(bucketStorage, 60n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(6);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'C', o: 30n, op: 'PUT' });
    expect(surviving[3]).toMatchObject({ row_id: 'A', o: 40n, op: 'PUT' });
    expect(surviving[4]).toMatchObject({ row_id: 'D', o: 50n, op: 'PUT' });
    expect(surviving[5]).toMatchObject({ row_id: 'E', o: 60n, op: 'PUT' });
  });

  test('7. superseded op at document boundary becomes MOVE tombstone', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, B@20, C@30], Doc2: [D@40, A@50] — A@10 superseded by A@50
    const ops1 = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'C', 'c1', ctx, sourceTableId)
    ];
    const ops2 = [makeOp(40, 'D', 'd1', ctx, sourceTableId), makeOp(50, 'A', 'a2', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 50n);

    await compact(bucketStorage, 50n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(5);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'C', o: 30n, op: 'PUT' });
    expect(surviving[3]).toMatchObject({ row_id: 'D', o: 40n, op: 'PUT' });
    expect(surviving[4]).toMatchObject({ row_id: 'A', o: 50n, op: 'PUT' });
  });

  test('8. same row_id ops spanning document boundary produces MOVE tombstone', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    // Doc1: [A@10, B@20], Doc2: [A@30, C@40] — A@10 superseded by A@30
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'A', 'a2', ctx, sourceTableId), makeOp(40, 'C', 'c1', ctx, sourceTableId)];
    const doc1 = serializeBucketData(BUCKET, ops1);
    const doc2 = serializeBucketData(BUCKET, ops2);
    await insertDocs(collection, [doc1, doc2]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

    await compact(bucketStorage, 40n);

    const surviving = await readAllOps(collection);
    expect(surviving).toHaveLength(4);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ row_id: 'B', o: 20n, op: 'PUT' });
    expect(surviving[2]).toMatchObject({ row_id: 'A', o: 30n, op: 'PUT' });
    expect(surviving[3]).toMatchObject({ row_id: 'C', o: 40n, op: 'PUT' });
  });
});

describe('V3 MOVE tombstone properties', () => {
  const BUCKET = 'global[]';
  const TABLE = 'items';

  async function setupV3() {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data: [SELECT id as id, description FROM items]
`,
        { storageVersion: 3 }
      )
    );
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const mapping = syncRules.syncConfigContent[0].mapping;
    const definitionId = mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.replicationStreamId,
      definitionId,
      bucket: BUCKET
    };

    return { bucketStorage, syncRules, db, collection, bucketStateCollection, sourceTableId, ctx };
  }

  function makeOp(
    opId: number,
    rowId: string,
    data: string,
    ctx: { replicationStreamId: number; definitionId: string; bucket: string },
    sourceTableId: bson.ObjectId,
    overrides?: { op?: 'PUT' | 'REMOVE' }
  ): BucketDataDoc {
    return {
      bucketKey: {
        replicationStreamId: ctx.replicationStreamId,
        definitionId: ctx.definitionId,
        bucket: ctx.bucket
      },
      o: BigInt(opId),
      op: overrides?.op ?? 'PUT',
      source_table: sourceTableId,
      source_key: test_utils.rid(rowId),
      table: TABLE,
      row_id: rowId,
      checksum: BigInt(opId * 7),
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data })
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocumentV3[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: { count: 10, bytes: 100n, chunks: 1 }
    });
  }

  async function compact(bucketStorage: MongoSyncBucketStorage, maxOpId: bigint) {
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId
    });
  }

  test('checksum preserved across compaction with superseded ops', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'A', 'a2', ctx, sourceTableId),
      makeOp(40, 'C', 'c1', ctx, sourceTableId),
      makeOp(50, 'B', 'b2', ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 50n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await compact(bucketStorage, 50n);

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    expect(checksumAfter).toBe(checksumBefore);

    const allOpsAfter = docsAfter.flatMap((d) => d.ops!);
    // Two MOVEs collapsed into one CLEAR
    expect(allOpsAfter.length).toBe(4);
    const clearOps = allOpsAfter.filter((op) => op.op === 'CLEAR');
    expect(clearOps.length).toBe(1);
  });

  test('checksum preserved across compaction with multiple documents', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const ops1 = [makeOp(10, 'A', 'a1', ctx, sourceTableId), makeOp(20, 'B', 'b1', ctx, sourceTableId)];
    const ops2 = [makeOp(30, 'C', 'c1', ctx, sourceTableId), makeOp(40, 'A', 'a2', ctx, sourceTableId)];
    const ops3 = [makeOp(50, 'D', 'd1', ctx, sourceTableId), makeOp(60, 'B', 'b2', ctx, sourceTableId)];
    await insertDocs(collection, [
      serializeBucketData(BUCKET, ops1),
      serializeBucketData(BUCKET, ops2),
      serializeBucketData(BUCKET, ops3)
    ]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 60n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await compact(bucketStorage, 60n);

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    expect(checksumAfter).toBe(checksumBefore);

    const allOpsAfter = docsAfter.flatMap((d) => d.ops!);
    // Two MOVEs collapsed into one CLEAR
    expect(allOpsAfter.length).toBe(5);
    const clearOps = allOpsAfter.filter((op) => op.op === 'CLEAR');
    expect(clearOps.length).toBe(1);
  });

  test('tombstones have null data and pack densely after rechunking', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const ops = [
      makeOp(10, 'A', 'x'.repeat(500_000), ctx, sourceTableId),
      makeOp(20, 'B', 'y'.repeat(500_000), ctx, sourceTableId),
      makeOp(30, 'A', 'z'.repeat(500_000), ctx, sourceTableId)
    ];
    const doc1 = serializeBucketData(BUCKET, ops);
    await insertDocs(collection, [doc1]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

    await compact(bucketStorage, 30n);

    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const allOps = docs.flatMap((d) => d.ops!);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(1);
    expect(moveOps[0].o).toBe(10n);
    expect(moveOps[0].data).toBeNull();

    const putOps = allOps.filter((op) => op.op === 'PUT');
    expect(putOps.length).toBe(2);
    expect(putOps.every((op) => op.data != null)).toBe(true);

    expect(docs.length).toBe(1);
    // Size calculation is not exact - we just check for a range
    expect(docs[0].size).toBeGreaterThan(1_000_000);
    expect(docs[0].size).toBeLessThan(1_001_000);
  });

  test('tombstones and survivors end up in same document after rechunking', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const ops1 = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'C', 'c1', ctx, sourceTableId)
    ];
    const ops2 = [makeOp(40, 'D', 'd1', ctx, sourceTableId), makeOp(50, 'A', 'a2', ctx, sourceTableId)];
    await insertDocs(collection, [serializeBucketData(BUCKET, ops1), serializeBucketData(BUCKET, ops2)]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 50n);

    const checksumBefore = (await collection.find({ '_id.b': BUCKET }).toArray()).reduce(
      (sum, d) => addChecksums(sum, Number(d.checksum)),
      0
    );

    await compact(bucketStorage, 50n);

    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docs.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    const allOps = docs.flatMap((d) => d.ops!);
    expect(allOps.length).toBe(5);
    const moveOp = allOps.find((op) => op.op === 'MOVE');
    expect(moveOp).toMatchObject({ o: 10n, data: null });

    expect(docs.length).toBe(1);

    const moveOpsInDoc = docs[0].ops!.filter((op) => op.op === 'MOVE');
    const putOpsInDoc = docs[0].ops!.filter((op) => op.op === 'PUT');
    expect(moveOpsInDoc.length).toBe(1);
    expect(putOpsInDoc.length).toBe(4);
  });
});

/**
 * Streaming compactor tests
 *
 * These tests exercise the streaming compactor's batched behavior:
 * reverse-order batched reads, cross-batch seen map dedup with memory
 * bounding, scoped deletes via $in, and byte-based batch cutting.
 */
describe('Streaming compactor', () => {
  const BUCKET = 'global[]';
  const TABLE = 'items';

  async function setupV3() {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data: [SELECT id as id, description FROM items]
`,
        { storageVersion: 3 }
      )
    );
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = bucketStorage.storageIds.bucketDefinitionIds[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.replicationStreamId,
      definitionId,
      bucket: BUCKET
    };

    return { bucketStorage, syncRules, db, collection, bucketStateCollection, sourceTableId, ctx };
  }

  function makeOp(
    opId: number,
    rowId: string,
    data: string,
    ctx: { replicationStreamId: number; definitionId: string; bucket: string },
    sourceTableId: bson.ObjectId,
    overrides?: { op?: 'PUT' | 'REMOVE' }
  ): BucketDataDoc {
    return {
      bucketKey: {
        replicationStreamId: ctx.replicationStreamId,
        definitionId: ctx.definitionId,
        bucket: ctx.bucket
      },
      o: BigInt(opId),
      op: overrides?.op ?? 'PUT',
      source_table: sourceTableId,
      source_key: test_utils.rid(rowId),
      table: TABLE,
      row_id: rowId,
      checksum: BigInt(opId * 7),
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data })
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocumentV3[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: { count: 10, bytes: 100n, chunks: 1 }
    });
  }

  async function readAllOps(
    collection: any
  ): Promise<{ row_id: string | undefined; o: bigint; op: string; target_op: bigint | undefined }[]> {
    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    return docs.flatMap((d: any) =>
      d.ops!.map((op: any) => ({
        row_id: op.row_id ?? undefined,
        o: op.o,
        op: op.op,
        target_op: op.target_op ?? undefined
      }))
    );
  }

  async function setupCompactedTail() {
    const setup = await setupV3();
    const { collection, bucketStateCollection, ctx, sourceTableId } = setup;
    const documents = [
      serializeBucketData(BUCKET, [makeOp(1, 'A', 'a', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(2, 'B', 'b', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(3, 'C', 'c', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(4, 'D', 'd', ctx, sourceTableId)])
    ];
    await insertDocs(collection, documents);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 4n,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      compacted_state: {
        op_id: 2n,
        checksum: documents[0].checksum + documents[1].checksum,
        count: documents[0].count + documents[1].count,
        bytes: BigInt(documents[0].size + documents[1].size),
        chunks: 2,
        at: new Date(0)
      },
      bucket_stats: {
        count: documents.reduce((total, document) => total + document.count, 0),
        bytes: BigInt(documents.reduce((total, document) => total + document.size, 0)),
        chunks: documents.length
      }
    });
    return setup;
  }

  async function expectRecoveredCompactedTail(collection: any, bucketStateCollection: any, definitionId: string) {
    const documents = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    expect(documents).toHaveLength(2);
    expect(documents.flatMap((document: BucketDataDocumentV3) => document.ops!.map((op) => op.o))).toEqual([
      1n,
      2n,
      3n,
      4n
    ]);

    const bytes = BigInt(documents.reduce((total: number, document: BucketDataDocumentV3) => total + document.size, 0));
    const state = await bucketStateCollection.findOne({ _id: { d: definitionId, b: BUCKET } });
    expect(state?.compacted_state).toMatchObject({
      op_id: 4n,
      checksum: 70n,
      count: 4,
      bytes,
      chunks: 2
    });
    expect(state?.bucket_stats).toEqual({ count: 4, bytes, chunks: 2 });
  }

  test('initial compaction merges small chunks and refreshes bucket metadata', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    await insertDocs(collection, [
      serializeBucketData(BUCKET, [makeOp(1, 'A', 'a', ctx, sourceTableId), makeOp(2, 'B', 'b', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(3, 'C', 'c', ctx, sourceTableId), makeOp(4, 'D', 'd', ctx, sourceTableId)])
    ]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 4n);

    const result = await bucketStorage.compactInitialReplication({ maxOpId: 4n });

    expect(result).toEqual({ buckets: 1 });
    const documents = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    expect(documents).toHaveLength(1);
    expect(documents[0].ops!.map((op) => op.o)).toEqual([1n, 2n, 3n, 4n]);

    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.compacted_state).toMatchObject({
      op_id: 4n,
      count: 4,
      checksum: 70n
    });
  });

  test('capped chunk compaction repairs stale bucket stats after a committed first merge', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const operations = [
      makeOp(1, 'A', 'a', ctx, sourceTableId),
      makeOp(2, 'B', 'b', ctx, sourceTableId),
      makeOp(3, 'C', 'c', ctx, sourceTableId),
      makeOp(4, 'D', 'd', ctx, sourceTableId)
    ];
    const originalDocuments = [
      serializeBucketData(BUCKET, operations.slice(0, 2)),
      serializeBucketData(BUCKET, operations.slice(2))
    ];
    const committedMerge = serializeBucketData(BUCKET, operations);
    const untouchedTail = serializeBucketData(BUCKET, [makeOp(5, 'E', 'e', ctx, sourceTableId)]);
    await insertDocs(collection, [committedMerge, untouchedTail]);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 5n,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: {
        count: originalDocuments.reduce((total, document) => total + document.count, untouchedTail.count),
        bytes: BigInt(originalDocuments.reduce((total, document) => total + document.size, untouchedTail.size)),
        chunks: originalDocuments.length + 1
      }
    });

    const result = await bucketStorage.compactInitialReplication({ maxOpId: 4n });

    expect(result).toEqual({ buckets: 1 });
    const expectedStats = {
      count: committedMerge.count + untouchedTail.count,
      bytes: BigInt(committedMerge.size + untouchedTail.size),
      chunks: 2
    };
    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.bucket_stats).toEqual(expectedStats);
    expect(state?.compacted_state).toMatchObject({
      op_id: 4n,
      checksum: committedMerge.checksum,
      count: committedMerge.count,
      bytes: BigInt(committedMerge.size),
      chunks: 1
    });
  });

  test('full compaction retry rebuilds stats after a committed replacement', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const documents = [
      serializeBucketData(BUCKET, [makeOp(1, 'A', 'a', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(2, 'B', 'b', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(3, 'C', 'c', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(4, 'D', 'd', ctx, sourceTableId)])
    ];
    await insertDocs(collection, documents);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 4n,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      bucket_stats: {
        count: documents.reduce((total, document) => total + document.count, 0),
        bytes: BigInt(documents.reduce((total, document) => total + document.size, 0)),
        chunks: documents.length
      }
    });

    const compactor = bucketStorage.createMongoCompactor({ maxOpId: 4n, compactBuckets: [BUCKET] });
    const originalFlush = (compactor as any).flushCompactionGroup.bind(compactor);
    let injectedFailure = false;
    vi.spyOn(compactor as any, 'flushCompactionGroup').mockImplementation(async (...args: any[]) => {
      const result = await originalFlush(...args);
      if (!injectedFailure) {
        injectedFailure = true;
        throw new ObjectStorageError('failure after committed full-compaction replacement', {
          cause: new Error('socket reset'),
          retryable: true
        });
      }
      return result;
    });

    await expect(compactor.compact()).resolves.toBe(1);

    expect(injectedFailure).toBe(true);
    const currentDocuments = await collection.find({ '_id.b': BUCKET }).toArray();
    expect(currentDocuments).toHaveLength(1);
    const expectedStats = {
      count: currentDocuments.reduce((total, document) => total + document.count, 0),
      bytes: BigInt(currentDocuments.reduce((total, document) => total + document.size, 0)),
      chunks: currentDocuments.length
    };
    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.bucket_stats).toEqual(expectedStats);
    expect(state?.compacted_state).toMatchObject({ op_id: 4n, ...expectedStats });
  });

  test('chunk compaction retry rebuilds state after a committed partial merge', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx } = await setupCompactedTail();
    const compactor = bucketStorage.createMongoCompactor({
      maxOpId: 4n,
      compactChunksOnly: true
    });
    const originalFlush = (compactor as any).flushCompactionGroup.bind(compactor);
    let injectedFailure = false;
    vi.spyOn(compactor as any, 'flushCompactionGroup').mockImplementation(async (...args: any[]) => {
      const result = await originalFlush(...args);
      if (!injectedFailure) {
        injectedFailure = true;
        throw new ObjectStorageError('failure after committed chunk merge', {
          cause: new Error('socket reset'),
          retryable: true
        });
      }
      return result;
    });

    await expect(compactor.compact()).resolves.toBe(1);

    expect(injectedFailure).toBe(true);
    await expectRecoveredCompactedTail(collection, bucketStateCollection, ctx.definitionId);
  });

  test('chunk compaction treats a missing cached op as a resume hint', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();
    const documents = [
      serializeBucketData(BUCKET, [makeOp(1, 'A', 'a', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(3, 'C', 'c', ctx, sourceTableId)]),
      serializeBucketData(BUCKET, [makeOp(4, 'D', 'd', ctx, sourceTableId)])
    ];
    await insertDocs(collection, documents);
    await bucketStateCollection.insertOne({
      _id: { d: ctx.definitionId, b: BUCKET },
      last_op: 4n,
      next_compact_check: new Date(0),
      first_uncompacted_write: new Date(0),
      compacted_state: {
        op_id: 2n,
        checksum: 21n,
        count: 2,
        bytes: 100n,
        chunks: 2,
        at: new Date(0)
      },
      bucket_stats: { count: 4, bytes: 200n, chunks: 4 }
    });

    await expect(bucketStorage.createMongoCompactor({ maxOpId: 4n, compactChunksOnly: true }).compact()).resolves.toBe(
      1
    );

    const currentDocuments = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const bytes = BigInt(currentDocuments.reduce((total, document) => total + document.size, 0));
    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.compacted_state).toMatchObject({
      op_id: 4n,
      checksum: 56n,
      count: 3,
      bytes,
      chunks: 2
    });
    expect(state?.bucket_stats).toEqual({ count: 3, bytes, chunks: 2 });
  });

  test('later chunk compaction rebuilds state after a committed partial merge', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx } = await setupCompactedTail();
    const failedCompactor = bucketStorage.createMongoCompactor({
      maxOpId: 4n,
      compactChunksOnly: true
    });
    const originalFlush = (failedCompactor as any).flushCompactionGroup.bind(failedCompactor);
    const flushSpy = vi
      .spyOn(failedCompactor as any, 'flushCompactionGroup')
      .mockImplementation(async (...args: any[]) => {
        const result = await originalFlush(...args);
        throw new Error('failure after committed chunk merge');
      });

    await expect(failedCompactor.compact()).resolves.toBe(0);
    flushSpy.mockRestore();

    const interruptedState = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(interruptedState?.compacted_state?.op_id).toBe(2n);
    expect(await collection.findOne({ _id: { b: BUCKET, o: 2n } })).toBeNull();
    await bucketStateCollection.updateOne(
      { _id: { d: ctx.definitionId, b: BUCKET } },
      { $set: { next_compact_check: new Date(0) } }
    );

    const result = await bucketStorage.createMongoCompactor({ maxOpId: 4n, compactChunksOnly: true }).compact();

    expect(result).toBe(1);
    await expectRecoveredCompactedTail(collection, bucketStateCollection, ctx.definitionId);
  });

  test('1. multi-batch compaction preserves checksum and creates MOVE tombstones', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 6 documents, 3-4 ops each, 3 rows with 2 versions spread across documents.
    // Rows A, B, C each have old+new versions:
    //   A: new@24 (Doc1), old@10 (Doc4)
    //   B: new@20 (Doc2), old@6  (Doc5)
    //   C: new@16 (Doc3), old@2  (Doc6)
    // With moveBatchQueryLimit=2, the streaming compactor must process
    // 2 documents per batch (3 batches total) and still produce correct output.
    const doc1 = serializeBucketData(BUCKET, [
      makeOp(21, 'D', 'd1', ctx, sourceTableId),
      makeOp(22, 'E', 'e1', ctx, sourceTableId),
      makeOp(23, 'F', 'f1', ctx, sourceTableId),
      makeOp(24, 'A', 'a_new', ctx, sourceTableId)
    ]);
    const doc2 = serializeBucketData(BUCKET, [
      makeOp(18, 'G', 'g1', ctx, sourceTableId),
      makeOp(19, 'H', 'h1', ctx, sourceTableId),
      makeOp(20, 'B', 'b_new', ctx, sourceTableId)
    ]);
    const doc3 = serializeBucketData(BUCKET, [
      makeOp(14, 'I', 'i1', ctx, sourceTableId),
      makeOp(15, 'J', 'j1', ctx, sourceTableId),
      makeOp(16, 'C', 'c_new', ctx, sourceTableId)
    ]);
    const doc4 = serializeBucketData(BUCKET, [
      makeOp(10, 'A', 'a_old', ctx, sourceTableId),
      makeOp(11, 'K', 'k1', ctx, sourceTableId),
      makeOp(12, 'L', 'l1', ctx, sourceTableId)
    ]);
    const doc5 = serializeBucketData(BUCKET, [
      makeOp(6, 'B', 'b_old', ctx, sourceTableId),
      makeOp(7, 'M', 'm1', ctx, sourceTableId),
      makeOp(8, 'N', 'n1', ctx, sourceTableId)
    ]);
    const doc6 = serializeBucketData(BUCKET, [
      makeOp(2, 'C', 'c_old', ctx, sourceTableId),
      makeOp(3, 'O', 'o1', ctx, sourceTableId),
      makeOp(4, 'P', 'p1', ctx, sourceTableId)
    ]);
    await insertDocs(collection, [doc1, doc2, doc3, doc4, doc5, doc6]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 24n);

    // Compute checksum before compaction
    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    // Compact with moveBatchQueryLimit=2 to force 3 batches of 2 documents each
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 2,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 24n
    });

    // Verify checksum preserved
    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    // Verify MOVE tombstones for superseded old versions (A@10, B@6, C@2)
    const allOps = await readAllOps(collection);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(3);
    expect(moveOps.map((op) => op.o).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))).toEqual([2n, 6n, 10n]);

    // Verify the newer version of each duplicated row survives as PUT
    const putOps = allOps.filter((op) => op.op === 'PUT');
    expect(putOps.length).toBe(16);
    const putRowIds = putOps.map((op) => op.row_id).sort();
    expect(putRowIds).toContain('A');
    expect(putRowIds).toContain('B');
    expect(putRowIds).toContain('C');
  });

  test('2. scoped delete isolation - ops above maxOpId preserved', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // Insert ops in _id.o range 100-500 via direct document insertion
    const doc1 = serializeBucketData(BUCKET, [
      makeOp(100, 'A', 'a1', ctx, sourceTableId),
      makeOp(200, 'B', 'b1', ctx, sourceTableId),
      makeOp(300, 'C', 'c1', ctx, sourceTableId)
    ]);
    const doc2 = serializeBucketData(BUCKET, [
      makeOp(400, 'D', 'd1', ctx, sourceTableId),
      makeOp(500, 'E', 'e1', ctx, sourceTableId)
    ]);
    // One extra document with _id.o=600 — clearly outside the batch range
    const docOutside = serializeBucketData(BUCKET, [makeOp(600, 'F', 'f_outside', ctx, sourceTableId)]);

    await insertDocs(collection, [doc1, doc2, docOutside]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 600n);

    // Run compaction with maxOpId=500n
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 2,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 500n
    });

    // The document at _id.o=600 must still be present and unmodified after compaction.
    // The streaming compactor should only touch documents containing ops <= maxOpId.
    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const allOpsAfter = docsAfter.flatMap((d) => d.ops!);
    const op600 = allOpsAfter.find((op) => op.o === 600n);
    expect(op600).toBeDefined();
    expect(op600!.op).toBe('PUT');
    expect(op600!.row_id).toBe('F');

    const state = await bucketStateCollection.findOne({ _id: { d: ctx.definitionId, b: BUCKET } });
    expect(state?.bucket_stats).toEqual({
      count: docsAfter.reduce((total, document) => total + document.count, 0),
      bytes: BigInt(docsAfter.reduce((total, document) => total + document.size, 0)),
      chunks: docsAfter.length
    });
  });

  test('3. seen map overflow - some old ops pass through without tombstoning', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 20 rows, each with 2 versions (old + new). 40 ops total.
    // memoryLimitMB=0.001 → idLimitBytes ≈ 1048 bytes.
    // The seen map adds ~140 bytes per entry (key.length + 140), so it can hold ~7 entries
    // before overflowing. The streaming compactor processes ops newest-first; after ~7 rows
    // are tracked, the seen map overflows and subsequent old-version ops are not tombstoned.
    const rows = Array.from({ length: 20 }, (_, i) => `row${i + 1}`);
    const allOps: BucketDataDoc[] = [];

    // Old versions at op_ids 1-20 (processed last by descending-order compactor)
    for (let i = 0; i < 20; i++) {
      allOps.push(makeOp(i + 1, rows[i], `old_${rows[i]}`, ctx, sourceTableId));
    }
    // New versions at op_ids 21-40 (processed first by descending-order compactor)
    for (let i = 0; i < 20; i++) {
      allOps.push(makeOp(i + 21, rows[i], `new_${rows[i]}`, ctx, sourceTableId));
    }

    // Single document containing all ops
    const doc = serializeBucketData(BUCKET, allOps);
    await insertDocs(collection, [doc]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

    // Compute checksum before compaction
    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 100,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 40n,
      memoryLimitMB: 0.001
    });

    // Bucket checksum must be correct even with overflow — old ops that pass through
    // as PUT still contribute their original checksum.
    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    // With seen map overflow, fewer than 20 old-version ops become MOVE tombstones.
    // Some old-version ops pass through as PUT because the seen map was full when
    // the compactor encountered them. With ~1048 bytes and ~176 bytes per entry,
    // approximately 5-7 rows are tracked before overflow, producing ~5-7 MOVE
    // tombstones from the 20 duplicate pairs.
    const opsAfter = await readAllOps(collection);
    const moveOps = opsAfter.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBeGreaterThan(0);
    expect(moveOps.length).toBeLessThanOrEqual(12);
  });

  test('5. scoped delete does not remove sandwiched non-processable docs', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // Doc1: processable (has ops <= maxOpId=300), _id.o = 300
    const doc1 = serializeBucketData(BUCKET, [
      makeOp(200, 'A', 'a1', ctx, sourceTableId),
      makeOp(300, 'B', 'b1', ctx, sourceTableId)
    ]);
    // Doc2: NON-processable (all ops > maxOpId=300), _id.o = 350
    // This doc is sandwiched between doc1 (_id.o=300) and doc3 (_id.o=500)
    // in _id.o sort order. A continuous range delete [200, 500] would
    // incorrectly catch this doc.
    const doc2 = serializeBucketData(BUCKET, [
      makeOp(340, 'X', 'x_sandwich', ctx, sourceTableId),
      makeOp(350, 'Y', 'y_sandwich', ctx, sourceTableId)
    ]);
    // Doc3: processable (has ops <= maxOpId=300), _id.o = 400
    const doc3 = serializeBucketData(BUCKET, [
      makeOp(250, 'C', 'c1', ctx, sourceTableId),
      makeOp(400, 'D', 'd1', ctx, sourceTableId)
    ]);

    await insertDocs(collection, [doc1, doc2, doc3]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 400n);

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 300n
    });

    // The sandwiched document (doc2, ops 340+350) must survive because ALL
    // its ops are > maxOpId — it should not be touched by compaction.
    const opsAfter = await readAllOps(collection);
    const sandwichOps = opsAfter.filter((op) => op.row_id === 'X' || op.row_id === 'Y');
    expect(sandwichOps.length).toBe(2);
    expect(sandwichOps.every((op) => op.op === 'PUT')).toBe(true);
  });

  test('6. byte-based read batches do not constrain output merging', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 6 documents, each with 2 PUT ops. With moveBatchByteLimit=400,
    // the byte limit should be hit after 1-2 documents, forcing multiple
    // batch iterations even though the document count limit (10) is never reached.
    const rows = Array.from({ length: 12 }, (_, i) => `row${i + 1}`);
    const docs: BucketDataDocumentV3[] = [];
    for (let i = 0; i < 6; i++) {
      docs.push(
        serializeBucketData(BUCKET, [
          makeOp(i * 2 + 1, rows[i * 2], `data_${rows[i * 2]}`, ctx, sourceTableId),
          makeOp(i * 2 + 2, rows[i * 2 + 1], `data_${rows[i * 2 + 1]}`, ctx, sourceTableId)
        ])
      );
    }
    expect(docs[0].size + docs[1].size).toBeGreaterThan(400);
    await insertDocs(collection, docs);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 12n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 400,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 12n
    });

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    // All 12 ops should survive — no superseding, just byte-based batching
    const allOps = await readAllOps(collection);
    expect(allOps.length).toBe(12);
    expect(allOps.every((op) => op.op === 'PUT')).toBe(true);

    // Read batches are only a memory bound. The pending output group crosses
    // those boundaries and merges all post-compaction results that fit.
    expect(docsAfter).toHaveLength(1);
    expect(docsAfter[0]).toMatchObject({
      min_op: docs[0].min_op,
      _id: docs[docs.length - 1]._id
    });
  });

  test('7. cross-batch seen map overflow - dedup continues across batches', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 10 rows (A-J), 2 versions each. 20 docs total, 1 PUT per doc.
    // New versions at opIds 20-11 (docs 1-10), old versions at opIds 10-1 (docs 11-20).
    // Processed newest-first: new versions fill the seen map; old versions that
    // were tracked get tombstoned; old versions beyond map capacity survive as PUT.
    const rows = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'];
    const docs: BucketDataDocumentV3[] = [];

    // New versions first (higher opIds — processed first in reverse order)
    for (let i = 0; i < 10; i++) {
      docs.push(
        serializeBucketData(BUCKET, [makeOp(20 - i, rows[i], `${rows[i].toLowerCase()}_new`, ctx, sourceTableId)])
      );
    }
    // Old versions (lower opIds — processed later)
    for (let i = 0; i < 10; i++) {
      docs.push(
        serializeBucketData(BUCKET, [makeOp(10 - i, rows[i], `${rows[i].toLowerCase()}_old`, ctx, sourceTableId)])
      );
    }
    await insertDocs(collection, docs);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 20n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    // moveBatchQueryLimit=5 forces 4 cross-batch iterations. memoryLimitMB=0.001
    // limits the seen map to ~6-7 entries, so overflow occurs mid-processing.
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 5,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 20n,
      memoryLimitMB: 0.001
    });

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    const allOps = await readAllOps(collection);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');

    // At least some rows were tracked before overflow → MOVE tombstones exist
    expect(moveOps.length).toBeGreaterThan(0);

    // Some old-version ops (o <= 10) survive as PUT — overflow prevented tracking
    const oldVersionPutOps = allOps.filter((op) => op.op === 'PUT' && op.o <= 10n);
    expect(oldVersionPutOps.length).toBeGreaterThan(0);

    // Not all 10 old versions became MOVEs (overflow occurred)
    expect(moveOps.length).toBeLessThan(10);

    // MOVEs have target_op stored at the document level (not per-op in V3).
    // Documents containing MOVEs should have non-null target_op.
    const docsWithMoves = docsAfter.filter((d) => d.ops!.some((op: any) => op.op === 'MOVE'));
    expect(docsWithMoves.length).toBeGreaterThan(0);
    for (const doc of docsWithMoves) {
      expect(doc.target_op).toBeDefined();
      expect(doc.target_op).not.toBeNull();
    }
  });

  test('8. all ops above maxOpId - paginates without writing', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 5 docs, each with 1 PUT op at high opIds (all > maxOpId=10).
    // The compactor should paginate through batches where processableDocs
    // is always empty, terminating cleanly without touching any data.
    const docs: BucketDataDocumentV3[] = [];
    for (let i = 0; i < 5; i++) {
      docs.push(serializeBucketData(BUCKET, [makeOp((i + 1) * 100, `row${i}`, `data${i}`, ctx, sourceTableId)]));
    }
    await insertDocs(collection, docs);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 500n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 2,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 10n
    });

    // All ops are > maxOpId, so processableDocs is always empty.
    // The compactor should paginate through all documents without
    // deleting or modifying anything.
    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    // All 5 docs should still exist with their PUT ops intact
    expect(docsAfter.length).toBe(5);
    const allOps = await readAllOps(collection);
    expect(allOps.length).toBe(5);
    expect(allOps.every((op) => op.op === 'PUT')).toBe(true);
  });

  test('9. maxOpId filtering leaves a straddling document untouched', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // Single document with two ops: one <= maxOpId, one > maxOpId
    const doc = serializeBucketData(BUCKET, [
      makeOp(200, 'A', 'old_A', ctx, sourceTableId),
      makeOp(400, 'B', 'new_B', ctx, sourceTableId)
    ]);
    await insertDocs(collection, [doc]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 400n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 300n
    });

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    const allOps = await readAllOps(collection);
    expect(allOps.length).toBe(2);

    const op400 = allOps.find((op) => op.o === 400n);
    expect(op400).toBeDefined();
    expect(op400!.op).toBe('PUT');
    expect(op400!.row_id).toBe('B');
  });

  test('10. compacted documents have non-overlapping ranges', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // Four documents with truly disjoint [min_op, _id.o] ranges.
    // maxOpId=350: only doc1 is processable; doc2 straddles the horizon.
    // Compaction must preserve range disjointness after rechunking.
    const doc1 = serializeBucketData(BUCKET, [
      makeOp(100, 'A', 'a1', ctx, sourceTableId),
      makeOp(200, 'B', 'b1', ctx, sourceTableId)
    ]); // range [100, 200]
    const doc2 = serializeBucketData(BUCKET, [
      makeOp(300, 'C', 'c1', ctx, sourceTableId),
      makeOp(400, 'D', 'd1', ctx, sourceTableId)
    ]); // range [300, 400], mixed (300≤350, 400>350)
    const doc3 = serializeBucketData(BUCKET, [
      makeOp(500, 'E', 'e1', ctx, sourceTableId),
      makeOp(600, 'F', 'f1', ctx, sourceTableId)
    ]); // range [500, 600], all >350
    const doc4 = serializeBucketData(BUCKET, [
      makeOp(700, 'G', 'g1', ctx, sourceTableId),
      makeOp(800, 'H', 'h1', ctx, sourceTableId)
    ]); // range [700, 800], all >350

    await insertDocs(collection, [doc1, doc2, doc3, doc4]);
    await insertBucketState(bucketStateCollection, ctx.definitionId, 800n);

    const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
    const checksumBefore = docsBefore.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 350n
    });

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();

    // Every pair of documents must have disjoint [min_op, _id.o] ranges.
    const overlaps = docsAfter.flatMap((a, i) =>
      docsAfter
        .slice(i + 1)
        .filter((b) => a._id.o >= b.min_op && b._id.o >= a.min_op)
        .map((b) => `[${a.min_op}, ${a._id.o}] vs [${b.min_op}, ${b._id.o}]`)
    );
    expect(overlaps).toEqual([]);

    // Checksum must be preserved across compaction
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);
  });

  describe('V3 CLEAR pass', () => {
    async function doCompact(bucketStorage: MongoSyncBucketStorage, maxOpId: bigint) {
      await bucketStorage.compact({
        clearBatchLimit: 200,
        moveBatchLimit: 10,
        moveBatchQueryLimit: 10,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId
      });
    }

    // Helper to find a CLEAR op in a bucket
    async function hasClearOp(collection: any): Promise<boolean> {
      const allOps = await readAllOps(collection);
      return allOps.some((op) => op.op === 'CLEAR');
    }

    // Helper to assert checksum preservation before/after compact
    async function assertChecksumPreserved(collection: any, compactFn: () => Promise<void>) {
      const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
      const checksumBefore = docsBefore.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);
      await compactFn();
      const docsAfter = await collection.find({ '_id.b': BUCKET }).toArray();
      const checksumAfter = docsAfter.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);
      expect(checksumAfter).toBe(checksumBefore);
    }

    test('duplicate rows produce CLEAR op', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      const doc = serializeBucketData(BUCKET, [
        makeOp(10, 'A', 'a1', ctx, sourceTableId),
        makeOp(20, 'A', 'a2', ctx, sourceTableId),
        makeOp(30, 'A', 'a3', ctx, sourceTableId)
      ]);
      await insertDocs(collection, [doc]);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

      await doCompact(bucketStorage, 30n);

      expect(await hasClearOp(collection)).toBe(true);
    });

    test('unique rows produce no CLEAR op', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      const doc = serializeBucketData(BUCKET, [
        makeOp(10, 'A', 'a1', ctx, sourceTableId),
        makeOp(20, 'B', 'b1', ctx, sourceTableId)
      ]);
      await insertDocs(collection, [doc]);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 20n);

      await doCompact(bucketStorage, 20n);

      expect(await hasClearOp(collection)).toBe(false);
      const allOps = await readAllOps(collection);
      expect(allOps.length).toBe(2);
      expect(allOps.every((op) => op.op === 'PUT')).toBe(true);
    });

    test('end-to-end: duplicate + unique rows, checksum preserved', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      const doc = serializeBucketData(BUCKET, [
        makeOp(10, 'A', 'a1', ctx, sourceTableId),
        makeOp(20, 'A', 'a2', ctx, sourceTableId),
        makeOp(30, 'B', 'b1', ctx, sourceTableId),
        makeOp(40, 'A', 'a3', ctx, sourceTableId)
      ]);
      await insertDocs(collection, [doc]);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

      await assertChecksumPreserved(collection, () => doCompact(bucketStorage, 40n));

      expect(await hasClearOp(collection)).toBe(true);
      const allOps = await readAllOps(collection);
      const putOps = allOps.filter((op) => op.op === 'PUT');
      expect(putOps.length).toBe(2);
      expect(putOps.some((op) => op.row_id === 'B' && op.o === 30n)).toBe(true);
      expect(putOps.some((op) => op.row_id === 'A' && op.o === 40n)).toBe(true);
    });

    // 15 MOVE ops, each in its own document. moveBatchQueryLimit=1
    // forces the MOVE pass to produce 15 separate rechunked documents
    // (one per batch). clearBatchLimit=5 forces the CLEAR read loop
    // to paginate through 3 iterations.
    test('read pagination iterates across multiple read batches', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      const docs: BucketDataDocumentV3[] = [];
      for (let i = 1; i <= 15; i++) {
        const op = { ...makeOp(i, `row_${i}`, '', ctx, sourceTableId), op: 'MOVE' as const, data: null };
        docs.push(serializeBucketData(BUCKET, [op]));
      }
      await insertDocs(collection, docs);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 15n);

      const docsBefore = await collection.find({ '_id.b': BUCKET }).toArray();
      const checksumBefore = docsBefore.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);

      await bucketStorage.compact({
        clearBatchLimit: 5,
        moveBatchLimit: 1,
        moveBatchQueryLimit: 1,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId: 15n
      });

      const docsAfter = await collection.find({ '_id.b': BUCKET }).toArray();
      const checksumAfter = docsAfter.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);
      expect(checksumAfter).toBe(checksumBefore);

      expect(await hasClearOp(collection)).toBe(true);
      const allOps = await readAllOps(collection);
      expect(allOps.length).toBe(1);
      expect(allOps[0].op).toBe('CLEAR');
    });

    test('CLEAR document carries target_op from collapsed MOVE tombstones', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      // Three versions of row A: ops 10, 20, 30.
      // MOVE pass: 30 survives, 20 and 10 become MOVE tombstones (target=30).
      // CLEAR pass: collapses both MOVEs. The CLEAR doc should carry target_op=30.
      const doc = serializeBucketData(BUCKET, [
        makeOp(10, 'A', 'a1', ctx, sourceTableId),
        makeOp(20, 'A', 'a2', ctx, sourceTableId),
        makeOp(30, 'A', 'a3', ctx, sourceTableId)
      ]);
      await insertDocs(collection, [doc]);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 30n);

      await doCompact(bucketStorage, 30n);

      const docsAfter = await collection.find({ '_id.b': BUCKET }).toArray();
      const clearDoc = docsAfter.find((d: any) => d.ops!.some((op: any) => op.op === 'CLEAR'));
      expect(clearDoc).toBeDefined();
      expect(clearDoc!.target_op).toBe(30n);
    });

    test('straddling document is excluded from compacted state', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      // Row A at 10, 30, 50 — ops 10+30 ≤ maxOpId=40, op 50 > 40 (pass-through)
      // Row B at 20 — ≤ 40, first occurrence
      // Row C at 60 — > 40 (pass-through, no earlier version)
      const doc = serializeBucketData(BUCKET, [
        makeOp(10, 'A', 'a1', ctx, sourceTableId),
        makeOp(20, 'B', 'b1', ctx, sourceTableId),
        makeOp(30, 'A', 'a2', ctx, sourceTableId),
        makeOp(50, 'A', 'a3', ctx, sourceTableId),
        makeOp(60, 'C', 'c1', ctx, sourceTableId)
      ]);
      await insertDocs(collection, [doc]);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 60n);

      await doCompact(bucketStorage, 40n);

      const state = await bucketStateCollection.findOne({
        _id: { d: ctx.definitionId, b: BUCKET }
      });
      expect(state).toBeDefined();
      expect(state!.compacted_state).toBeUndefined();

      // The document ends beyond maxOpId, so it is not read or modified.
      const allOps = await readAllOps(collection);
      expect(allOps.length).toBe(5);
      expect(allOps.every((op) => op.op == 'PUT')).toBe(true);
    });

    test('upperBound pagination prevents re-reading replacement documents', async () => {
      const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

      // Two distinct-row PUTs at ~600KB each. Rechunking produces two
      // documents: C1(_id.o=10) and C2(_id.o=30). The fix sets upperBound
      // from the oldest replacement C1, so _id < { o: 10 } excludes both.
      // No re-read. Both PUTs survive with intact data and valid row_ids.
      const largeData = 'x'.repeat(600_000);
      const doc = serializeBucketData(BUCKET, [
        makeOp(10, 'A', largeData, ctx, sourceTableId),
        makeOp(30, 'B', largeData, ctx, sourceTableId)
      ]);
      await insertDocs(collection, [doc]);
      await insertBucketState(bucketStateCollection, ctx.definitionId, 40n);

      await bucketStorage.compact({
        clearBatchLimit: 200,
        moveBatchLimit: 10,
        moveBatchQueryLimit: 1,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId: 40n
      });

      const docsAfter = await collection.find({ '_id.b': BUCKET }).toArray();
      const rawOps = docsAfter.flatMap((d: any) => d.ops) as any[];

      // Row A@10 must survive as PUT with data intact
      const rowA = rawOps.find((op: any) => op.row_id === 'A' && op.o === 10n);
      expect(rowA).toBeDefined();
      expect(rowA!.op).toBe('PUT');
      expect(rowA!.data).not.toBeNull();

      // Row B@30 must survive as PUT with data intact
      const rowB = rawOps.find((op: any) => op.row_id === 'B' && op.o === 30n);
      expect(rowB).toBeDefined();
      expect(rowB!.op).toBe('PUT');
      expect(rowB!.data).not.toBeNull();

      // Checksum preserved
      const expectedChecksum = [10, 30].reduce((sum, id) => addChecksums(sum, id * 7), 0);
      const checksumAfter = docsAfter.reduce((s: number, d: any) => addChecksums(s, Number(d.checksum)), 0);
      expect(checksumAfter).toBe(expectedChecksum);
    });
  });
});
