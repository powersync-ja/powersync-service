import {
  chunkBucketData,
  DEFAULT_MAX_DOC_SIZE_BYTES
} from '@module/storage/implementation/bucket-operations/chunking.js';
import { VersionedPowerSyncMongo } from '@module/storage/implementation/collection-access/versioned-collections.js';
import { BucketDataDoc } from '@module/storage/implementation/common/BucketDataDoc.js';
import { BucketStateDocument } from '@module/storage/implementation/common/models.js';
import { AbstractMongoSyncBucketStorage } from '@module/storage/implementation/createMongoSyncBucketStorage.js';
import {
  BucketDataDocument,
  loadBucketDataDocument,
  serializeBucketData
} from '@module/storage/implementation/document-formats/bucket-document-format.js';
import { addChecksums, storage, SyncRulesBucketStorage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

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

    const setup = async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
bucket_definitions:
  by_user:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const { checkpoint } = await populate(bucketStorage, 1);

      return { bucketStorage, checkpoint, factory, syncRules };
    };

    test('full compact', async () => {
      const { bucketStorage, checkpoint, factory, syncRules } = await setup();
      const storageDb = bucketStorage.db;

      // Simulate bucket_state from old version not being available
      if (storageDb.storageConfig.incrementalReprocessing) {
        // This should actually never happen on V3, but we test this anyway.
        // Can remove this if it causes issues in the future.
        await (storageDb as VersionedPowerSyncMongo).bucketState(bucketStorage.group_id).deleteMany({});
      } else {
        await factory.db.bucket_state.deleteMany({});
      }

      await bucketStorage.compact({
        clearBatchLimit: 200,
        moveBatchLimit: 10,
        moveBatchQueryLimit: 10,
        minBucketChanges: 1,
        minChangeRatio: 0,
        maxOpId: checkpoint,
        signal: null as any
      });

      const users = ['u1', 'u2'];
      const userRequests = users.map((user) => bucketRequest(syncRules, `by_user["${user}"]`));
      const [u1Request, u2Request] = userRequests;
      const checksumAfter = await bucketStorage.getChecksums(checkpoint, userRequests);
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

    test('populatePersistentChecksumCache', async () => {
      // Populate old replication stream
      const { factory } = await setup();

      // Now populate another replication stream (bucket definition name changed)
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
bucket_definitions:
  by_user2:
    parameters: select request.user_id() as user_id
    data: [select * from test where owner_id = bucket.user_id]
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const storageDb = (bucketStorage as any).db;

      await populate(bucketStorage, 2);
      const { checkpoint } = await bucketStorage.getCheckpoint();

      // Default is to small small numbers - should be a no-op
      const result0 = await bucketStorage.populatePersistentChecksumCache({
        maxOpId: checkpoint
      });
      expect(result0.buckets).toEqual(0);

      // This should cache the checksums for the two buckets
      const result1 = await bucketStorage.populatePersistentChecksumCache({
        maxOpId: checkpoint,
        minBucketChanges: 1
      });
      expect(result1.buckets).toEqual(2);

      // This should be a no-op, as the checksums are already cached
      const result2 = await bucketStorage.populatePersistentChecksumCache({
        maxOpId: checkpoint,
        minBucketChanges: 1
      });
      expect(result2.buckets).toEqual(0);

      const users = ['u1', 'u2'];
      const userRequests = users.map((user) => bucketRequest(syncRules, `by_user2["${user}"]`));
      const [u1Request, u2Request] = userRequests;
      const checksumAfter = await bucketStorage.getChecksums(checkpoint, userRequests);
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

    test('dirty bucket discovery handles bigint bucket_state bytes', async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(`
bucket_definitions:
  global:
    data: [select * from test]
    `)
      );
      const bucketStorage = factory.getInstance(syncRules);
      const storageDb = bucketStorage.db;

      // This simulates bucket_state created using bigint bytes.
      // This typically happens when buckets get very large (> 2GiB). We don't want to create that much
      // data in the tests, so we directly insert the bucket_state here.
      if (storageDb.storageConfig.incrementalReprocessing) {
        const bucketStateCollection = (storageDb as VersionedPowerSyncMongo).bucketState<
          import('@module/storage/implementation/common/models.js').BucketStateDocument
        >(bucketStorage.group_id);
        await bucketStateCollection.insertOne({
          _id: {
            d: '1',
            b: 'global[]'
          },
          last_op: 5n,
          compacted_state: {
            op_id: 3n,
            count: 3,
            checksum: 0n,
            bytes: 7n
          },
          estimate_since_compact: {
            count: 2,
            bytes: 5n
          }
        });
      } else {
        await factory.db.bucket_state.insertOne({
          _id: {
            g: bucketStorage.group_id,
            b: 'global[]'
          },
          last_op: 5n,
          compacted_state: {
            op_id: 3n,
            count: 3,
            checksum: 0n,
            bytes: 7n
          },
          estimate_since_compact: {
            count: 2,
            bytes: 5n
          }
        });
      }

      // This test uses a couple of "internal" APIs of the compactor.
      const compactor = bucketStorage.createMongoCompactor({ maxOpId: 5n });

      const dirtyBuckets = compactor.dirtyBucketBatches({
        minBucketChanges: 1,
        minChangeRatio: 0.39
      });
      const firstBatch = await dirtyBuckets.next();

      expect(firstBatch.done).toBe(false);
      expect(firstBatch.value).toHaveLength(1);
      expect(firstBatch.value[0].bucket).toBe('global[]');
      expect(firstBatch.value[0].estimatedCount).toBe(5);
      expect(typeof firstBatch.value[0].estimatedCount).toBe('number');
      expect(firstBatch.value[0].dirtyRatio).toBeCloseTo(5 / 12);

      const checksumBuckets = await (compactor as any).dirtyBucketBatchForChecksums({
        minBucketChanges: 1
      });
      expect(checksumBuckets).toEqual([
        {
          bucket: 'global[]',
          definitionId: storageDb.storageConfig.incrementalReprocessing ? '1' : null,
          estimatedCount: 5
        }
      ]);
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
      target_op: null,
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
    const bucketStorage = factory.getInstance(syncRules) as AbstractMongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongo;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData<BucketDataDocument>(bucketStorage.group_id, definitionId);
    const bucketStateCollection = db.bucketState<BucketStateDocument>(bucketStorage.group_id);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.group_id,
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
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data }),
      target_op: null
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocument[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      estimate_since_compact: { count: 10, bytes: 100 }
    });
  }

  async function compact(bucketStorage: AbstractMongoSyncBucketStorage, maxOpId: bigint) {
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId,
      signal: null as any
    });
  }

  test('1. ops[] ordering - preserves caller ordering (no implicit sort)', () => {
    const ops = [
      makeBucketDataDoc({ o: 5n, data: '{"id":"c"}' }),
      makeBucketDataDoc({ o: 3n, data: '{"id":"a"}' }),
      makeBucketDataDoc({ o: 7n, data: '{"id":"b"}' })
    ];

    const doc = serializeBucketData('test[]', ops);

    expect(doc.ops.map((op) => op.o)).toEqual([5n, 3n, 7n]);
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
      for (let i = 1; i < doc.ops.length; i++) {
        expect(doc.ops[i].o).toBeGreaterThanOrEqual(doc.ops[i - 1].o);
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
    expect(doc.size).toBe(4 + 5 + 8);
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
      expect(doc._id.o).toBe(doc.ops.reduce((max, op) => (op.o > max ? op.o : max), 0n));
      expect(doc.min_op).toBe(doc.ops.reduce((min, op) => (op.o < min ? op.o : min), doc.ops[0].o));
      expect(doc.count).toBe(doc.ops.length);
      expect(doc.checksum).toBe(doc.ops.reduce((sum, op) => sum + op.checksum, 0n));
      expect(doc.size).toBe(doc.ops.reduce((sum, op) => sum + (op.data?.length ?? 0), 0));
    }
  });

  test('3. target_op correctness - max of non-null target_ops', () => {
    const ops = [
      makeBucketDataDoc({ o: 1n, target_op: null }),
      makeBucketDataDoc({ o: 2n, target_op: 10n }),
      makeBucketDataDoc({ o: 3n, target_op: 5n }),
      makeBucketDataDoc({ o: 4n, target_op: null })
    ];

    const doc = serializeBucketData('test[]', ops);
    expect(doc.target_op).toBe(10n);
  });

  test('3. target_op correctness - all null yields null', () => {
    const ops = [makeBucketDataDoc({ o: 1n, target_op: null }), makeBucketDataDoc({ o: 2n, target_op: null })];

    const doc = serializeBucketData('test[]', ops);
    expect(doc.target_op).toBeNull();
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
    const allOps = docs.flatMap((d) => d.ops);
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
    const allOps = docs.flatMap((d) => d.ops);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(2);
    expect(moveOps.map((op) => op.o).sort()).toEqual([10n, 20n]);
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
    const allOps = docs.flatMap((d) => d.ops);
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
    const allOps = docs.flatMap((d) => d.ops);
    const putOps = allOps.filter((op) => op.op === 'PUT');
    expect(putOps.length).toBe(0);
    const removeOps = allOps.filter((op) => op.op === 'REMOVE');
    expect(removeOps.length).toBe(1);
    expect(removeOps[0].o).toBe(20n);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(1);
    expect(moveOps[0].o).toBe(10n);
  });

  test('7. empty bucket compact is a no-op', async () => {
    const { bucketStorage, collection } = await setupV3Storage();

    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId: 1n,
      signal: null as any
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
    expect(doc.ops[0].data).toBeNull();

    const context = { replicationStreamId: 1, definitionId: '1' };
    const deserialized = [...loadBucketDataDocument(context, doc)];
    expect(deserialized[0].data).toBeNull();
  });

  test('9. serialization fidelity - empty string data preserved', () => {
    const ops = [makeBucketDataDoc({ o: 1n, data: '' })];
    const doc = serializeBucketData('test[]', ops);
    expect(doc.ops[0].data).toBe('');

    const context = { replicationStreamId: 1, definitionId: '1' };
    const deserialized = [...loadBucketDataDocument(context, doc)];
    expect(deserialized[0].data).toBe('');
  });

  test('9. serialization fidelity - unicode characters preserved', () => {
    const unicodeData = '{"name":"日本語テスト","emoji":"🎉"}';
    const ops = [makeBucketDataDoc({ o: 1n, data: unicodeData })];
    const doc = serializeBucketData('test[]', ops);
    expect(doc.ops[0].data).toBe(unicodeData);

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
      const maxO = doc.ops.reduce((max, op) => (op.o > max ? op.o : max), 0n);
      expect(doc._id.o).toBe(maxO);
    }
  });

  test('compaction with maxOpId filtering - ops above maxOpId excluded', async () => {
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
    const allOpsAfter = docsAfter.flatMap((d) => d.ops);
    for (const op of allOpsAfter) {
      expect(op.o).toBeLessThanOrEqual(15n);
    }
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
    const storedOps = docs.flatMap((d) => d.ops);
    let storedChecksum = 0;
    for (const op of storedOps) {
      storedChecksum = addChecksums(storedChecksum, Number(op.checksum));
    }

    expect(storedChecksum).toBe(jsChecksum);
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
    const bucketStorage = factory.getInstance(syncRules) as AbstractMongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongo;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData<BucketDataDocument>(bucketStorage.group_id, definitionId);
    const bucketStateCollection = db.bucketState<BucketStateDocument>(bucketStorage.group_id);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.group_id,
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
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data }),
      target_op: null
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocument[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      estimate_since_compact: { count: 10, bytes: 100 }
    });
  }

  async function compact(bucketStorage: AbstractMongoSyncBucketStorage, maxOpId: bigint) {
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId,
      signal: null as any
    });
  }

  async function readAllOps(collection: any): Promise<{ row_id: string; o: bigint; op: string }[]> {
    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    return docs.flatMap((d: any) =>
      d.ops.map((op: any) => ({ row_id: op.row_id!, o: op.o, op: op.op, target_op: op.target_op ?? undefined }))
    );
  }

  async function readAllDocs(collection: any): Promise<BucketDataDocument[]> {
    return collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
  }

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
    expect(surviving).toHaveLength(3);
    expect(surviving[0]).toMatchObject({ op: 'MOVE', o: 10n });
    expect(surviving[1]).toMatchObject({ op: 'MOVE', o: 20n });
    expect(surviving[2]).toMatchObject({ row_id: 'A', o: 30n, op: 'REMOVE' });
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
    const bucketStorage = factory.getInstance(syncRules) as AbstractMongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongo;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData<BucketDataDocument>(bucketStorage.group_id, definitionId);
    const bucketStateCollection = db.bucketState<BucketStateDocument>(bucketStorage.group_id);
    const sourceTableId = new bson.ObjectId();

    const ctx = {
      replicationStreamId: bucketStorage.group_id,
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
      data: overrides?.op === 'REMOVE' ? null : JSON.stringify({ id: rowId, description: data }),
      target_op: null
    };
  }

  async function insertDocs(collection: any, docs: BucketDataDocument[]) {
    await collection.insertMany(docs);
  }

  async function insertBucketState(bucketStateCollection: any, definitionId: string, lastOp: bigint) {
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: lastOp,
      estimate_since_compact: { count: 10, bytes: 100 }
    });
  }

  async function compact(bucketStorage: AbstractMongoSyncBucketStorage, maxOpId: bigint) {
    await bucketStorage.compact({
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      maxOpId,
      signal: null as any
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

    const allOpsAfter = docsAfter.flatMap((d) => d.ops);
    expect(allOpsAfter.length).toBe(5);
    const moveOps = allOpsAfter.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(2);
    expect(moveOps.map((op) => op.o).sort()).toEqual([10n, 20n]);
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

    const allOpsAfter = docsAfter.flatMap((d) => d.ops);
    expect(allOpsAfter.length).toBe(6);
    const moveOps = allOpsAfter.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(2);
    expect(moveOps.map((op) => op.o).sort()).toEqual([10n, 20n]);
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
    const allOps = docs.flatMap((d) => d.ops);
    const moveOps = allOps.filter((op) => op.op === 'MOVE');
    expect(moveOps.length).toBe(1);
    expect(moveOps[0].o).toBe(10n);
    expect(moveOps[0].data).toBeNull();

    const putOps = allOps.filter((op) => op.op === 'PUT');
    expect(putOps.length).toBe(2);
    expect(putOps.every((op) => op.data != null)).toBe(true);

    expect(docs.length).toBe(1);
    const putSize = putOps.reduce((sum, op) => sum + (op.data?.length ?? 0), 0);
    expect(docs[0].size).toBe(putSize);
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

    const allOps = docs.flatMap((d) => d.ops);
    expect(allOps.length).toBe(5);
    const moveOp = allOps.find((op) => op.op === 'MOVE');
    expect(moveOp).toMatchObject({ o: 10n, data: null });

    expect(docs.length).toBe(1);

    const moveOpsInDoc = docs[0].ops.filter((op) => op.op === 'MOVE');
    const putOpsInDoc = docs[0].ops.filter((op) => op.op === 'PUT');
    expect(moveOpsInDoc.length).toBe(1);
    expect(putOpsInDoc.length).toBe(4);
  });
});
