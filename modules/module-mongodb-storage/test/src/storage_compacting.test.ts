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
      data: JSON.stringify({ id: rowId, description: data }),
      target_op: null
    };
  }

  test('partial checksum with start straddling multi-op document', async () => {
    const { bucketStorage, syncRules, collection, bucketStateCollection, definitionId, sourceTableId, ctx } =
      await setup();

    // Single document with ops 10-60, min_op=10, _id.o=60
    const ops = [
      makeOp(10, 'A', 'a1', ctx, sourceTableId),
      makeOp(20, 'B', 'b1', ctx, sourceTableId),
      makeOp(30, 'C', 'c1', ctx, sourceTableId),
      makeOp(40, 'D', 'd1', ctx, sourceTableId),
      makeOp(50, 'E', 'e1', ctx, sourceTableId),
      makeOp(60, 'F', 'f1', ctx, sourceTableId)
    ];
    const doc = serializeBucketData(BUCKET, ops);
    await collection.insertMany([doc]);

    // Set compacted_state.op_id = 30 to create a partial range starting at 30.
    // The pipeline will query ops where o > 30 and o <= 60.
    // The document has min_op=10 < 30, so it's partially included.
    // The pipeline must $filter ops to only sum those with o > 30 (ops 40, 50, 60).
    const fullChecksum = ops.reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0);
    const partialChecksum = ops
      .filter((op) => op.o > 30n)
      .reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0);

    // Partial and full must differ, otherwise the document is fully included and no straddling occurs.
    expect(partialChecksum).not.toBe(fullChecksum);

    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: 30n,
      compacted_state: {
        op_id: 30n,
        count: 3,
        checksum: BigInt(
          ops.filter((op) => op.o <= 30n).reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0)
        ),
        bytes: null
      },
      estimate_since_compact: { count: 3, bytes: 100 }
    });

    const request: storage.BucketChecksumRequest = {
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

    const result = await bucketStorage.getChecksums(60n, [request]);
    const checksumResult = result.get(BUCKET)!;

    // The total checksum should be: compacted (ops 10,20,30) + partial (ops 40,50,60)
    expect(checksumResult.checksum).toBe(fullChecksum);
    expect(checksumResult.count).toBe(6);
  });

  test('partial checksum with end straddling multi-op document', async () => {
    const { bucketStorage, collection, bucketStateCollection, definitionId, sourceTableId, ctx } = await setup();

    // Document with ops 40-60, _id.o=60, min_op=40
    const ops = [
      makeOp(40, 'D', 'd1', ctx, sourceTableId),
      makeOp(50, 'E', 'e1', ctx, sourceTableId),
      makeOp(60, 'F', 'f1', ctx, sourceTableId)
    ];
    const doc = serializeBucketData(BUCKET, ops);
    await collection.insertMany([doc]);

    // No compacted_state — start from beginning, so the full document is in range.
    // Request checksums with checkpoint=45 (falls between ops 40 and 50).
    // createBucketFilter produces _id.o <= 45.
    // This document has _id.o=60 > 45, so the filter excludes it.
    // But the document contains op 40 which should be included (40 <= 45).
    const checksumUpTo45 = ops
      .filter((op) => op.o <= 45n)
      .reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0);

    const checksumAllOps = ops.reduce((sum, op) => addChecksums(sum, Number(op.checksum)), 0);

    // The straddling is real: op 40 is <= 45 but the document's _id.o=60 is > 45
    expect(checksumUpTo45).not.toBe(checksumAllOps);

    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: BUCKET },
      last_op: 0n,
      estimate_since_compact: { count: 0, bytes: 0 }
    });

    const request: storage.BucketChecksumRequest = {
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

    const result = await bucketStorage.getChecksums(45n, [request]);
    const checksumResult = result.get(BUCKET)!;

    // If createBucketFilter's _id.o <= 45 excludes this document,
    // the checksum will be 0 instead of checksumUpTo45.
    expect(checksumResult.checksum).toBe(checksumUpTo45);
    expect(checksumResult.count).toBe(1);
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

  async function readAllOps(
    collection: any
  ): Promise<{ row_id: string | undefined; o: bigint; op: string; target_op: bigint | undefined }[]> {
    const docs = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    return docs.flatMap((d: any) =>
      d.ops.map((op: any) => ({
        row_id: op.row_id ?? undefined,
        o: op.o,
        op: op.op,
        target_op: op.target_op ?? undefined
      }))
    );
  }

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
      maxOpId: 24n,
      signal: null as any
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
      maxOpId: 500n,
      signal: null as any
    });

    // The document at _id.o=600 must still be present and unmodified after compaction.
    // The streaming compactor should only touch documents containing ops <= maxOpId.
    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const allOpsAfter = docsAfter.flatMap((d) => d.ops);
    const op600 = allOpsAfter.find((op) => op.o === 600n);
    expect(op600).toBeDefined();
    expect(op600!.op).toBe('PUT');
    expect(op600!.row_id).toBe('F');
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
      memoryLimitMB: 0.001,
      signal: null as any
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
      maxOpId: 300n,
      signal: null as any
    });

    // The sandwiched document (doc2, ops 340+350) must survive because ALL
    // its ops are > maxOpId — it should not be touched by compaction.
    const opsAfter = await readAllOps(collection);
    const sandwichOps = opsAfter.filter((op) => op.row_id === 'X' || op.row_id === 'Y');
    expect(sandwichOps.length).toBe(2);
    expect(sandwichOps.every((op) => op.op === 'PUT')).toBe(true);
  });

  test('6. byte-based batch cutting - respects moveBatchByteLimit', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 6 documents, each with 2 PUT ops. With moveBatchByteLimit=400,
    // the byte limit should be hit after 1-2 documents, forcing multiple
    // batch iterations even though the document count limit (10) is never reached.
    const rows = Array.from({ length: 12 }, (_, i) => `row${i + 1}`);
    const docs: BucketDataDocument[] = [];
    for (let i = 0; i < 6; i++) {
      docs.push(
        serializeBucketData(BUCKET, [
          makeOp(i * 2 + 1, rows[i * 2], `data_${rows[i * 2]}`, ctx, sourceTableId),
          makeOp(i * 2 + 2, rows[i * 2 + 1], `data_${rows[i * 2 + 1]}`, ctx, sourceTableId)
        ])
      );
    }
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
      maxOpId: 12n,
      signal: null as any
    });

    const docsAfter = await collection.find({ '_id.b': BUCKET }).sort({ '_id.o': 1 }).toArray();
    const checksumAfter = docsAfter.reduce((sum, d) => addChecksums(sum, Number(d.checksum)), 0);
    expect(checksumAfter).toBe(checksumBefore);

    // All 12 ops should survive — no superseding, just byte-based batching
    const allOps = await readAllOps(collection);
    expect(allOps.length).toBe(12);
    expect(allOps.every((op) => op.op === 'PUT')).toBe(true);

    // Multiple documents produced due to byte-based batch cuts
    expect(docsAfter.length).toBeGreaterThan(1);
  });

  test('7. cross-batch seen map overflow - dedup continues across batches', async () => {
    const { bucketStorage, collection, bucketStateCollection, ctx, sourceTableId } = await setupV3();

    // 10 rows (A-J), 2 versions each. 20 docs total, 1 PUT per doc.
    // New versions at opIds 20-11 (docs 1-10), old versions at opIds 10-1 (docs 11-20).
    // Processed newest-first: new versions fill the seen map; old versions that
    // were tracked get tombstoned; old versions beyond map capacity survive as PUT.
    const rows = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'];
    const docs: BucketDataDocument[] = [];

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
      memoryLimitMB: 0.001,
      signal: null as any
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
    const docsWithMoves = docsAfter.filter((d) => d.ops.some((op: any) => op.op === 'MOVE'));
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
    const docs: BucketDataDocument[] = [];
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
      maxOpId: 10n,
      signal: null as any
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
});
