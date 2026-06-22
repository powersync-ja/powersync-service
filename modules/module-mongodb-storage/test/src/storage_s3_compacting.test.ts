import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { createS3TestStorageSuite } from './helpers/s3TestFactory.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

function s3Factory() {
  const { objectStorage, factoryGen } = createS3TestStorageSuite({ url: env.MONGO_TEST_URL, isCI: env.CI });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

describe('S3 compaction (Phase 2d red tests)', () => {
  test('Compaction round-trip with S3-backed docs', async () => {
    const { memoryStorage, factory: factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    // Write ops with duplicates to exercise dedup during compaction.
    // A@2 is superseded by A@6, becoming a MOVE tombstone.
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Op 1: A@first
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'first version' },
      afterReplicaId: test_utils.rid('A-v1')
    });

    // Op 2: B
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'second item' },
      afterReplicaId: test_utils.rid('B')
    });

    // Op 3: A@updated (supersedes Op 1)
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'updated version' },
      afterReplicaId: test_utils.rid('A-v2')
    });

    // Op 4: C
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'C', description: 'third item' },
      afterReplicaId: test_utils.rid('C')
    });

    await writer.commit('1/1');

    // Verify S3 objects were created (write path works).
    const storedPaths = (memoryStorage as any).store as Map<string, Buffer>;
    expect(storedPaths.size).toBeGreaterThan(0);

    // Verify MongoDB documents have storage_ref (no ops[]).
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    const groupId = bucketStorage.replicationStreamId;

    const docsBefore = await collection.find({}).toArray();
    expect(docsBefore.length).toBeGreaterThan(0);
    for (const doc of docsBefore) {
      expect(doc.storage_ref).toBeDefined();
      expect(doc.ops).toBeUndefined();
    }

    // Get checkpoint and the bucket name.
    const { checkpoint } = await bucketStorage.getCheckpoint();
    expect(checkpoint).toBeGreaterThan(0n);

    const request = bucketRequest(syncRules as any, 'global[]', 0n);
    const bucket = request.bucket;

    // Read bucket_state before compaction to confirm it exists and has
    // estimate_since_compact populated by the writer.
    const bucketStateBefore = await bucketStateCollection.findOne({
      _id: { d: definitionId, b: bucket }
    });
    expect(bucketStateBefore).toBeDefined();
    expect(bucketStateBefore!.estimate_since_compact).toBeDefined();
    expect(bucketStateBefore!.estimate_since_compact!.count).toBeGreaterThan(0);

    // Record the ops that exist so we can verify survival later.
    // Use the Phase 2c read path to read actual ops (this path works).
    const batchBefore = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = test_utils.getBatchData(batchBefore);
    expect(dataBefore.length).toBe(4);

    // Compact the bucket.
    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    // --- Verification 1: bucket_state compacted_state is wrong ---
    // The compactor writes compacted_state with checksum=0 and count=0
    // because it cannot read ops from storage_ref docs. The correct output
    // should have non-zero checksum and count >= 3 (1 MOVE tombstone + 3 PUTs,
    // with potential consolidation).
    const bucketStateAfter = await bucketStateCollection.findOne({
      _id: { d: definitionId, b: bucket }
    });
    expect(bucketStateAfter).toBeDefined();
    expect(bucketStateAfter!.compacted_state).toBeDefined();

    // FAILS: checksum is 0 (should be non-zero sum of surviving op checksums)
    expect(bucketStateAfter!.compacted_state!.checksum).not.toBe(0n);

    // FAILS: count is 0 (should be >= 3 for 1 MOVE + 2 PUT, or 3 PUT if
    // the MOVE was consolidated into CLEAR)
    const compactedCount = bucketStateAfter!.compacted_state!.count;
    expect(compactedCount).toBeGreaterThanOrEqual(3);

    // The compacted_state.op_id must equal the maxOpId (checkpoint)
    expect(bucketStateAfter!.compacted_state!.op_id).toBe(checkpoint);

    // --- Verification 2: MongoDB docs replaced with new metadata shells ---
    const docsAfter = await collection.find({}).toArray();
    for (const doc of docsAfter) {
      expect(doc.storage_ref).toBeDefined();
      expect(doc.ops).toBeUndefined();
    }

    // --- Verification 3: S3 objects cleaned/replaced ---
    // Old writer-generated paths should be gone from storage UNLESS they
    // collided with a new compaction path (e.g. same minOp/maxOp after
    // dedup). In that case the path was reused and still exists.
    const oldS3Paths = new Set(docsBefore.map((d: any) => d.storage_ref?.path).filter(Boolean));
    const afterS3Paths = new Set(docsAfter.map((d: any) => d.storage_ref?.path).filter(Boolean));
    for (const path of oldS3Paths) {
      if (!afterS3Paths.has(path)) {
        expect(storedPaths.has(path)).toBe(false);
      }
    }
    for (const path of afterS3Paths) {
      expect(storedPaths.has(path)).toBe(true);
    }
    expect(afterS3Paths.size).toBeGreaterThan(0);

    // --- Verification 4: Read path still returns correct ops ---
    // Phase 2c reads ops from S3 correctly. Since compaction didn't delete
    // the S3 objects or modify the docs, the ops are still readable.
    // This assertion should PASS today and must continue passing after
    // Phase 2d implementation.
    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.length).toBe(dataBefore.length);
    expect(dataAfter).toEqual(
      expect.arrayContaining(dataBefore.map((d: any) => expect.objectContaining({ op: d.op, object_id: d.object_id })))
    );
  });
});
