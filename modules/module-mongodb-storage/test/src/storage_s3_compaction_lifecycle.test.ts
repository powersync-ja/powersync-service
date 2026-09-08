import * as lib_mongo from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, compactActive, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { ObjectStorageError } from '../../src/storage/implementation/v3/object-storage/ObjectStorage.js';
import { env } from './env.js';
import { createMemoryS3TestStorageSuite, createS3TestStorageSuite } from './helpers/s3TestFactory.js';

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

function memoryS3Factory(options: { inlineThresholdBytes?: number } = {}) {
  const { objectStorage, factoryGen } = createMemoryS3TestStorageSuite({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    inlineThresholdBytes: options.inlineThresholdBytes ?? 0
  });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

async function expectUsageMatchesBucketData(bucketStorage: MongoSyncBucketStorage, definitionId: string) {
  const v3BucketStorage = bucketStorage as MongoSyncBucketStorageV3;
  const db = v3BucketStorage.db as VersionedPowerSyncMongoV3;
  const documents = await db.bucketData(v3BucketStorage.replicationStreamId, definitionId).find({}).toArray();
  const expectedBytes = documents.reduce((sum, document) => sum + BigInt(document.storage_ref?.file_size ?? 0), 0n);
  const entries = await db.objectStorageUsage
    .aggregate<{ active_bytes: bigint }>([
      { $match: { '_id.g': v3BucketStorage.replicationStreamId } },
      { $project: { definitions: { $objectToArray: '$definitions' } } },
      { $unwind: '$definitions' },
      { $group: { _id: null, active_bytes: { $sum: '$definitions.v' } } }
    ])
    .toArray()
    .catch((error) => {
      if (lib_mongo.isMongoNamespaceNotFoundError(error)) {
        return [];
      }
      throw error;
    });
  expect(BigInt(entries[0]?.active_bytes ?? 0)).toBe(expectedBytes);
}

describe('S3 compaction storage lifecycle', () => {
  test('a later compaction recovers from a transient object storage failure', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');
    for (const [index, description] of ['old', 'new'].entries()) {
      await writer.save({
        sourceTable,
        tag: index === 0 ? storage.SaveOperationTag.INSERT : storage.SaveOperationTag.UPDATE,
        after: { id: 'A', description },
        afterReplicaId: test_utils.rid('A')
      });
      await writer.commit(`${index + 1}/1`);
    }

    const originalGet = memoryStorage.get.bind(memoryStorage);
    let injectedFailure = false;
    memoryStorage.get = async (...args) => {
      if (!injectedFailure) {
        injectedFailure = true;
        throw new ObjectStorageError('temporary object storage failure', {
          cause: new Error('socket reset'),
          retryable: true
        });
      }
      return originalGet(...args);
    };

    const checkpoint = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const compactOptions = {
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [request.bucket],
      minBucketChanges: 1,
      minChangeRatio: 0
    };
    await expect(compactActive(factory, compactOptions)).rejects.toThrow('temporary object storage failure');
    await compactActive(factory, compactOptions);

    expect(injectedFailure).toBe(true);
    const batch = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const data = batch.flatMap((chunk) => chunk.chunkData.data);
    expect(data).toHaveLength(2);
  });

  test('small inline updates merge into an inline replacement', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory({ inlineThresholdBytes: 10_000 });
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 10);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'old' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('1/1');
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: 'new' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('2/1');

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    expect(await collection.countDocuments({ storage_ref: { $exists: true } })).toBe(0);

    const checkpoint = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    await compactActive(factory, {
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [request.bucket],
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const docs = await collection.find({}).toArray();
    expect(docs).toHaveLength(1);
    expect(docs[0].storage_ref).toBeUndefined();
    expect(docs[0].ops).toBeDefined();
    expect(memoryStorage.store.size).toBe(0);
    await expectUsageMatchesBucketData(bucketStorage, definitionId);
  });

  test('boundary CLEAR replacement updates active object-storage usage', async () => {
    const { factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 11);
    await writer.markAllSnapshotDone('1/1');
    for (const [index, description] of ['first', 'second', 'latest'].entries()) {
      await writer.save({
        sourceTable,
        tag: index === 0 ? storage.SaveOperationTag.INSERT : storage.SaveOperationTag.UPDATE,
        after: { id: 'A', description: description.repeat(100) },
        afterReplicaId: test_utils.rid('A')
      });
    }
    await writer.commit('1/1');

    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const checkpoint = await bucketStorage.getCheckpoint();
    await compactActive(factory, {
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [request.bucket],
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batch = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const data = batch.flatMap((chunk) => chunk.chunkData.data);
    expect(data.some((op: any) => op.op === 'CLEAR')).toBe(true);
    await expectUsageMatchesBucketData(bucketStorage, definitionId);
  });

  test('leading and boundary CLEAR replacements update active object-storage usage', async () => {
    const { factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 12);
    await writer.markAllSnapshotDone('1/1');

    // The first and last A versions force a leading MOVE document. The
    // middle document also contains a large surviving PUT, keeping it as the
    // CLEAR boundary document rather than merging it with the leading group.
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'old' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: 'middle' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'b'.repeat(990_000) },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.commit('2/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: 'latest'.repeat(10_000) },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('3/1');

    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const checkpoint = await bucketStorage.getCheckpoint();
    await compactActive(factory, {
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 2,
      moveBatchQueryLimit: 1,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const batch = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const data = batch.flatMap((chunk) => chunk.chunkData.data);
    expect(data.some((op: any) => op.op === 'CLEAR')).toBe(true);
    await expectUsageMatchesBucketData(bucketStorage, definitionId);
  });

  test('MOVE compaction merges adjacent objects using their compacted size in one write', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'old'.repeat(120_000) },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'middle'.repeat(60_000) },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.commit('2/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: 'new'.repeat(120_000) },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('3/1');

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docsBefore = await collection.find({}).sort({ _id: 1 }).toArray();
    expect(docsBefore).toHaveLength(3);

    const oldPaths = new Set(docsBefore.map((doc) => doc.storage_ref!.path));
    const objectStore = memoryStorage.store;
    expect(objectStore.size).toBe(3);

    const checkpoint = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    await compactActive(factory, {
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      // Force each input object into a different MongoDB query batch. The
      // pending merge group must survive across those batches.
      moveBatchQueryLimit: 1,
      moveBatchByteLimit: 16 * 1024 * 1024,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const docsAfter = await collection.find({}).toArray();
    expect(docsAfter).toHaveLength(1);
    expect(docsAfter[0]).toMatchObject({
      min_op: docsBefore[0].min_op,
      _id: { o: docsBefore[2]._id.o },
      count: 3
    });
    expect(oldPaths.has(docsAfter[0].storage_ref!.path)).toBe(false);

    // The three old objects remain during their grace period and exactly one
    // final replacement was uploaded. There was no intermediate MOVE-only
    // object before the merge.
    expect(objectStore.size).toBe(4);
    await expectUsageMatchesBucketData(bucketStorage, definitionId);
  });

  test('compaction only retires S3 objects at or below the maxOpId horizon', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Commit each operation separately to create objects on both sides of the
    // compaction horizon.
    for (let i = 1; i <= 12; i++) {
      const letter = String.fromCharCode(64 + i); // A..L
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: letter, description: `val${i}`.repeat(50) },
        afterReplicaId: test_utils.rid(letter)
      });
      await writer.commit(`${i}/1`);
    }

    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docsBefore = await collection.find({}).sort({ '_id.o': 1 }).toArray();
    expect(docsBefore).toHaveLength(12);

    const maxOpId = docsBefore[5]._id.o;
    const readCheckpoint = test_utils.testCheckpoint(docsBefore[docsBefore.length - 1]._id.o);
    const lowerPaths = new Set(docsBefore.slice(0, 6).map((doc) => doc.storage_ref!.path));
    const upperPaths = new Set(docsBefore.slice(6).map((doc) => doc.storage_ref!.path));

    await compactActive(factory, {
      maxOpId,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const docsAfter = await collection.find({}).sort({ '_id.o': 1 }).toArray();
    const referencedPathsAfter = new Set(docsAfter.map((doc) => doc.storage_ref!.path));

    // The lower documents are replaced and queued for delayed deletion.
    expect([...lowerPaths].some((path) => referencedPathsAfter.has(path))).toBe(false);
    const pendingDeletes = await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).find({}).toArray();
    const retiredPaths = new Set(pendingDeletes.map((marker) => marker.path));
    expect([...lowerPaths].every((path) => retiredPaths.has(path))).toBe(true);

    // Objects above the horizon remain referenced and must not be retired.
    expect([...upperPaths].every((path) => referencedPathsAfter.has(path))).toBe(true);
    expect([...upperPaths].some((path) => retiredPaths.has(path))).toBe(false);
    expect([...upperPaths].every((path) => memoryStorage.store.has(path))).toBe(true);

    // Retired objects remain readable during the reference grace period.
    expect([...lowerPaths].every((path) => memoryStorage.store.has(path))).toBe(true);
    await expectUsageMatchesBucketData(bucketStorage, definitionId);

    const batch = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(readCheckpoint, [request]));
    const data = batch.flatMap((chunk) => chunk.chunkData.data);
    expect(data).toHaveLength(12);
    for (let i = 7; i <= 12; i++) {
      const letter = String.fromCharCode(64 + i);
      expect(data.find((op: any) => op.object_id === letter)).toMatchObject({ op: 'PUT' });
    }
  });

  test('compaction round-trip hydrates and replaces S3-backed data', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    // Write several operations, including a repeated object id, to exercise
    // the compaction round-trip.
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Op 1: A@first
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'first version' },
      afterReplicaId: test_utils.rid('A')
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
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: 'updated version' },
      afterReplicaId: test_utils.rid('A')
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
    const storedPaths = memoryStorage.store;
    expect(storedPaths.size).toBeGreaterThan(0);

    // Verify MongoDB documents have storage_ref (no ops[]).
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);

    const docsBefore = await collection.find({}).toArray();
    expect(docsBefore).toHaveLength(1);
    for (const doc of docsBefore) {
      expect(doc.storage_ref).toBeDefined();
      expect(doc.ops).toBeUndefined();
    }

    // Get checkpoint and the bucket name.
    const checkpoint = await bucketStorage.getCheckpoint();
    expect(checkpoint.checkpoint).toBeGreaterThan(0n);

    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const bucket = request.bucket;

    // Read bucket_state before compaction to confirm the writer recorded its
    // aggregate statistics and scheduled a compact check.
    const bucketStateBefore = await bucketStateCollection.findOne({
      _id: { d: definitionId, b: bucket }
    });
    expect(bucketStateBefore).toBeDefined();
    expect(bucketStateBefore!.bucket_stats.count).toBeGreaterThan(0);
    expect(bucketStateBefore!.next_compact_check).toBeDefined();

    // Record the input operations and object path.
    const batchBefore = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = test_utils.getBatchData(batchBefore);
    expect(dataBefore).toHaveLength(4);
    const oldS3Paths = new Set(docsBefore.map((doc) => doc.storage_ref!.path));
    const expectedCompactedOpId = docsBefore
      .filter((doc) => doc._id.b === bucket && doc._id.o <= checkpoint.checkpoint)
      .reduce<bigint | null>((highest, doc) => (highest == null || doc._id.o > highest ? doc._id.o : highest), null);
    expect(expectedCompactedOpId).not.toBeNull();

    // Compact the bucket.
    await compactActive(factory, {
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    // The compacted state reflects the hydrated object contents.
    const bucketStateAfter = await bucketStateCollection.findOne({
      _id: { d: definitionId, b: bucket }
    });
    expect(bucketStateAfter).toBeDefined();
    expect(bucketStateAfter!.compacted_state).toBeDefined();

    expect(bucketStateAfter!.compacted_state!.checksum).not.toBe(0n);
    expect(bucketStateAfter!.compacted_state!.count).toBe(4);

    // Record the highest persisted document that was actually included in the
    // compaction scan, rather than the requested upper bound.
    expect(bucketStateAfter!.compacted_state!.op_id).toBe(expectedCompactedOpId);

    // Compacted MongoDB documents remain metadata shells for object storage.
    const docsAfter = await collection.find({}).toArray();
    expect(docsAfter).toHaveLength(1);
    for (const doc of docsAfter) {
      expect(doc.storage_ref).toBeDefined();
      expect(doc.ops).toBeUndefined();
    }

    // Paths are globally unique, and compaction records delayed deletion markers
    // instead of deleting objects while old readers may still be downloading them.
    const afterS3Paths = new Set(docsAfter.map((doc) => doc.storage_ref!.path));
    expect([...afterS3Paths].some((path) => oldS3Paths.has(path))).toBe(false);
    const pendingDeletes = await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).find({}).toArray();
    expect(pendingDeletes.map((marker) => marker.path)).toEqual(expect.arrayContaining([...oldS3Paths]));
    for (const path of oldS3Paths) {
      expect(storedPaths.has(path)).toBe(true);
    }
    for (const path of afterS3Paths) {
      expect(storedPaths.has(path)).toBe(true);
    }
    await expectUsageMatchesBucketData(bucketStorage, definitionId);

    // The replacement object is readable and contains the expected compacted
    // operation sequence.
    const batchAfter = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = batchAfter.flatMap((chunk) => chunk.chunkData.data);
    expect(dataAfter).toMatchObject([
      { op: 'MOVE' },
      { object_id: 'B', op: 'PUT' },
      { object_id: 'A', op: 'PUT', data: '{"id":"A","description":"updated version"}' },
      { object_id: 'C', op: 'PUT' }
    ]);
  });
});
