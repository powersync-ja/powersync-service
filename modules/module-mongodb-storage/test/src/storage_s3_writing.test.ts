import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';
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

function memoryS3Factory() {
  const { objectStorage, factoryGen } = createMemoryS3TestStorageSuite({
    url: env.MONGO_TEST_URL,
    isCI: env.CI
  });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

describe('S3 write path (Phase 2b red tests)', () => {
  test('1. Write persists ops to S3', async () => {
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
      after: { id: 'item1', description: 'hello' },
      afterReplicaId: test_utils.rid('item1')
    });

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'item2', description: 'world' },
      afterReplicaId: test_utils.rid('item2')
    });

    await writer.commit('1/1');

    // Verify S3 object was uploaded
    const storedPaths = memoryStorage.store;
    expect(storedPaths.size).toBeGreaterThan(0);

    // Find the stored path and decompress + deserialize
    const [path, entry] = [...storedPaths.entries()][0];
    expect(path).toBeTruthy();

    const wrapper = bson.deserialize(entry.data, { promoteValues: false });
    expect(wrapper).toHaveProperty('ops');
    expect(Array.isArray(wrapper.ops)).toBe(true);
    expect(wrapper.ops).toHaveLength(2);

    // Verify ops content: first op is item1, second is item2
    const ops = wrapper.ops as any[];
    expect(ops[0].op).toBe('PUT');
    expect(ops[0].row_id).toBe('item1');
    expect(ops[0].data).toBeTruthy();
    expect(ops[1].op).toBe('PUT');
    expect(ops[1].row_id).toBe('item2');
    expect(ops[1].data).toBeTruthy();

    // Verify MongoDB document has storage_ref and no ops
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBeGreaterThan(0);

    const doc = docs[0];
    expect(doc.storage_ref).toBeDefined();
    expect(doc.storage_ref!.path).toBeTruthy();
    expect(doc.storage_ref!.file_size).toBeTypeOf('number');
    expect(doc.storage_ref!.file_size).toBeGreaterThan(0);
    expect(doc.ops).toBeUndefined();
  });

  test('2. Metadata shell has correct fields', async () => {
    const { memoryStorage, factory: factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 2);
    await writer.markAllSnapshotDone('1/1');

    // Three ops with known data sizes
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'aaaa' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'bbbbb' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'C', description: 'cccccccc' },
      afterReplicaId: test_utils.rid('C')
    });

    await writer.commit('1/1');

    // Read the MongoDB document
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBeGreaterThan(0);

    const doc = docs[0];

    // Metadata fields should be correctly computed
    expect(doc._id).toBeDefined();
    expect(doc._id.b).toBeTypeOf('string');
    expect(doc._id.o).toBeTypeOf('bigint');

    expect(doc.min_op).toBeTypeOf('bigint');
    expect(doc.count).toBe(3);
    expect(doc.checksum).toBeTypeOf('bigint');
    expect(doc.size).toBeGreaterThan(0);

    // target_op should be defined (null or bigint)
    expect(doc.target_op === null || typeof doc.target_op === 'bigint').toBe(true);

    // With S3 offloading, the document MUST have storage_ref and MUST NOT have ops.
    expect(doc.storage_ref).toBeDefined();
    expect(doc.storage_ref!.file_size).toBeTypeOf('number');
    expect(doc.storage_ref!.file_size).toBeGreaterThan(0);

    const stored = await memoryStorage.get(doc.storage_ref!.path);
    const storedOps = (bson.deserialize(stored.data) as { ops: unknown[] }).ops;
    expect(doc.size).toBe(bson.calculateObjectSize(storedOps));
  });

  test('3. No object storage = unchanged behavior', async () => {
    // Factory without object storage
    const factoryGen = mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI
    });
    const unusedMemory = new MemoryObjectStorage();

    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 3);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'x', description: 'test' },
      afterReplicaId: test_utils.rid('x')
    });

    await writer.commit('1/1');

    // MemoryObjectStorage should have no entries (no S3 upload)
    expect(unusedMemory.store.size).toBe(0);

    // MongoDB document should have ops array (as today)
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBeGreaterThan(0);

    const doc = docs[0];
    expect(doc.ops).toBeDefined();
    expect(Array.isArray(doc.ops)).toBe(true);
    expect(doc.ops!.length).toBeGreaterThan(0);
    expect(doc.storage_ref).toBeUndefined();
  });

  test('4. Clearing storage deletes only the replication stream object prefix', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 4);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'clear-prefix', description: 'x'.repeat(1_000) },
      afterReplicaId: test_utils.rid('clear-prefix')
    });
    await writer.commit('1/1');

    const streamPrefix = `bucket-data/${bucketStorage.replicationStreamId}/`;
    expect(Array.from(memoryStorage.store.keys()).some((path) => path.startsWith(streamPrefix))).toBe(true);

    const unrelatedPath = 'bucket-data/999/unrelated/object.bson';
    await memoryStorage.put(unrelatedPath, new Uint8Array(), {
      contentType: 'application/bson',
      contentEncoding: null
    });

    await bucketStorage.clear();

    expect(Array.from(memoryStorage.store.keys()).some((path) => path.startsWith(streamPrefix))).toBe(false);
    expect(memoryStorage.store.has(unrelatedPath)).toBe(true);
  });
});
