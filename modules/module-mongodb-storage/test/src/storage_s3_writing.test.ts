import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import * as zstd from '@mongodb-js/zstd';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

/**
 * Creates a factory with MemoryObjectStorage wired in, plus a
 * reference to the memoryStorage for assertions.
 */
function s3Factory() {
  const memoryStorage = new MemoryObjectStorage();
  const factory = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    internalOptions: { objectStorage: memoryStorage, inlineThresholdBytes: 0 }
  });
  return { memoryStorage, factory };
}

describe('S3 write path (Phase 2b red tests)', () => {
  test('1. Write persists ops to S3', async () => {
    const { memoryStorage, factory: factoryGen } = s3Factory();
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
    // The memoryStorage should have at least one entry.
    // Since the write path isn't implemented yet, this will FAIL.
    const storedPaths = (memoryStorage as any).store as Map<string, Buffer>;
    expect(storedPaths.size).toBeGreaterThan(0);

    // Find the stored path and decompress + deserialize
    const [path, compressed] = [...storedPaths.entries()][0];
    expect(path).toBeTruthy();

    const decompressed = await zstd.decompress(compressed);
    const wrapper = bson.deserialize(decompressed, { promoteValues: false });
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
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBeGreaterThan(0);

    const doc = docs[0];
    expect(doc.storage_ref).toBeDefined();
    expect(doc.storage_ref!.path).toBeTruthy();
    expect(doc.storage_ref!.compressed_size).toBeTypeOf('number');
    expect(doc.storage_ref!.compressed_size).toBeGreaterThan(0);
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
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
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
    expect(doc.storage_ref!.compressed_size).toBeTypeOf('number');
    expect(doc.storage_ref!.compressed_size).toBeGreaterThan(0);

    // The size field should reflect the sum of decompressed op.data lengths
    // (NOT the compressed size from storage_ref.compressed_size)
    expect(doc.size).not.toBe(doc.storage_ref!.compressed_size);
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
    const storedPaths = (unusedMemory as any).store as Map<string, Buffer>;
    expect(storedPaths.size).toBe(0);

    // MongoDB document should have ops array (as today)
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection.find({}).toArray();
    expect(docs.length).toBeGreaterThan(0);

    const doc = docs[0];
    expect(doc.ops).toBeDefined();
    expect(Array.isArray(doc.ops)).toBe(true);
    expect(doc.ops!.length).toBeGreaterThan(0);
    expect(doc.storage_ref).toBeUndefined();
  });
});
