import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { createMemoryS3TestStorageSuite } from './helpers/s3TestFactory.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

function memoryS3Factory(options: { inlineThresholdBytes?: number } = {}) {
  const { objectStorage, factoryGen } = createMemoryS3TestStorageSuite({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    inlineThresholdBytes: options.inlineThresholdBytes ?? 0
  });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

describe('S3 object storage writes', () => {
  test('aborts uploads when the writer signal is aborted', async () => {
    const { memoryStorage, factory: factoryGen } = memoryS3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    const controller = new AbortController();
    await using writer = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, signal: controller.signal });
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'item1', description: 'hello' },
      afterReplicaId: test_utils.rid('item1')
    });

    // Replication is stopping: the flush must not upload anything after this point.
    controller.abort();
    await expect(writer.flush()).rejects.toMatchObject({ name: 'AbortError' });
    expect(memoryStorage.store.size).toBe(0);
  });

  test('writes operation payload and MongoDB metadata shell', async () => {
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

    // Verify the S3 payload.
    const storedPaths = memoryStorage.store;
    expect(storedPaths.size).toBe(1);

    // Find and deserialize the stored payload.
    const [path, entry] = [...storedPaths.entries()][0];
    const wrapper = bson.deserialize(entry.data, { promoteValues: false });
    const ops = wrapper.ops as any[];
    expect(ops).toHaveLength(2);

    expect(ops).toMatchObject([
      { op: 'PUT', row_id: 'item1' },
      { op: 'PUT', row_id: 'item2' }
    ]);
    expect(ops.every((op) => op.data != null)).toBe(true);
    expect(ops.every((op) => typeof op.subkey == 'string')).toBe(true);

    // Verify the corresponding MongoDB metadata shell.
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection.find({}).toArray();
    expect(docs).toHaveLength(1);

    const doc = docs[0];
    expect(doc).toMatchObject({
      count: 2,
      storage_ref: {
        path,
        file_size: entry.data.byteLength
      }
    });
    expect(doc._id.b).toBeTypeOf('string');
    expect(doc._id.o).toBeTypeOf('bigint');
    expect(doc.min_op).toBeTypeOf('bigint');
    expect(doc.checksum).toBeTypeOf('bigint');
    expect(doc.target_op === null || typeof doc.target_op === 'bigint').toBe(true);
    expect(doc.size).toBe(bson.calculateObjectSize(ops));
    expect(doc.ops).toBeUndefined();
  });

  test('clearing storage deletes only the replication stream object prefix', async () => {
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

    const unrelatedPath = `bucket-data/${bucketStorage.replicationStreamId + 1}/unrelated/object.bson`;
    await memoryStorage.put(unrelatedPath, new Uint8Array(), {
      contentType: 'application/bson',
      contentEncoding: null
    });

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const deletionMarkers = db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId);
    await deletionMarkers.insertOne({
      _id: new bson.ObjectId(),
      path: `${streamPrefix}pending-object.bson`,
      delete_after: new Date()
    });

    await bucketStorage.clear();

    expect(Array.from(memoryStorage.store.keys()).some((path) => path.startsWith(streamPrefix))).toBe(false);
    expect(memoryStorage.store.has(unrelatedPath)).toBe(true);
    expect(await db.db.listCollections({ name: deletionMarkers.collectionName }, { nameOnly: true }).hasNext()).toBe(
      false
    );
  });
});
