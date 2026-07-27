import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { DEFAULT_INLINE_THRESHOLD_BYTES } from '../../src/storage/implementation/common/PersistedBatch.js';
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

function s3Factory(threshold?: number) {
  const memoryStorage = new MemoryObjectStorage();
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    objectStorage: memoryStorage,
    inlineThresholdBytes: threshold
  });
  return { memoryStorage, factoryGen };
}

describe('Object storage inline threshold', () => {
  test('stores small documents inline using the default threshold', async () => {
    const { memoryStorage, factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    expect(bucketStorage.inlineThresholdBytes).toBe(DEFAULT_INLINE_THRESHOLD_BYTES);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'small1', description: 'tiny' },
      afterReplicaId: test_utils.rid('small1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'small2', description: 'tiny' },
      afterReplicaId: test_utils.rid('small2')
    });
    await writer.commit('1/1');
    const checkpoint = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const documents = await db
      .bucketData(bucketStorage.replicationStreamId, definitionId)
      .find({ '_id.b': request.bucket })
      .toArray();
    expect(documents).not.toHaveLength(0);
    for (const document of documents) {
      expect(document.size).toBeLessThanOrEqual(DEFAULT_INLINE_THRESHOLD_BYTES);
      expect(document.ops).toBeDefined();
      expect(document.storage_ref).toBeUndefined();
    }
    expect(memoryStorage.store.size).toBe(0);

    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const data = test_utils.getBatchData(batch);
    expect(data.map((op) => op.object_id)).toEqual(['small1', 'small2']);
  });

  test('stores documents above a configured threshold in object storage', async () => {
    const { memoryStorage, factoryGen } = s3Factory(256);
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    expect(bucketStorage.inlineThresholdBytes).toBe(256);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'large', description: 'value'.repeat(500) },
      afterReplicaId: test_utils.rid('large')
    });
    await writer.commit('1/1');
    const checkpoint = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const documents = await db
      .bucketData(bucketStorage.replicationStreamId, definitionId)
      .find({ '_id.b': request.bucket })
      .toArray();
    expect(documents).not.toHaveLength(0);
    for (const document of documents) {
      expect(document.size).toBeGreaterThan(256);
      expect(document.ops).toBeUndefined();
      expect(document.storage_ref).toBeDefined();
    }
    expect(new Set(memoryStorage.store.keys())).toEqual(
      new Set(documents.map((document) => document.storage_ref!.path))
    );

    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    expect(test_utils.getBatchData(batch).map((op) => op.object_id)).toEqual(['large']);
  });
});
