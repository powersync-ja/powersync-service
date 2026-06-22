import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
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
    internalOptions: { objectStorage: memoryStorage, inlineThresholdBytes: threshold }
  });
  return { memoryStorage, factoryGen };
}

describe('S3 inline threshold', () => {
  test('small ops stay inline and survive S3 loss', async () => {
    const { memoryStorage, factoryGen } = s3Factory(1024);
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

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
    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    // Verify ops were stored inline (no S3 objects created)
    const store = (memoryStorage as any).store as Map<string, Buffer>;
    expect(store.size).toBe(0);

    // Reading back should work fine — ops are inline
    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const data = test_utils.getBatchData(batch);
    expect(data.length).toBe(2);
    expect(data.some((d: any) => d.object_id === 'small1')).toBe(true);
    expect(data.some((d: any) => d.object_id === 'small2')).toBe(true);
  });

  test('large ops go to S3 and fail on S3 loss', async () => {
    const { memoryStorage, factoryGen } = s3Factory(256);
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Each op ~500 bytes, 20 ops = ~10KB BSON, well above 256 threshold
    for (let i = 1; i <= 20; i++) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: `row${i}`, description: `value${i}`.repeat(50) },
        afterReplicaId: test_utils.rid(`row${i}`)
      });
    }
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    // Verify ops were stored on S3
    const store = (memoryStorage as any).store as Map<string, Buffer>;
    expect(store.size).toBeGreaterThan(0);

    // Clear S3 — simulate S3 loss
    store.clear();

    // Reading back should fail hard (S3 object no longer exists)
    await expect(test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]))).rejects.toThrow();
  });
});
