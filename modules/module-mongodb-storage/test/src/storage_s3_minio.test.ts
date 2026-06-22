import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { S3ObjectStorage } from '../../src/storage/implementation/v3/object-storage/S3ObjectStorage.js';
import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { env } from './env.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

describe('S3 minio end-to-end', () => {
  test('write, compact, read round-trip against MinIO S3', async () => {
    // Skip if MinIO is not running (manual integration test)
    if (!process.env.MINIO_URL) {
      return;
    }
    const s3 = new S3ObjectStorage({
      bucket: 'powersync-s3-test',
      region: 'us-east-1',
      endpoint: 'http://localhost:9000',
      accessKeyId: 'minioadmin',
      secretAccessKey: 'minioadmin'
    });

    const factoryGen = mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      internalOptions: { objectStorage: s3, inlineThresholdBytes: 0 }
    });
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 })
    );
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'alpha' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'alpha-v2' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'beta' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    const batchBefore = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [request])
    );
    const dataBefore = test_utils.getBatchData(batchBefore);
    expect(dataBefore.length).toBe(3);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const batchAfter = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [request])
    );
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.length).toBeGreaterThanOrEqual(2);
  });
});
