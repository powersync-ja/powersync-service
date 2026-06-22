import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { addChecksums, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
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

function s3Factory() {
  const memoryStorage = new MemoryObjectStorage();
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    internalOptions: { objectStorage: memoryStorage, inlineThresholdBytes: 0 }
  });
  return { memoryStorage, factoryGen };
}

describe('V3 checksums with S3 object storage', () => {
  test('partial checksum with start straddling S3-backed document', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;

    const request = bucketRequest(syncRules as any, 'global[]', 0n);
    const bucket = request.bucket;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];

    // Write 6 ops through S3 writer — all land in one S3-backed doc
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');
    for (const id of ['A', 'B', 'C', 'D', 'E', 'F']) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id, description: id },
        afterReplicaId: test_utils.rid(id)
      });
    }
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();

    // Baseline: full checksum with no compacted_state
    const full = (await bucketStorage.getChecksums(checkpoint, [request])).get(bucket)!;

    // Set compacted_state.op_id = 3 to create a partial range starting after op 3.
    // The doc has min_op=1, _id.o=6 so is_fully_included=false for the range (3, 6].
    await db.bucketState(bucketStorage.replicationStreamId).updateOne(
      { _id: { d: definitionId, b: bucket } },
      {
        $set: {
          last_op: 3n,
          compacted_state: { op_id: 3n, count: 0, checksum: 0n, bytes: null },
          estimate_since_compact: { count: 3, bytes: 100 }
        }
      },
      { upsert: true }
    );

    const partial = (await bucketStorage.getChecksums(checkpoint, [request])).get(bucket)!;
    expect(partial.checksum).toBe(full.checksum);
    expect(partial.count).toBe(full.count);
  });

  test('partial checksum with end straddling S3-backed document', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    // Write 50 ops with large data to fill multiple S3 documents
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');
    for (let i = 1; i <= 50; i++) {
      const id = `row${i}`;
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id, description: `value${i}`.repeat(50) },
        afterReplicaId: test_utils.rid(id)
      });
    }
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();

    const full = (await bucketStorage.getChecksums(checkpoint, [request])).get(request.bucket)!;

    // Partial checkpoint at op 7 — some S3 docs will have _id.o > 7 but min_op <= 7
    const partial = (await bucketStorage.getChecksums(7n, [request])).get(request.bucket)!;
    expect(partial.count).toBeGreaterThan(0);
    expect(partial.count).toBeLessThan(full.count);
  });

  test('checksum preserved after CLEAR-producing S3 compaction', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;

    const request = bucketRequest(syncRules as any, 'global[]', 0n);
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // A@1, A@2 get superseded by A@4 (same replica_id for dedup). B@3 independent.
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v1' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v2' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'beta' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v4' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();

    // Compact — produces CLEAR doc from collapsed MOVEs
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

    // Shift compacted_state.op_id back to before the CLEAR doc so getChecksums
    // queries the pipeline (not just reads compacted_state directly).
    await db
      .bucketState(bucketStorage.replicationStreamId)
      .updateOne({ _id: { d: definitionId, b: request.bucket } }, { $set: { 'compacted_state.op_id': 0n } });

    // Ground truth: sum of doc-level checksum fields
    const docs = await db
      .bucketData(bucketStorage.replicationStreamId, definitionId)
      .find({ '_id.b': request.bucket })
      .project({ checksum: 1 })
      .toArray();
    const groundTruth = docs.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);

    const checksum = (await bucketStorage.getChecksums(checkpoint, [request])).get(request.bucket)!;
    expect(checksum.checksum).toBe(groundTruth);

    // Surviving data: 2 PUT ops (B@3 and latest A@4)
    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.length).toBeGreaterThanOrEqual(2);
    expect(dataAfter.some((d: any) => d.object_id === 'B')).toBe(true);
    expect(dataAfter.some((d: any) => d.object_id === 'A')).toBe(true);
  });
});
