import {
  addChecksums,
  CheckpointChecksumInvalidatedError,
  storage,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
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
  return { memoryStorage: objectStorage, factoryGen };
}

describe('V3 checksums with S3 object storage', () => {
  test('partial checksum with start straddling S3-backed document', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;

    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const bucket = request.bucket;
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];

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
    const checkpoint = await bucketStorage.getCheckpoint();

    // Baseline: full checksum with no compacted_state
    const full = (await bucketStorage.getChecksums(checkpoint, [request])).get(bucket)!;

    // Set compacted_state.op_id = 3 to create a partial range starting after op 3.
    // The doc has min_op=1, _id.o=6 so is_fully_included=false for the range (3, 6].
    await db.bucketState(bucketStorage.replicationStreamId).updateOne(
      { _id: { d: definitionId, b: bucket } },
      {
        $set: {
          last_op: 3n,
          compacted_state: { op_id: 3n, count: 0, checksum: 0n, bytes: 0n, chunks: 0, at: new Date() }
        }
      },
      { upsert: true }
    );

    const partial = (await bucketStorage.getChecksums(checkpoint, [request])).get(bucket)!;
    expect(partial.checksum).toBe(full.checksum);
    expect(partial.count).toBe(full.count);
  });

  test('partial checksum with end straddling S3-backed document invalidates the checkpoint', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;

    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];

    // Write several operations into one S3-backed document that straddles the
    // requested checkpoint.
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');
    for (let i = 1; i <= 12; i++) {
      const id = `row${i}`;
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id, description: `value${i}`.repeat(50) },
        afterReplicaId: test_utils.rid(id)
      });
    }
    await writer.commit('1/1');
    const documents = await db
      .bucketData(bucketStorage.replicationStreamId, definitionId)
      .find({ '_id.b': request.bucket })
      .toArray();
    expect(documents).toHaveLength(1);
    expect(documents[0].storage_ref).toBeDefined();
    expect(documents[0].ops).toBeUndefined();

    // The document starts before op 7 and ends after it.
    await expect(bucketStorage.getChecksums(test_utils.testCheckpoint(7n), [request])).rejects.toBeInstanceOf(
      CheckpointChecksumInvalidatedError
    );
  });

  test('checksum preserved after CLEAR-producing S3 compaction', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;

    const request = bucketRequest(syncRules.syncConfigContent[0], 'global[]', 0n);
    const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];

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
      tag: storage.SaveOperationTag.UPDATE,
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
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: 'v4' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('1/1');
    const checkpoint = await bucketStorage.getCheckpoint();

    // Compact — produces CLEAR doc from collapsed MOVEs
    await bucketStorage.compact({
      maxOpId: checkpoint.checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0
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
      .toArray();
    const groundTruth = docs.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);
    expect(docs.some((doc) => doc.has_clear_op)).toBe(true);
    expect(docs.every((doc) => doc.storage_ref != null && doc.ops == null)).toBe(true);

    const checksum = (await bucketStorage.getChecksums(checkpoint, [request])).get(request.bucket)!;
    expect(checksum.checksum).toBe(groundTruth);

    // The two superseded A operations collapse into CLEAR.
    const batchAfter = await test_utils.getBatchArray(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = batchAfter.flatMap((chunk) => chunk.chunkData.data);
    expect(dataAfter).toMatchObject([{ op: 'CLEAR' }, { object_id: 'B', op: 'PUT' }, { object_id: 'A', op: 'PUT' }]);
  });
});
