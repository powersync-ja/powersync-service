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
    internalOptions: { objectStorage: memoryStorage }
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

    // Write ops through S3 writer: ops 10, 20, 30, 40, 50, 60
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Use raw replica IDs to control op ordering — ops 1-6 will map to internal ops 1-6
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'a1' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'b1' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'C', description: 'c1' },
      afterReplicaId: test_utils.rid('C')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'D', description: 'd1' },
      afterReplicaId: test_utils.rid('D')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'E', description: 'e1' },
      afterReplicaId: test_utils.rid('E')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'F', description: 'f1' },
      afterReplicaId: test_utils.rid('F')
    });
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();
    const ops = [1n, 2n, 3n, 4n, 5n, 6n];

    // Compute expected checksums
    const fullChecksum = ops.reduce((sum, op) => addChecksums(sum, Number(op)), 0);
    const partialChecksum = ops.filter((o) => o > 3n).reduce((sum, op) => addChecksums(sum, Number(op)), 0);
    const compactedChecksum = ops.filter((o) => o <= 3n).reduce((sum, op) => addChecksums(sum, Number(op)), 0);

    // Set compacted_state.op_id = 3n to create a partial range starting after op 3.
    // The checksum pipeline must $filter ops in the S3-backed doc to only sum ops > 3.
    // Use updateOne with upsert — the writer may have already created bucket_state.
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    await bucketStateCollection.updateOne(
      { _id: { d: definitionId, b: bucket } },
      {
        $set: {
          last_op: 3n,
          compacted_state: {
            op_id: 3n,
            count: 3,
            checksum: BigInt(compactedChecksum),
            bytes: null
          },
          estimate_since_compact: { count: 3, bytes: 100 }
        }
      },
      { upsert: true }
    );

    const result = await bucketStorage.getChecksums(checkpoint, [request]);
    const checksumResult = result.get(bucket)!;

    expect(checksumResult.checksum).toBe(fullChecksum);
    expect(checksumResult.count).toBe(6);
  });

  test('partial checksum with end straddling S3-backed document', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    // Write 50 ops with large data to fill multiple S3 documents (default
    // chunk limit ~16KB; each op is ~500 bytes → ~25KB → 2+ S3 docs).
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

    // Pick a partial checkpoint that falls between ops (e.g., op 7).
    // Some S3-backed docs will have _id.o > 7 but min_op <= 7.
    const partialCheckpoint = 7n;
    const partialResult = await bucketStorage.getChecksums(partialCheckpoint, [request]);
    const partial = partialResult.get(request.bucket)!;

    // Full checksum (all ops) should be larger than partial
    const fullResult = await bucketStorage.getChecksums(checkpoint, [request]);
    const full = fullResult.get(request.bucket)!;

    // At least one S3-backed doc straddles the partial checkpoint: partial
    // checksum must be non-zero and less than full checksum (ops 8-20 excluded).
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

    // A@1, A@2 get superseded by A@4 (same row_id='A'). B@3 is independent.
    // Compaction will turn A@1, A@2 into MOVE tombstones, then CLEAR collapses them.
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v1' },
      afterReplicaId: test_utils.rid('A-v1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v2' },
      afterReplicaId: test_utils.rid('A-v2')
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
      afterReplicaId: test_utils.rid('A-v4')
    });
    await writer.commit('1/1');

    const { checkpoint } = await bucketStorage.getCheckpoint();

    // Compact the bucket. The compactor produces a CLEAR doc (collapsing A@1, A@2
    // MOVEs) and surviving PUTs (B@3, A@4). compacted_state is set to the total.
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

    // Manually shift compacted_state.op_id back to before the CLEAR doc so that
    // getChecksums will query the pipeline (not just read from compacted_state).
    // The pipeline must then process the CLEAR doc and detect has_clear_op = 1.
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    await bucketStateCollection.updateOne(
      { _id: { d: definitionId, b: request.bucket } },
      { $set: { 'compacted_state.op_id': 0n } }
    );

    // Compute ground-truth checksum by summing doc-level checksum fields.
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const docs = await collection
      .find({ '_id.b': request.bucket })
      .project({ checksum: 1 })
      .toArray();
    const groundTruth = docs.reduce((sum: number, d: any) => addChecksums(sum, Number(d.checksum)), 0);

    // getChecksums must return the same total. The CLEAR doc in the pipeline
    // should be treated as a full/replacing checksum (has_clear_op detected),
    // not added on top of compacted_state as a partial checksum.
    const result = await bucketStorage.getChecksums(checkpoint, [request]);
    const checksum = result.get(request.bucket)!;
    expect(checksum.checksum).toBe(groundTruth);

    // Surviving data: 2 PUT ops (B@3 and A@4)
    const batchAfter = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [request])
    );
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.length).toBeGreaterThanOrEqual(2);
    expect(dataAfter.some((d: any) => d.object_id === 'B')).toBe(true);
    expect(dataAfter.some((d: any) => d.object_id === 'A')).toBe(true);
  });
});
