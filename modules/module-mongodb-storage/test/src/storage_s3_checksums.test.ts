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
    const partialChecksum = ops
      .filter((o) => o > 3n)
      .reduce((sum, op) => addChecksums(sum, Number(op)), 0);
    const compactedChecksum = ops
      .filter((o) => o <= 3n)
      .reduce((sum, op) => addChecksums(sum, Number(op)), 0);

    // Set compacted_state.op_id = 3n to create a partial range starting after op 3.
    // The checksum pipeline must $filter ops in the S3-backed doc to only sum ops > 3.
    const bucketStateCollection = db.bucketState(bucketStorage.replicationStreamId);
    await bucketStateCollection.insertOne({
      _id: { d: definitionId, b: bucket },
      last_op: 3n,
      compacted_state: {
        op_id: 3n,
        count: 3,
        checksum: BigInt(compactedChecksum),
        bytes: null
      },
      estimate_since_compact: { count: 3, bytes: 100 }
    });

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
});
