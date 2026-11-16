import { MongoReplicationBatchSize, MongoStorageConfig } from '@module/types/types.js';
import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { BatchOperationBuffer } from '../../src/storage/implementation/common/BatchOperationBuffer.js';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import {
  DEFAULT_REPLICATION_BATCH_LIMITS,
  REPLICATION_BATCH_LIMITS
} from '../../src/storage/implementation/ReplicationBatchLimits.js';
import { env } from './env.js';

const STORAGE_CONFIG = { type: 'mongodb', uri: 'mongodb://localhost:27017/powersync' } as const;
const PROFILES = ['small', 'medium', 'large'] as const;
const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    items:
      query: SELECT * FROM items
`;

describe('replication batch configuration', () => {
  test.each(PROFILES)('decodes %s', (profile) => {
    const config = MongoStorageConfig.decode({ ...STORAGE_CONFIG, replication_batch_size: profile });
    expect(config.replication_batch_size).toBe(profile);
  });

  test.each(['huge', '', 1, null])('rejects invalid profile %j', (profile) => {
    expect(() =>
      MongoStorageConfig.decode({ ...STORAGE_CONFIG, replication_batch_size: profile as MongoReplicationBatchSize })
    ).toThrow();
  });

  test('defaults to medium and preserves the previous limits as large', () => {
    expect(MongoStorageConfig.decode(STORAGE_CONFIG).replication_batch_size).toBeUndefined();
    expect(DEFAULT_REPLICATION_BATCH_LIMITS).toBe(REPLICATION_BATCH_LIMITS.medium);
    expect(REPLICATION_BATCH_LIMITS.large).toEqual({
      maxInputBatchCount: 16_000,
      maxInputBatchBytes: 8 * 1024 * 1024,
      maxTransactionBatchCount: 16_000,
      maxTransactionBatchBytes: 16 * 1024 * 1024,
      maxPipelineBytes: 128 * 1024 * 1024
    });
  });

  test.each(PROFILES)('%s uses its own input and transaction byte targets', (profile) => {
    const limits = REPLICATION_BATCH_LIMITS[profile];
    const input = new OperationBatch(limits);
    input.currentSize = limits.maxInputBatchBytes;
    expect(input.shouldFlush()).toBe(false);
    input.currentSize++;
    expect(input.shouldFlush()).toBe(true);

    const writes = new BatchOperationBuffer(
      1,
      limits.maxTransactionBatchBytes - 1,
      { checkDefinitionId: (id) => id!, getBucketDefinitionId: () => '' },
      undefined,
      limits
    );
    expect(writes.shouldFlushTransaction(limits.maxTransactionBatchCount - 1)).toBe(false);
    expect(writes.shouldFlushTransaction(limits.maxTransactionBatchCount)).toBe(true);
    writes.currentSize++;
    expect(writes.shouldFlushTransaction(0)).toBe(true);
  });
});

describe.each([1, 4])('replication batch configuration v%s', (storageVersion) => {
  test.each<MongoReplicationBatchSize | undefined>([undefined, ...PROFILES])(
    'automatically flushes at the configured row count (%s)',
    async (profile) => {
      const gen = mongoTestStorageFactoryGenerator({
        url: env.MONGO_TEST_URL,
        isCI: env.CI,
        replicationBatchSize: profile
      });
      await using factory = await gen.factory();
      const limits = REPLICATION_BATCH_LIMITS[profile ?? 'medium'];
      expect(factory.replicationBatchLimits).toBe(limits);
      const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion }));
      const buckets = await getTestStorage(factory, stream);
      const beforeBatchFlush = vi.fn();
      await using writer = await buckets.createWriter({
        ...test_utils.BATCH_OPTIONS,
        hooks: { beforeBatchFlush }
      });
      const table = await test_utils.resolveTestTable(writer, 'items', ['id'], gen, 1);
      await writer.markAllSnapshotDone('1/1');
      beforeBatchFlush.mockClear();

      // Observe the real transaction splitting decision as well as the input flush.
      using transaction = vi.spyOn(BatchOperationBuffer.prototype, 'shouldFlushTransaction');
      const save = (id: number) =>
        writer.save({
          sourceTable: table,
          tag: storage.SaveOperationTag.INSERT,
          afterReplicaId: String(id),
          after: { id: String(id) }
        });
      for (let i = 1; i < limits.maxInputBatchCount; i++) {
        await save(i);
      }
      expect(beforeBatchFlush).not.toHaveBeenCalled();
      await save(limits.maxInputBatchCount);
      expect(beforeBatchFlush).toHaveBeenCalled();
      expect(transaction.mock.results.some((result) => result.type === 'return' && result.value === true)).toBe(true);
      await writer.commit('1/2');
      expect(await buckets.getCheckpoint()).toMatchObject({
        lsn: '1/2',
        checkpoint: BigInt(limits.maxInputBatchCount)
      });
    }
  );
});
