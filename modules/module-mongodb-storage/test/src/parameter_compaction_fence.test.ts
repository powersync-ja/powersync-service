import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import type { VersionedPowerSyncMongo } from '../../src/storage/implementation/db.js';
import type { SyncRuleDocumentBase } from '../../src/storage/implementation/models.js';
import { MongoParameterCompactor } from '../../src/storage/implementation/MongoParameterCompactor.js';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/MongoSyncBucketStorage.js';
import { MongoParameterCompactorV1 } from '../../src/storage/implementation/v1/MongoParameterCompactorV1.js';
import { MongoParameterCompactorV3 } from '../../src/storage/implementation/v3/MongoParameterCompactorV3.js';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const PARAMETER_RULES = `
bucket_definitions:
  test:
    parameters: select id from test where id = request.user_id()
    data: []
`;

/**
 * Wraps a collection so that any delete fails. Used to check that an interrupted compaction pass
 * leaves the invalidation fence advanced, but not the compaction cursor.
 */
function withFailingDeletes<T extends object>(collection: T): T {
  return new Proxy(collection, {
    get(target, property) {
      if (property == 'deleteMany' || property == 'bulkWrite') {
        return () => Promise.reject(new Error('simulated delete failure'));
      }
      const value = Reflect.get(target, property, target);
      // Bind to the target: the driver's collection uses private fields internally.
      return typeof value == 'function' ? value.bind(target) : value;
    }
  }) as T;
}

class FailingParameterCompactorV1 extends MongoParameterCompactorV1 {
  protected override async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    return (await super.getCollections()).map(withFailingDeletes);
  }
}

class FailingParameterCompactorV3 extends MongoParameterCompactorV3 {
  protected override async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    return (await super.getCollections()).map(withFailingDeletes);
  }
}

describe('parameter compaction invalidation fence', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`storage v${storageVersion}`, () => {
      function createCompactor(
        db: VersionedPowerSyncMongo,
        streamId: number,
        checkpoint: InternalOpId,
        options: storage.CompactOptions & { failDeletes?: boolean } = {}
      ): MongoParameterCompactor {
        const { failDeletes, ...compactOptions } = options;
        if (storageVersion >= 3) {
          const Compactor = failDeletes ? FailingParameterCompactorV3 : MongoParameterCompactorV3;
          return new Compactor(db, streamId, checkpoint, compactOptions);
        }
        const Compactor = failDeletes ? FailingParameterCompactorV1 : MongoParameterCompactorV1;
        return new Compactor(db, streamId, checkpoint, compactOptions);
      }

      async function readCompactionState(db: VersionedPowerSyncMongo, streamId: number) {
        const doc = (await db.sync_rules.findOne({ _id: streamId })) as SyncRuleDocumentBase;
        return {
          compactedBefore: doc.parameter_compaction?.compacted_before ?? null,
          invalidBefore: doc.parameter_compaction?.checkpoint_changes_invalid_before ?? null
        };
      }

      /**
       * Replicates three checkpoints:
       *
       * 1. `checkpoint1`: t1 and t2 inserted.
       * 2. t2 deleted - this writes a parameter tombstone, the only remaining record of t2's lookup.
       * 3. `checkpoint3`: t3 inserted, which puts the tombstone strictly below the checkpoint,
       *    making it eligible for compaction.
       */
      async function replicateParameterHistory(factory: storage.BucketStorageFactory) {
        const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(PARAMETER_RULES, { storageVersion }));
        const processingStorage = factory.getInstance(syncRules);
        const writer = await processingStorage.createWriter(test_utils.BATCH_OPTIONS);
        const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

        await writer.markAllSnapshotDone('1/1');
        for (const id of ['t1', 't2']) {
          await writer.save({
            sourceTable: testTable,
            tag: storage.SaveOperationTag.INSERT,
            after: { id },
            afterReplicaId: test_utils.rid(id)
          });
        }
        await writer.commit('1/1');

        const active = await factory.getActiveSyncConfig();
        if (active == null) {
          throw new Error('Expected an active sync config');
        }
        const bucketStorage = active.storage as MongoSyncBucketStorage;
        const checkpoint1 = await bucketStorage.getCheckpoint();

        await writer.save({
          sourceTable: testTable,
          tag: storage.SaveOperationTag.DELETE,
          before: { id: 't2' },
          beforeReplicaId: test_utils.rid('t2')
        });
        await writer.commit('1/2');

        await writer.save({
          sourceTable: testTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 't3' },
          afterReplicaId: test_utils.rid('t3')
        });
        await writer.commit('1/3');
        await writer.dispose();

        const checkpoint3 = await bucketStorage.getCheckpoint();
        expect(checkpoint3.checkpoint).toBeGreaterThan(checkpoint1.checkpoint);

        return {
          bucketStorage,
          replicationStream: active.replicationStream,
          streamId: syncRules.replicationStreamId,
          checkpoint1,
          checkpoint3
        };
      }

      test('invalidates parameter buckets for checkpoints below the fence', async () => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const { bucketStorage, streamId, checkpoint1, checkpoint3 } = await replicateParameterHistory(factory);
        const db = bucketStorage.db;

        // The tombstone for t2 and the new entry for t3 are both in this range.
        const baseline = await bucketStorage.getCheckpointChanges({
          lastCheckpoint: checkpoint1,
          nextCheckpoint: checkpoint3
        });
        expect(baseline.invalidateParameterBuckets).toBe(false);
        expect(baseline.updatedParameterLookups.size).toBe(2);

        await createCompactor(db, streamId, checkpoint3.checkpoint).compact();

        // The fence is advanced because the pass deleted parameter entries.
        expect(await readCompactionState(db, streamId)).toEqual({
          compactedBefore: checkpoint3.checkpoint,
          invalidBefore: checkpoint3.checkpoint
        });

        // A checkpoint read after compaction captures the fence. The transition from checkpoint1
        // can no longer be resolved to individual lookups, so all parameter buckets are
        // invalidated. This is a different cache entry from the baseline above, even though the
        // checkpoint and LSN are unchanged.
        const checkpoint3After = await bucketStorage.getCheckpoint();
        expect(checkpoint3After.checkpoint).toBe(checkpoint3.checkpoint);
        const afterCompaction = await bucketStorage.getCheckpointChanges({
          lastCheckpoint: checkpoint1,
          nextCheckpoint: checkpoint3After
        });
        expect(afterCompaction.invalidateParameterBuckets).toBe(true);
        expect(afterCompaction.updatedParameterLookups.size).toBe(0);

        // A transition starting at the fence is not affected by it.
        const atFence = await bucketStorage.getCheckpointChanges({
          lastCheckpoint: checkpoint3After,
          nextCheckpoint: checkpoint3After
        });
        expect(atFence.invalidateParameterBuckets).toBe(false);
        expect(atFence.updatedParameterLookups.size).toBe(0);
      });

      test('reads changes at the checkpoint snapshot, including compacted entries', async () => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const { bucketStorage, replicationStream, streamId, checkpoint1, checkpoint3 } =
          await replicateParameterHistory(factory);
        const db = bucketStorage.db;

        const baseline = await bucketStorage.getCheckpointChanges({
          lastCheckpoint: checkpoint1,
          nextCheckpoint: checkpoint3
        });
        expect(baseline.updatedParameterLookups.size).toBe(2);

        await createCompactor(db, streamId, checkpoint3.checkpoint).compact();

        // checkpoint3 was captured before compaction, so it does not have the fence and the
        // change query runs at its snapshot. That snapshot still contains the deleted tombstone,
        // which is the only record of t2's lookup.
        //
        // A separate storage instance is used to get a cold checkpoint-changes cache.
        const coldStorage = (factory as MongoBucketStorage).getInstance(replicationStream);
        const atSnapshot = await coldStorage.getCheckpointChanges({
          lastCheckpoint: checkpoint1,
          nextCheckpoint: checkpoint3
        });
        expect(atSnapshot.invalidateParameterBuckets).toBe(false);
        expect(atSnapshot.updatedParameterLookups).toEqual(baseline.updatedParameterLookups);
      });

      test('advances the fence before the first delete, and the cursor only after the last', async () => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const { bucketStorage, streamId, checkpoint3 } = await replicateParameterHistory(factory);
        const db = bucketStorage.db;

        // V1 seeds the cursor when the stream is created, V3 leaves it unset.
        const initialState = await readCompactionState(db, streamId);
        expect(initialState.invalidBefore).toBeNull();

        await expect(
          createCompactor(db, streamId, checkpoint3.checkpoint, { failDeletes: true }).compact()
        ).rejects.toThrow('simulated delete failure');

        // The fence was committed before the first delete was attempted, but the interrupted pass
        // may not skip any deletion work on a retry, so the cursor stays where it was.
        expect(await readCompactionState(db, streamId)).toEqual({
          compactedBefore: initialState.compactedBefore,
          invalidBefore: checkpoint3.checkpoint
        });

        // Retrying completes the pass.
        await createCompactor(db, streamId, checkpoint3.checkpoint).compact();
        expect(await readCompactionState(db, streamId)).toEqual({
          compactedBefore: checkpoint3.checkpoint,
          invalidBefore: checkpoint3.checkpoint
        });
      });

      test('an aborted pass does not advance the cursor', async () => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const { bucketStorage, streamId, checkpoint3 } = await replicateParameterHistory(factory);
        const db = bucketStorage.db;

        const initialState = await readCompactionState(db, streamId);
        const controller = new AbortController();
        controller.abort();
        await expect(
          createCompactor(db, streamId, checkpoint3.checkpoint, { signal: controller.signal }).compact()
        ).rejects.toThrow();

        expect(await readCompactionState(db, streamId)).toEqual(initialState);
        expect(initialState.invalidBefore).toBeNull();
      });

      test('does not advance the fence for a pass without deletes', async () => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const { bucketStorage, streamId, checkpoint3 } = await replicateParameterHistory(factory);
        const db = bucketStorage.db;

        // Compact past every persisted entry, so that the next pass has an empty range.
        const firstTarget = checkpoint3.checkpoint + 1000n;
        await createCompactor(db, streamId, firstTarget).compact();
        expect(await readCompactionState(db, streamId)).toEqual({
          compactedBefore: firstTarget,
          invalidBefore: firstTarget
        });

        const secondTarget = firstTarget + 1000n;
        await createCompactor(db, streamId, secondTarget).compact();
        // The cursor advances, but nothing was deleted, so checkpoint change detection stays
        // available for everything above the previous fence.
        expect(await readCompactionState(db, streamId)).toEqual({
          compactedBefore: secondTarget,
          invalidBefore: firstTarget
        });
      });
    });
  }
});
