import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { compactActive, register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

for (let storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Mongo Sync Bucket Storage - Parameters - v${storageVersion}`, () =>
    register.registerDataStorageParameterTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));

  describe(`Mongo Sync Bucket Storage - Data - v${storageVersion}`, () =>
    register.registerDataStorageDataTests({
      ...INITIALIZED_MONGO_STORAGE_FACTORY,
      storageVersion,
      compressedBucketStorage: storageVersion >= 3
    }));

  describe(`Mongo Sync Bucket Storage - Checkpoints - v${storageVersion}`, () =>
    register.registerDataStorageCheckpointTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion }));

  describe(`Mongo Sync Bucket Storage - write checkpoint metadata - v${storageVersion}`, () => {
    test('uses checkpoint_requested_at as the client-requested checkpoint marker', async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
bucket_definitions:
  global:
    data: []
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules);

      // user1 has no existing row, so this covers updateMany with upsert.
      // The initial request stores checkpoint id 42 at source head 5/0.
      const requestedResult = await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '5/0' }, checkpoint_request_id: 42n }
      ]);
      expect(requestedResult.writeCheckpoints.get('user1')).toEqual(42n);
      expect(requestedResult.shouldAdvance).toBe(true);

      const requested = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      expect(requested?.client_id).toEqual(42n);
      expect(requested?.lsns).toEqual({ '1': '5/0' });
      expect(requested?.processed_at_lsn).toBeNull();
      expect(requested?.checkpoint_requested_at).toBeInstanceOf(Date);

      // Request id 41 is lower than the stored id 42, so the entire stale
      // request is ignored, including its newer 6/0 source head.
      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '6/0' }, checkpoint_request_id: 41n }
      ]);
      const stale = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      expect(stale?.checkpoint_requested_at).toEqual(requested?.checkpoint_requested_at);

      const expiredRequestedAt = new Date('2024-01-01T00:00:00.000Z');
      await factory.db.write_checkpoints.updateOne(
        { user_id: 'user1' },
        { $set: { checkpoint_requested_at: expiredRequestedAt } }
      );
      // Although the previous incoming request was 41, the stored id is still
      // 42. This is therefore an equal-id retry of the original request.
      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '6/0' }, checkpoint_request_id: 42n }
      ]);
      const retried = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      // Retrying the current id refreshes its retention timestamp without
      // replacing the original source head or resetting its processed state.
      expect(retried?.checkpoint_requested_at).toBeInstanceOf(Date);
      expect(retried!.checkpoint_requested_at!.getTime()).toBeGreaterThan(expiredRequestedAt.getTime());
      expect(retried?.lsns).toEqual({ '1': '5/0' });
      expect(retried?.processed_at_lsn).toBeNull();

      await factory.db.write_checkpoints.updateOne(
        { user_id: 'user1' },
        { $set: { checkpoint_requested_at: expiredRequestedAt } }
      );
      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '6/0' }, checkpoint_request_id: 43n }
      ]);
      const advanced = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      // A greater id refreshes retention and advances the stored checkpoint.
      expect(advanced?.checkpoint_requested_at).toBeInstanceOf(Date);
      expect(advanced!.checkpoint_requested_at!.getTime()).toBeGreaterThan(expiredRequestedAt.getTime());
      expect(advanced?.client_id).toEqual(43n);
      expect(advanced?.lsns).toEqual({ '1': '6/0' });

      await bucketStorage.createManagedWriteCheckpoints([{ user_id: 'user1', heads: { '1': '7/0' } }]);
      const generated = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      // Generated checkpoints unset the field, keeping the document out of the
      // partial checkpoint_requested_at index.
      expect(generated).not.toBeNull();
      expect(generated?.checkpoint_requested_at).toBeUndefined();

      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user2', heads: { '1': '8/0' }, checkpoint_request_id: 50n }
      ]);
      await factory.db.write_checkpoints.updateOne(
        { user_id: 'user2' },
        { $set: { checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z') } }
      );
      await bucketStorage.compact({
        compactBuckets: [],
        deleteCheckpointRequestsBefore: new Date('2024-02-01T00:00:00.000Z')
      });
      // The request is expired but still pending, so compaction must retain it.
      await expect(factory.db.write_checkpoints.findOne({ user_id: 'user2' })).resolves.not.toBeNull();

      // Advancing replication to the request head sets processed_at_lsn.
      await using managedWriter = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      await managedWriter.markAllSnapshotDone('1/1');
      await managedWriter.keepalive('8/0');

      await compactActive(factory, {
        compactBuckets: [],
        deleteCheckpointRequestsBefore: new Date('2024-02-01T00:00:00.000Z')
      });
      // The request is now both expired and processed, so it can be removed.
      await expect(factory.db.write_checkpoints.findOne({ user_id: 'user2' })).resolves.toBeNull();

      if (storageVersion < 3) {
        bucketStorage.setWriteCheckpointMode({
          mode: storage.WriteCheckpointMode.CUSTOM
        });
        await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
        await writer.markAllSnapshotDone('1/1');
        const customCheckpointRequestedAt = new Date('2024-01-01T00:00:00.000Z');
        writer.addCustomWriteCheckpoint({
          user_id: 'custom1',
          checkpoint: 51n,
          checkpoint_requested_at: customCheckpointRequestedAt
        });
        await writer.flush();
        const customRequested = await factory.db.custom_write_checkpoints.findOne({ user_id: 'custom1' });
        expect(customRequested?.checkpoint_requested_at).toEqual(customCheckpointRequestedAt);

        writer.addCustomWriteCheckpoint({
          user_id: 'custom1',
          checkpoint: 52n
        });
        await writer.flush();
        const customGenerated = await factory.db.custom_write_checkpoints.findOne({ user_id: 'custom1' });
        expect(customGenerated).not.toBeNull();
        expect(customGenerated?.checkpoint_requested_at).toBeUndefined();
      }
    });

    test.runIf(storageVersion >= 3)(
      'stores and streams checkpoints from different event definitions independently',
      async (context) => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const syncRules = await factory.updateSyncRules(
          updateSyncRulesFromYaml(
            `
config:
  edition: 3

streams:
  global:
    query: SELECT * FROM checkpoints

event_definitions:
  checkpoint_a:
    payloads:
      - SELECT id FROM checkpoints WHERE kind = 'a'
  checkpoint_b:
    payloads:
      - SELECT id FROM checkpoints WHERE kind = 'b'
  unrelated_event:
    payloads:
      - SELECT id FROM checkpoints WHERE kind = 'unrelated'
    `,
            { storageVersion }
          )
        );
        const bucketStorage = factory.getInstance(syncRules);
        const db = bucketStorage.db as VersionedPowerSyncMongoV3;
        const activeSyncConfig = bucketStorage.getParsedSyncRules({ defaultSchema: 'public' });
        const eventA = activeSyncConfig.eventDescriptors.find((event) => event.name == 'checkpoint_a')!;
        const eventB = activeSyncConfig.eventDescriptors.find((event) => event.name == 'checkpoint_b')!;
        const unrelatedEvent = activeSyncConfig.eventDescriptors.find((event) => event.name == 'unrelated_event')!;
        const mapping = syncRules.syncConfigContent[0].mapping;
        const eventAId = mapping.eventId(eventA);
        const eventBId = mapping.eventId(eventB);
        const unrelatedEventId = mapping.eventId(unrelatedEvent);
        expect(() =>
          bucketStorage.setWriteCheckpointMode({
            mode: storage.WriteCheckpointMode.CUSTOM
          })
        ).toThrow('eventName');
        bucketStorage.setWriteCheckpointMode({
          mode: storage.WriteCheckpointMode.CUSTOM,
          eventName: 'unknown-event'
        });
        await expect(
          bucketStorage.lastWriteCheckpoint({ user_id: 'user1', syncConfig: activeSyncConfig })
        ).rejects.toThrow('Unknown custom checkpoint event definition unknown-event');

        bucketStorage.setWriteCheckpointMode({
          mode: storage.WriteCheckpointMode.CUSTOM,
          eventName: eventA.name
        });
        // Reads before the first checkpoint must behave like an empty result;
        // they must not require the lazily-created collection to exist.
        await expect(
          bucketStorage.lastWriteCheckpoint({ user_id: 'user1', syncConfig: activeSyncConfig })
        ).resolves.toBeNull();

        await using invalidWriter = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
        invalidWriter.addCustomWriteCheckpoint({ user_id: 'invalid', checkpoint: 1n });
        await expect(invalidWriter.flush()).rejects.toThrow('require an event definition id');
        await expect(db.listCustomCheckpointRequestCollections(syncRules.replicationStreamId)).resolves.toEqual([]);

        await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
        await writer.markAllSnapshotDone('1/1');
        const abortController = new AbortController();
        context.onTestFinished(() => abortController.abort());
        const iter = bucketStorage
          .watchCheckpointChanges({
            user_id: 'user1',
            syncConfig: activeSyncConfig,
            signal: abortController.signal
          })
          [Symbol.asyncIterator]();

        writer.addCustomWriteCheckpoint({ user_id: 'user1', checkpoint: 5n, event_id: eventAId });
        writer.addCustomWriteCheckpoint({ user_id: 'user1', checkpoint: 8n, event_id: eventBId });
        await writer.flush();
        await writer.keepalive('5/0');

        const eventACollection = db.customCheckpointRequests({
          replicationStreamId: syncRules.replicationStreamId,
          eventId: eventAId
        });
        const eventBCollection = db.customCheckpointRequests({
          replicationStreamId: syncRules.replicationStreamId,
          eventId: eventBId
        });
        await expect(eventACollection.findOne({ user_id: 'user1' })).resolves.toMatchObject({ checkpoint: 5n });
        await expect(eventBCollection.findOne({ user_id: 'user1' })).resolves.toMatchObject({ checkpoint: 8n });
        const checkpointCollectionNames = (
          await db.listCustomCheckpointRequestCollections(syncRules.replicationStreamId)
        )
          .map((collection) => collection.collectionName)
          .sort();
        expect(checkpointCollectionNames).toEqual(
          [eventACollection.collectionName, eventBCollection.collectionName].sort()
        );
        expect(checkpointCollectionNames).not.toContain(
          db.customCheckpointRequests({
            replicationStreamId: syncRules.replicationStreamId,
            eventId: unrelatedEventId
          }).collectionName
        );
        await expect(eventACollection.indexExists(['user_unique', 'op_id', 'checkpoint_requested_at'])).resolves.toBe(
          true
        );
        await expect(
          bucketStorage.lastWriteCheckpoint({ user_id: 'user1', syncConfig: activeSyncConfig })
        ).resolves.toEqual(5n);
        await expect(iter.next()).resolves.toMatchObject({
          done: false,
          value: { writeCheckpoint: 5n }
        });

        writer.addCustomWriteCheckpoint({ user_id: 'user1', checkpoint: 9n, event_id: eventBId });
        await writer.flush();
        await writer.keepalive('6/0');
        // The processing event's record does not advance clients still served by
        // the active event definition.
        await expect(iter.next()).resolves.toMatchObject({
          done: false,
          value: { writeCheckpoint: 5n }
        });

        bucketStorage.setWriteCheckpointMode({
          mode: storage.WriteCheckpointMode.CUSTOM,
          eventName: eventB.name
        });
        await expect(
          bucketStorage.lastWriteCheckpoint({ user_id: 'user1', syncConfig: activeSyncConfig })
        ).resolves.toEqual(9n);

        writer.addCustomWriteCheckpoint({
          user_id: 'temporary',
          checkpoint: 10n,
          event_id: eventBId,
          checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z')
        });
        await writer.flush();
        await compactActive(factory, {
          compactBuckets: [],
          deleteCheckpointRequestsBefore: new Date('2024-01-02T00:00:00.000Z')
        });
        await expect(eventBCollection.findOne({ user_id: 'temporary' })).resolves.toBeNull();

        // A fresh storage instance has an empty initialization cache, just like
        // one created after a service restart. Recreating the existing indexes
        // must be idempotent before another checkpoint is written.
        const freshBucketStorage = factory.getInstance(syncRules);
        await using freshWriter = await freshBucketStorage.createWriter(test_utils.BATCH_OPTIONS);
        freshWriter.addCustomWriteCheckpoint({
          user_id: 'after-restart',
          checkpoint: 11n,
          event_id: eventAId
        });
        await freshWriter.flush();
        await expect(eventACollection.findOne({ user_id: 'after-restart' })).resolves.toMatchObject({
          checkpoint: 11n
        });

        await bucketStorage.clear();
        await expect(db.listCustomCheckpointRequestCollections(syncRules.replicationStreamId)).resolves.toEqual([]);
      }
    );

    test.runIf(storageVersion >= 3)(
      'resolves custom checkpoints through the active event mapping across deployments',
      async () => {
        await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
        const syncConfigYaml = (predicate: string, includeExtraStream: boolean) => `
config:
  edition: 3

streams:
  global:
    query: SELECT * FROM checkpoints
${includeExtraStream ? '  todos:\n    query: SELECT * FROM todos' : ''}

event_definitions:
  write_checkpoints:
    payloads:
      - SELECT user_id, checkpoint FROM checkpoints WHERE ${predicate}
`;

        const first = await factory.updateSyncRules(
          updateSyncRulesFromYaml(syncConfigYaml("kind = 'write'", false), { storageVersion })
        );
        const firstEventId = first.syncConfigContent[0].mapping.eventDefinitionIdByName('write_checkpoints');
        const firstStorage = factory.getInstance(first);
        await using firstWriter = await firstStorage.createWriter(test_utils.BATCH_OPTIONS);
        firstWriter.addCustomWriteCheckpoint({ user_id: 'user1', checkpoint: 5n, event_id: firstEventId });
        await firstWriter.markAllSnapshotDone('1/1');
        await firstWriter.commit('1/1');

        const unchanged = await factory.updateSyncRules(
          updateSyncRulesFromYaml(syncConfigYaml("kind = 'write'", true), { storageVersion })
        );
        const unchangedEventId = unchanged.syncConfigContent[1].mapping.eventDefinitionIdByName('write_checkpoints');
        expect(unchangedEventId).toBe(firstEventId);

        await using unchangedWriter = await factory.getInstance(unchanged).createWriter(test_utils.BATCH_OPTIONS);
        await unchangedWriter.markAllSnapshotDone('2/1');
        await unchangedWriter.commit('2/1');

        const unchangedActiveStorage = (await factory.getActiveSyncConfig())!.storage as MongoSyncBucketStorage;
        unchangedActiveStorage.setWriteCheckpointMode({
          mode: storage.WriteCheckpointMode.CUSTOM,
          eventName: 'write_checkpoints'
        });
        await expect(
          unchangedActiveStorage.lastWriteCheckpoint({
            user_id: 'user1',
            syncConfig: unchangedActiveStorage.getParsedSyncRules({ defaultSchema: 'public' })
          })
        ).resolves.toBe(5n);

        const changed = await factory.updateSyncRules(
          updateSyncRulesFromYaml(syncConfigYaml("kind = 'processed'", true), { storageVersion })
        );
        const changedEventId = changed.syncConfigContent[1].mapping.eventDefinitionIdByName('write_checkpoints');
        expect(changedEventId).not.toBe(unchangedEventId);

        await using changedWriter = await factory.getInstance(changed).createWriter(test_utils.BATCH_OPTIONS);
        changedWriter.addCustomWriteCheckpoint({ user_id: 'user1', checkpoint: 9n, event_id: changedEventId });
        await changedWriter.flush();

        // Until activation, reads still use the previous config's assigned id and collection.
        await expect(
          unchangedActiveStorage.lastWriteCheckpoint({
            user_id: 'user1',
            syncConfig: unchangedActiveStorage.getParsedSyncRules({ defaultSchema: 'public' })
          })
        ).resolves.toBe(5n);

        await changedWriter.markAllSnapshotDone('3/1');
        await changedWriter.commit('3/1');

        const changedActiveStorage = (await factory.getActiveSyncConfig())!.storage as MongoSyncBucketStorage;
        changedActiveStorage.setWriteCheckpointMode({
          mode: storage.WriteCheckpointMode.CUSTOM,
          eventName: 'write_checkpoints'
        });
        await expect(
          changedActiveStorage.lastWriteCheckpoint({
            user_id: 'user1',
            syncConfig: changedActiveStorage.getParsedSyncRules({ defaultSchema: 'public' })
          })
        ).resolves.toBe(9n);
      }
    );

    /**
     * It's extremely rare (but technically possible) to have duplicate write checkpoint records
     * for a full user_id. This is normally not an issue for checkpoints requested from `write-checkpoint2.json`,
     * but it can be for requests in the `sync/checkpoint-request` flow.
     * That flow seeds the client with the current latest state (if available). We need to ensure that the client
     * does NOT start at a lower bound if there are duplicate records.
     */
    test('updates duplicate managed rows on supplied checkpoint requests', async () => {
      await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
bucket_definitions:
  global:
    data: []
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules);

      // Replicate an extremely rare possibility where there are multiple records
      await factory.db.write_checkpoints.insertMany([
        {
          _id: new mongo.ObjectId('000000000000000000000001'),
          user_id: 'user1',
          client_id: 50n,
          lsns: { '1': '50/0' },
          processed_at_lsn: '50/0'
        },
        {
          _id: new mongo.ObjectId('000000000000000000000002'),
          user_id: 'user1',
          client_id: 42n,
          lsns: { '1': '42/0' },
          processed_at_lsn: null,
          checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z')
        }
      ]);

      // Simulate a client without a sequence state trying to seed its state.
      const seeded = await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '1/0' }, checkpoint_request_id: 1n }
      ]);

      // The result should be the current max state.
      const seededCheckpoint = seeded.writeCheckpoints.get('user1')!;
      expect(seededCheckpoint).toEqual(50n);
      expect(seeded.shouldAdvance).toBe(false);

      // Simulate a client using the above state for its next checkpoint request.
      const nextCheckpoint = seededCheckpoint + 1n;
      const incremented = await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': `${nextCheckpoint}/0` }, checkpoint_request_id: nextCheckpoint }
      ]);

      // This should be accepted by the service.
      expect(incremented.writeCheckpoints.get('user1')).toEqual(nextCheckpoint);
      expect(incremented.shouldAdvance).toBe(true);

      // The service should now reconcile all duplicates to the latest state.
      // Duplicates can now also automatically be deleted when they expire.
      const docs = await factory.db.write_checkpoints.find({ user_id: 'user1' }, { sort: { _id: 1 } }).toArray();
      expect(
        docs.map((doc) => ({
          client_id: doc.client_id,
          lsn: doc.lsns['1'],
          processed_at_lsn: doc.processed_at_lsn,
          is_checkpoint_request: doc.checkpoint_requested_at != null
        }))
      ).toEqual([
        {
          client_id: nextCheckpoint,
          lsn: '51/0',
          processed_at_lsn: null,
          is_checkpoint_request: true
        },
        {
          client_id: nextCheckpoint,
          lsn: '51/0',
          processed_at_lsn: null,
          is_checkpoint_request: true
        }
      ]);
    });
  });
}

describe('Sync Bucket Validation', register.registerBucketValidationTests);

describe('Mongo Sync Bucket Storage - split operations', () =>
  register.registerDataStorageDataTests(
    mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      checksumOptions: {
        bucketBatchLimit: 100,
        operationBatchLimit: 1
      }
    })
  ));

describe('Mongo Sync Bucket Storage - split buckets', () =>
  register.registerDataStorageDataTests(
    mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      checksumOptions: {
        bucketBatchLimit: 1,
        operationBatchLimit: 100
      }
    })
  ));
