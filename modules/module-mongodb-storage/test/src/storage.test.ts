import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
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

      await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '6/0' }, checkpoint_request_id: 41n }
      ]);
      const stale = await factory.db.write_checkpoints.findOne({ user_id: 'user1' });
      expect(stale?.checkpoint_requested_at).toEqual(requested?.checkpoint_requested_at);

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
      await expect(factory.db.write_checkpoints.findOne({ user_id: 'user2' })).resolves.toBeNull();

      bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);
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
    });

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
