import { MongoBucketStorage } from '@module/storage/MongoBucketStorage.js';
import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, releaseTestStorageLease, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { up as parameterIndexes } from '../../src/migrations/db/migrations/1684951997326-init.js';
import { up as bucketIndexes } from '../../src/migrations/db/migrations/1741697235857-bucket-state-index.js';
import { up as dirtyBucketIndexes } from '../../src/migrations/db/migrations/1760433882550-bucket-state-index2.js';
import { env } from './env.js';

const collectionNames = ['current_data', 'bucket_data', 'bucket_parameters', 'bucket_state', 'source_tables'];
const testStorage = mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI,
  monitorCommands: true,
  supportsMultipleSyncConfigs: true
});
const yaml = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    global:
      query: SELECT id FROM test
`;

async function activate(factory: MongoBucketStorage, version: number, lsn: string) {
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(yaml, { storageVersion: version }));
  const bucketStorage = await getTestStorage(factory, stream);
  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  const sourceTable = await test_utils.resolveTestTable(
    writer,
    'test',
    ['id'],
    testStorage,
    stream.replicationStreamId
  );
  await writer.save({
    sourceTable,
    tag: storage.SaveOperationTag.INSERT,
    after: { id: 'item-1' },
    afterReplicaId: test_utils.rid('item-1')
  });
  await writer.markAllSnapshotDone(lsn);
  await writer.commit(lsn);
  return stream;
}

async function names(factory: MongoBucketStorage) {
  return (await factory.db.db.listCollections({}, { nameOnly: true }).toArray()).map((c) => c.name);
}

async function terminateStopped(factory: MongoBucketStorage) {
  for (const stream of await factory.getStoppedReplicationStreams()) {
    const lock = await stream.lock();
    try {
      await factory.getInstance(stream, { skipLifecycleHooks: true }).terminate({ clearStorage: true });
    } finally {
      await lock.release();
    }
  }
}

describe('v1/v2 collection lifecycle', () => {
  test('migrations and v4 initialization do not create v1/v2 collections', async () => {
    await using factory = await testStorage.factory();
    await factory.db.drop();
    await factory.db.createCheckpointEventsCollection();
    const context = {
      service_context: { configuration: { storage: { type: 'mongodb', uri: env.MONGO_TEST_URL } } }
    } as unknown as Parameters<typeof parameterIndexes>[0];
    for (const migration of [parameterIndexes, bucketIndexes, dirtyBucketIndexes]) await migration(context);
    for (const name of collectionNames) expect(await names(factory)).not.toContain(name);
    await activate(factory, storage.STORAGE_VERSION_4, '1/1');
    for (const name of collectionNames) expect(await names(factory)).not.toContain(name);
  });

  test.each([1, 2])('drops unused v%i collections instead of deleting individual records', async (version) => {
    await using factory = await testStorage.factory();
    const previous = await activate(factory, version, '1/1');
    await releaseTestStorageLease(factory, previous.replicationStreamId);
    expect(await names(factory)).toEqual(expect.arrayContaining(collectionNames));
    const current = await activate(factory, storage.STORAGE_VERSION_4, '2/1');
    const dropped: string[] = [];
    const deleted: string[] = [];
    factory.db.client.on('commandStarted', (event) => {
      if (event.commandName === 'drop') dropped.push(event.command.drop);
      if (event.commandName === 'delete') deleted.push(event.command.delete);
    });
    await terminateStopped(factory);
    expect(dropped).toEqual(expect.arrayContaining(collectionNames));
    expect(deleted.filter((name) => collectionNames.includes(name))).toEqual([]);
    for (const name of collectionNames) expect(await names(factory)).not.toContain(name);
    expect((await factory.db.sync_rules.findOne({ _id: previous.replicationStreamId }))?.state).toBe(
      storage.SyncRuleState.TERMINATED
    );
    expect((await factory.getActiveSyncConfig())?.replicationStream.replicationStreamId).toBe(
      current.replicationStreamId
    );

    const returning = await activate(factory, version, '3/1');
    await releaseTestStorageLease(factory, current.replicationStreamId);
    await terminateStopped(factory);
    expect(await names(factory)).toEqual(expect.arrayContaining(collectionNames));
    expect((await factory.db.bucket_parameters.listIndexes().toArray()).map((index) => index.name)).toContain(
      'lookup1'
    );
    expect((await factory.db.bucket_state.listIndexes().toArray()).map((index) => index.name)).toEqual(
      expect.arrayContaining(['bucket_updates', 'dirty_count'])
    );
    expect(await factory.db.current_data.countDocuments({ '_id.g': returning.replicationStreamId })).toBe(1);
  });

  test.each([storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED])(
    'retains shared collections for a stream in state %s and deletes only stopped stream records',
    async (state) => {
      await using factory = await testStorage.factory();
      const previous = await activate(factory, storage.STORAGE_VERSION_1, '1/1');
      await releaseTestStorageLease(factory, previous.replicationStreamId);
      const current = await activate(factory, storage.STORAGE_VERSION_2, '2/1');
      await factory.db.sync_rules.updateOne({ _id: current.replicationStreamId }, { $set: { state } });
      await terminateStopped(factory);
      expect(await names(factory)).toEqual(expect.arrayContaining(collectionNames));
      expect(await factory.db.current_data.countDocuments({ '_id.g': previous.replicationStreamId })).toBe(0);
      expect(await factory.db.current_data.countDocuments({ '_id.g': current.replicationStreamId })).toBe(1);
      // A missing storage_version also identifies a v1 stream.
      await factory.db.sync_rules.updateOne({ _id: current.replicationStreamId }, { $unset: { storage_version: 1 } });
      expect(await factory.dropUnusedV1StorageCollections()).toBe(false);
    }
  );

  test('cleans up unused collections on startup without a stopped stream', async () => {
    await using factory = await testStorage.factory();
    const previous = await activate(factory, storage.STORAGE_VERSION_2, '1/1');
    await releaseTestStorageLease(factory, previous.replicationStreamId);
    await activate(factory, storage.STORAGE_VERSION_4, '2/1');
    await factory.db.sync_rules.updateOne(
      { _id: previous.replicationStreamId },
      { $set: { state: storage.SyncRuleState.TERMINATED } }
    );
    await using restarted = await testStorage.factory({ doNotClear: true });
    expect(await restarted.getStoppedReplicationStreams()).toEqual([]);
    for (const name of collectionNames) expect(await names(restarted)).not.toContain(name);
    const drop = vi.spyOn(restarted, 'dropUnusedV1StorageCollections');
    expect(await restarted.getStoppedReplicationStreams()).toEqual([]);
    expect(drop).not.toHaveBeenCalled();
  });

  test('retries a partially completed collection drop', async () => {
    await using factory = await testStorage.factory();
    const previous = await activate(factory, storage.STORAGE_VERSION_2, '1/1');
    await releaseTestStorageLease(factory, previous.replicationStreamId);
    await activate(factory, storage.STORAGE_VERSION_4, '2/1');
    const drop = vi.spyOn(factory.db.bucket_parameters, 'drop').mockRejectedValueOnce(new Error('drop interrupted'));
    await expect(terminateStopped(factory)).rejects.toThrow('drop interrupted');
    expect((await factory.db.sync_rules.findOne({ _id: previous.replicationStreamId }))?.state).toBe(
      storage.SyncRuleState.STOP
    );
    await terminateStopped(factory);
    for (const name of collectionNames) expect(await names(factory)).not.toContain(name);
    drop.mockRestore();
  });

  test('serializes collection drops with deployment by another worker', async () => {
    await using factory = await testStorage.factory();
    await using other = await testStorage.factory({ doNotClear: true });
    const entered = Promise.withResolvers<void>();
    const resume = Promise.withResolvers<void>();
    const originalDrop = factory.db.current_data.drop.bind(factory.db.current_data);
    const drop = vi.spyOn(factory.db.current_data, 'drop').mockImplementation(async (options) => {
      entered.resolve();
      await resume.promise;
      return originalDrop(options);
    });
    const cleanup = factory.dropUnusedV1StorageCollections();
    await entered.promise;
    let deployed = false;
    const deployment = other.updateSyncRules(updateSyncRulesFromYaml(yaml, { storageVersion: 2 })).then(() => {
      deployed = true;
    });
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(deployed).toBe(false);
    } finally {
      resume.resolve();
      await cleanup;
      drop.mockRestore();
    }
    await deployment;
    expect(deployed).toBe(true);
  }, 15_000);
});
