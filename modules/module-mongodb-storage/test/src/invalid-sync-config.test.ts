import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, register, test_utils } from '@powersync/service-core-tests';
import { ObjectId } from 'bson';
import { describe, expect, test } from 'vitest';
import { ReplicationStreamDocumentV3 } from '../../src/storage/implementation/v3/models.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const VALID_CONFIG = /* yaml */ `
  # Baseline valid deployment.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT * FROM todos
`;

const INVALID_CONFIG = /* yaml */ `
  # Valid stream plus a fatal SQL error.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT * FROM todos
    broken:
      query: SELECT FROM
`;

const CHANGED_CONFIG = /* yaml */ `
  # Valid deployment adding a stream.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT * FROM todos
    projects:
      query: SELECT * FROM projects
`;

function invalidOptions(storageVersion: number) {
  const options = updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion, validate: false });
  expect(storage.hasFatalSyncConfigErrors(options)).toBe(true);
  return options;
}

async function activate(factory: MongoBucketStorage, stream: storage.PersistedReplicationStream, lsn: string) {
  const bucketStorage = await getTestStorage(factory, stream);
  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone(lsn);
  await writer.commit(lsn);
  return bucketStorage;
}

describe.each([1, 2, 3, 4])('invalid sync config, storage v%i', (storageVersion) => {
  register.registerInvalidSyncConfigTests({ ...INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion });
});

describe.each([3, 4])('invalid sync config history, storage v%i', (storageVersion) => {
  /**
   * Activate A, then deploy invalid B. Deploy valid C, which marks B as stopped.
   * Stop C before it activates, then run cleanup to remove B and C from the stream's state.
   * Status must still return A as active and null as next. B's historical definition remains
   * stored, but removing C must not cause B to appear in diagnostics again.
   */
  test('does not revive invalid history after the latest stopped config is pruned', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    const bucketStorage = await activate(factory, first, '1/1');
    await factory.updateSyncRules(invalidOptions(storageVersion));
    await factory.updateSyncRules(updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion }));
    const pending = (await factory.getProcessingSyncConfig())!.content;
    await factory.db.sync_rules.updateOne(
      { _id: first.replicationStreamId },
      { $set: { 'sync_configs.$[config].state': storage.SyncRuleState.STOP } },
      { arrayFilters: [{ 'config._id': new ObjectId(pending.syncConfigId!) }] }
    );
    expect((await factory.getSyncConfigStatus()).next).toBeNull();
    await (bucketStorage as MongoSyncBucketStorageV3).cleanupStoppedSyncConfigs({
      defaultSchema: 'public',
      sourceConnectionTag: storage.SourceTable.DEFAULT_TAG
    });
    const doc = await factory.db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: first.replicationStreamId });
    expect(doc!.sync_configs.some((config) => config._id.toHexString() == pending.syncConfigId)).toBe(false);
    expect(doc!.sync_configs.some((config) => config.state == storage.SyncRuleState.INVALID)).toBe(false);
    const status = await factory.getSyncConfigStatus();
    expect(status.active?.content.sync_rules_content).toBe(VALID_CONFIG);
    expect(status.next).toBeNull();
  });

  /**
   * Activate A, append invalid B, then append and activate changed valid C. After cleanup
   * prunes B from the stream state, its definition and fatal errors must remain stored.
   */
  test('retains invalid history after activating a changed config and cleaning up stopped configs', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    await activate(factory, first, '1/1');
    const invalid = await factory.updateSyncRules(invalidOptions(storageVersion));
    const invalidContent = invalid.syncConfigContent.find((config) => config.sync_rules_content == INVALID_CONFIG)!;
    await factory.updateSyncRules(updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion }));
    const replicating = (await factory.getReplicatingReplicationStreams())[0];
    const replacementStorage = await activate(factory, replicating, '3/1');
    await (replacementStorage as MongoSyncBucketStorageV3).cleanupStoppedSyncConfigs({
      defaultSchema: 'public',
      sourceConnectionTag: storage.SourceTable.DEFAULT_TAG
    });

    const doc = await factory.db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: first.replicationStreamId });
    expect(doc!.sync_configs.find((config) => config._id.toHexString() == invalidContent.syncConfigId)).toBeUndefined();
    const versioned = factory.db.versioned(first.getStorageConfig()) as VersionedPowerSyncMongoV3;
    const history = await versioned.syncConfigDefinitions.findOne({ content: INVALID_CONFIG });
    expect(history?.serialized_plan?.errors?.some((error) => error.level == 'fatal')).toBe(true);
  });

  /**
   * Activate A, append invalid B, then activate valid C in a separate stream. Terminate
   * and clear the old stream; B must retain its definition, errors, and original stream ID.
   */
  test('retains invalid definition history after replacing and terminating its stream', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    await activate(factory, first, '1/1');
    await factory.updateSyncRules(invalidOptions(storageVersion));
    const next = await factory.updateSyncRules(
      updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion, forceNewReplicationStream: true })
    );
    await activate(factory, next, '2/1');
    const stopped = (await factory.getStoppedReplicationStreams())[0];
    const stoppedStorage = await getTestStorage(factory, stopped);
    await stoppedStorage.terminate({ clearStorage: true });
    const versioned = factory.db.versioned(first.getStorageConfig()) as VersionedPowerSyncMongoV3;
    const history = await versioned.syncConfigDefinitions.findOne({ content: INVALID_CONFIG });
    expect(history?.replication_stream_id).toBe(first.replicationStreamId);
    expect(history?.serialized_plan?.errors?.some((error) => error.level == 'fatal')).toBe(true);
  });
});
