import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const SYNC_CONFIG = /* yaml */ `
  # Valid Sync Streams deployment.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT * FROM todos
`;

const INVALID_CONFIG = /* yaml */ `
  # A valid stream and a fatal SQL error.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT * FROM todos
    broken:
      query: SELECT FROM
`;

describe.each(TEST_STORAGE_VERSIONS)('invalid Postgres deployment, storage v%i', (storageVersion) => {
  /**
   * Activate A, deploy pending B, then deploy invalid C and invalid D. A keeps advancing
   * checkpoints while B is cancelled and diagnostics selects D. Restoring valid A creates
   * a replacement that activates successfully while retaining the invalid deployment history.
   */
  test('cancels pending work while preserving the active writer and invalid history', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY.factory();
    const first = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion }));
    const bucketStorage = await getTestStorage(factory, first);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');
    await writer.commit('1/1');
    const pending = await factory.updateSyncRules(
      updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion, version_label: 'pending' })
    );
    await factory.updateSyncRules(updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion, version_label: 'bad-1' }));
    const invalid = await factory.updateSyncRules(
      updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion, version_label: 'bad-2' })
    );
    const status = await factory.getSyncConfigStatus();
    expect(status.active?.content.replicationStreamId).toBe(first.replicationStreamId);
    expect(status.next?.content.syncConfigState).toBe(storage.SyncRuleState.INVALID);
    expect(status.next?.content.version_label).toBe('bad-2');
    expect((await factory.getStoppedReplicationStreams()).map((stream) => stream.replicationStreamId)).toContain(
      pending.replicationStreamId
    );
    expect((await factory.getReplicatingReplicationStreams()).map((stream) => stream.replicationStreamId)).toEqual([
      first.replicationStreamId
    ]);
    await writer.commit('2/1');
    expect((await status.active!.storage.getCheckpoint()).lsn).toBe('2/1');

    const restored = await factory.configureSyncRules(updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion }));
    expect(restored.updated).toBe(true);
    expect((await factory.getSyncConfigStatus()).next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
    const nextStorage = await getTestStorage(factory, restored.persisted_sync_rules!);
    await using nextWriter = await nextStorage.createWriter(test_utils.BATCH_OPTIONS);
    await nextWriter.markAllSnapshotDone('3/1');
    await nextWriter.commit('3/1');
    expect((await factory.getSyncConfigStatus()).active?.content.replicationStreamId).toBe(
      restored.persisted_sync_rules!.replicationStreamId
    );
    expect((await factory.getSyncConfigStatus()).next).toBeNull();
    expect((await invalid.syncConfigContent[0].getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
    expect(
      invalid.syncConfigContent[0]
        .parsed({ defaultSchema: 'public' })
        .syncConfigs[0].errors.some((error) => error.type == 'fatal')
    ).toBe(true);
  });

  /**
   * Deploy valid processing A, invalid B, then valid C. B cancels A; terminating C
   * before activation must leave no active or next config and must not redisplay B.
   */
  test('does not redisplay old invalid history when its valid replacement is terminated', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY.factory();
    const pending = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion }));
    await factory.updateSyncRules(updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion }));
    expect((await factory.getStoppedReplicationStreams()).map((stream) => stream.replicationStreamId)).toContain(
      pending.replicationStreamId
    );
    expect(await factory.getReplicatingReplicationStreams()).toEqual([]);
    const replacement = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion }));
    await factory.getInstance(replacement).terminate({ clearStorage: true });
    expect(await factory.getSyncConfigStatus()).toEqual({ active: null, next: null });
  });
});

/**
 * Deploy valid A into empty storage and activate it, then restart its replication.
 * Status must move from processing A to active A, then retain errored A for clients
 * while exposing the new replacement as the next processing config.
 */
test('combined sync config status follows processing, active and errored configs', async () => {
  await using factory = await POSTGRES_STORAGE_FACTORY.factory();
  expect(await factory.getSyncConfigStatus()).toEqual({ active: null, next: null });
  const first = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion: 2 }));
  const processing = await factory.getSyncConfigStatus();
  expect(processing.active).toBeNull();
  expect(processing.next?.content.replicationStreamId).toBe(first.replicationStreamId);
  expect(processing.next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
  const bucketStorage = await getTestStorage(factory, first);
  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/1');
  await writer.commit('1/1');
  expect((await factory.getSyncConfigStatus()).active?.content.replicationStreamId).toBe(first.replicationStreamId);
  expect((await factory.getSyncConfigStatus()).next).toBeNull();
  await factory.restartReplication(first.replicationStreamId);
  const replacement = await factory.getSyncConfigStatus();
  expect(replacement.active?.content.syncConfigState).toBe(storage.SyncRuleState.ERRORED);
  expect(replacement.next?.content.replicationStreamId).not.toBe(first.replicationStreamId);
  expect(replacement.next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
});
