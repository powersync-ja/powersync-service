import { routes, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import { getTestStorage } from '../test-utils/leased-storage.js';
import * as test_utils from '../test-utils/test-utils-index.js';

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

// Not tagged as yaml: the content is deliberately malformed, which the formatter cannot parse.
const UNPARSEABLE_CONFIG = `
  # Malformed YAML, so no compiled plan exists.
  config:
    edition: 3
  streams: [todos
`;

function invalidOptions(storageVersion: number) {
  const options = updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion, validate: false });
  expect(storage.hasFatalSyncConfigErrors(options)).toBe(true);
  return options;
}

async function activate(
  factory: storage.BucketStorageFactory,
  stream: storage.PersistedReplicationStream,
  lsn: string
) {
  const bucketStorage = await getTestStorage(factory, stream);
  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone(lsn);
  await writer.commit(lsn);
  return bucketStorage;
}

/**
 * Register invalid sync config deployment and recovery tests for a storage implementation.
 */
export function registerInvalidSyncConfigTests(config: storage.TestStorageConfig & { storageVersion: number }) {
  const { storageVersion } = config;

  /**
   * Deploy invalid A into empty storage. Persist its content for diagnostics, but do not
   * replicate it, serve it to clients, or schedule it for stopped-stream cleanup.
   */
  test('persists the first invalid deployment without replication or client visibility', async () => {
    await using factory = await config.factory();
    const invalid = await factory.updateSyncRules(invalidOptions(storageVersion));
    expect(invalid.state).toBe(storage.SyncRuleState.INVALID);
    expect(await factory.getReplicatingReplicationStreams()).toEqual([]);
    expect(await factory.getActiveSyncConfig()).toBeNull();
    expect(await factory.getProcessingSyncConfig()).toBeNull();
    expect(await factory.getStoppedReplicationStreams()).toEqual([]);
    expect(invalid.syncConfigContent[0].sync_rules_content).toBe(INVALID_CONFIG);
  });

  /**
   * Load invalid A from the filesystem, then load identical A again on restart.
   * The second load must reuse the existing deployment instead of creating another attempt.
   */
  test('does not deploy the same invalid filesystem config again on restart', async () => {
    await using factory = await config.factory();
    expect((await factory.configureSyncRules(invalidOptions(storageVersion))).updated).toBe(true);
    expect((await factory.configureSyncRules(invalidOptions(storageVersion))).updated).toBe(false);
  });

  /**
   * Store A as invalid, then load the same file once it parses without fatal errors, as after
   * a service upgrade. The unchanged content must be deployed again instead of staying invalid.
   */
  test('redeploys an unchanged invalid config once it parses without fatal errors', async () => {
    await using factory = await config.factory();
    const valid = updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion });
    // Simulate an older parser that rejected the same content.
    const rejected: storage.UpdateSyncRulesOptions = {
      ...valid,
      config: {
        ...valid.config,
        parsed: { ...valid.config.parsed, errors: invalidOptions(storageVersion).config.parsed.errors }
      }
    };
    await factory.configureSyncRules(rejected);
    expect((await factory.getSyncConfigStatus()).next?.content.syncConfigState).toBe(storage.SyncRuleState.INVALID);
    expect((await factory.configureSyncRules(valid)).updated).toBe(true);
    const next = (await factory.getSyncConfigStatus()).next;
    expect(next?.content.sync_rules_content).toBe(VALID_CONFIG);
    expect(next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
  });

  /**
   * Deploy YAML that fails to parse, so no compiled plan exists. The fatal parse error alone
   * must make the deployment invalid, without a replication lock or replication.
   */
  test('stores a config that fails to parse as invalid', async () => {
    await using factory = await config.factory();
    const options = updateSyncRulesFromYaml(UNPARSEABLE_CONFIG, { storageVersion, validate: false });
    expect(options.config.plan).toBeNull();
    const result = await factory.configureSyncRules({ ...options, lock: true });
    try {
      expect(result.persisted_sync_rules?.state).toBe(storage.SyncRuleState.INVALID);
      expect(result.lock).toBeUndefined();
      expect(await factory.getReplicatingReplicationStreams()).toEqual([]);
      expect((await factory.getSyncConfigStatus()).next?.content.sync_rules_content).toBe(UNPARSEABLE_CONFIG);
    } finally {
      await result.lock?.release();
    }
  });

  /**
   * Deploy valid A without activating it, then deploy invalid B. Stop pending A
   * and leave no active or replicating config until a valid replacement is deployed.
   */
  test('stops pending processing work when an invalid config is deployed', async () => {
    await using factory = await config.factory();
    const pending = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    await factory.updateSyncRules(invalidOptions(storageVersion));
    expect((await factory.getStoppedReplicationStreams()).map((stream) => stream.replicationStreamId)).toContain(
      pending.replicationStreamId
    );
    expect(
      (await factory.getReplicatingReplicationStreams()).map((stream) => stream.replicationStreamId)
    ).not.toContain(pending.replicationStreamId);
    expect(await factory.getActiveSyncConfig()).toBeNull();
  });

  /**
   * Deploy invalid A, then valid B into storage with no active config. B replaces A
   * in diagnostics while processing; after B activates there is no next deployment.
   */
  test('hides standalone invalid history after a valid first config activates', async () => {
    await using factory = await config.factory();
    await factory.updateSyncRules(invalidOptions(storageVersion));
    expect((await factory.getSyncConfigStatus()).next).not.toBeNull();
    const next = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    expect((await factory.getSyncConfigStatus()).next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
    await activate(factory, next, '1/1');
    expect((await factory.getSyncConfigStatus()).next).toBeNull();
  });

  /**
   * Deploy invalid A, invalid B, then valid C without activating any config.
   * Each replacement stops the previous invalid attempt, and only the latest is next.
   */
  test('stops each superseded invalid deployment and exposes only its replacement', async () => {
    await using factory = await config.factory();
    const first = await factory.updateSyncRules({ ...invalidOptions(storageVersion), version_label: 'bad-1' });
    const second = await factory.updateSyncRules({ ...invalidOptions(storageVersion), version_label: 'bad-2' });
    expect((await first.syncConfigContent[0].getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
    expect((await factory.getSyncConfigStatus()).next?.content.version_label).toBe('bad-2');

    await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    expect((await second.syncConfigContent[0].getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
    expect((await factory.getSyncConfigStatus()).next?.content.sync_rules_content).toBe(VALID_CONFIG);
    expect(await factory.getActiveSyncConfig()).toBeNull();
  });

  /**
   * Deploy invalid A, activate valid B, then terminate B and clear its storage.
   * Historical A must not reappear as the next deployment after B is gone.
   */
  test('does not revive invalid history after the newer stream terminates', async () => {
    await using factory = await config.factory();
    await factory.updateSyncRules(invalidOptions(storageVersion));
    const next = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    const bucketStorage = await activate(factory, next, '1/1');
    await bucketStorage.terminate({ clearStorage: true });
    expect(await factory.getSyncConfigStatus()).toEqual({ active: null, next: null });
  });

  /**
   * Load invalid A from the filesystem with locking requested. Since A cannot replicate,
   * startup must not acquire a replication lock that no replication job can release.
   * Storage that never locks during deployment passes trivially.
   */
  test('does not allocate an unclaimed replication lock for an invalid filesystem deployment', async () => {
    await using factory = await config.factory();
    const result = await factory.configureSyncRules({ ...invalidOptions(storageVersion), lock: true });
    try {
      expect(await factory.getReplicatingReplicationStreams()).toEqual([]);
      expect(result.lock).toBeUndefined();
    } finally {
      await result.lock?.release();
    }
  });

  /**
   * Deploy invalid A with no active config, then request diagnostics with config content.
   * Return A, its version label and its persisted validation error through deploying_sync_rules
   * without starting replication.
   */
  test('exposes persisted fatal errors through diagnostics with no active config', async () => {
    await using factory = await config.factory();
    await factory.updateSyncRules({ ...invalidOptions(storageVersion), version_label: 'invalid-1' });
    const response = await routes.endpoints.diagnostics.handler({
      context: {
        service_context: {
          storageEngine: { activeBucketStorage: factory },
          routerEngine: {
            getAPI: () => ({
              getConnectionStatus: async () => ({
                connected: false,
                uri: 'test',
                id: '1',
                tag: 'default',
                errors: []
              }),
              getSourceConfig: async () => ({ id: '1', tag: 'default' }),
              getParseSyncRulesOptions: () => ({ defaultSchema: 'public' })
            })
          }
        }
      },
      params: { sync_rules_content: true },
      request: {}
    } as any);
    expect(response.deploying_sync_rules?.content).toBe(INVALID_CONFIG);
    expect(response.deploying_sync_rules?.version_label).toBe('invalid-1');
    // The disconnected source also reports fatal 'connection failed' errors, so match the parse error itself.
    expect(response.deploying_sync_rules?.errors).toContainEqual(
      expect.objectContaining({ level: 'fatal', message: 'Unexpected end of input' })
    );
  });

  /**
   * Deploy invalid A into empty storage, then deploy and activate valid B in a new stream.
   * Only B replicates and serves clients; A remains recorded in the stopped state.
   */
  test('recovers from an invalid first deployment with a valid replacement', async () => {
    await using factory = await config.factory();
    const invalid = await factory.updateSyncRules(invalidOptions(storageVersion));
    const valid = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    expect(valid.replicationStreamId).not.toBe(invalid.replicationStreamId);
    expect((await factory.getReplicatingReplicationStreams()).map((stream) => stream.replicationStreamId)).toEqual([
      valid.replicationStreamId
    ]);
    await activate(factory, valid, '1/1');
    expect((await factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(VALID_CONFIG);
    expect((await invalid.syncConfigContent[0].getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
  });

  /**
   * Activate A, load invalid B from the filesystem, then restore A as the filesystem config.
   * Treat the restored content as a new deployment that supersedes B, even though it matches A.
   */
  test('restoring the active filesystem config supersedes an invalid attempt', async () => {
    await using factory = await config.factory();
    const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
    await activate(factory, first, '1/1');
    await factory.configureSyncRules(invalidOptions(storageVersion));
    expect((await factory.getSyncConfigStatus()).next).not.toBeNull();
    expect((await factory.configureSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }))).updated).toBe(
      true
    );
    const status = await factory.getSyncConfigStatus();
    expect(status.next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
    expect(status.active?.content.sync_rules_content).toBe(VALID_CONFIG);
    expect(status.next?.content.sync_rules_content).toBe(VALID_CONFIG);
  });

  /**
   * Recover A after source history is lost while invalid B remains the filesystem deployment.
   * B must survive repeated recovery, startup, activation and cleanup until an operator deploys C.
   */
  test.each([false, true])(
    'preserves invalid diagnostics throughout automatic recovery (separate invalid stream: %s)',
    async (forceNewReplicationStream) => {
      await using factory = await config.factory();
      const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
      await activate(factory, first, '1/1');
      const rejected = { ...invalidOptions(storageVersion), version_label: 'bad', forceNewReplicationStream };
      await factory.configureSyncRules(rejected);
      const invalid = (await factory.getSyncConfigStatus()).next!.content;

      await factory.restartReplication(first.replicationStreamId);
      const recovery = (await factory.getProcessingSyncConfig())!;
      expect(recovery.content.sync_rules_content).toBe(VALID_CONFIG);
      expect((await factory.getSyncConfigStatus()).active?.content.syncConfigState).toBe(storage.SyncRuleState.ERRORED);
      expect((await factory.getSyncConfigStatus()).next?.content.syncConfigState).toBe(storage.SyncRuleState.INVALID);
      expect((await factory.configureSyncRules(rejected)).updated).toBe(false);
      expect((await factory.getProcessingSyncConfig())?.content.replicationStreamId).toBe(
        recovery.content.replicationStreamId
      );

      await factory.restartReplication(recovery.content.replicationStreamId);
      const retry = (await factory.getProcessingSyncConfig())!;
      expect(retry.content.replicationStreamId).not.toBe(recovery.content.replicationStreamId);
      await activate(factory, retry.replicationStream, '2/1');
      for (const stopped of await factory.getStoppedReplicationStreams()) {
        const stoppedStorage = await getTestStorage(factory, stopped);
        await stoppedStorage.terminate({ clearStorage: true });
      }

      const status = await factory.getSyncConfigStatus();
      expect(status.active?.content.sync_rules_content).toBe(VALID_CONFIG);
      expect(status.active?.content.replicationStreamId).toBe(retry.content.replicationStreamId);
      expect(status.next?.content.sync_rules_content).toBe(INVALID_CONFIG);
      expect(status.next?.content.syncConfigId).toBe(invalid.syncConfigId);
      expect(status.next?.content.replicationStreamId).toBe(invalid.replicationStreamId);
      expect(status.next?.content.syncConfigState).toBe(storage.SyncRuleState.INVALID);
      expect(
        status.next?.content
          .parsed({ defaultSchema: 'public' })
          .syncConfigs[0].errors.some((error) => error.type == 'fatal')
      ).toBe(true);
      expect((await factory.configureSyncRules(rejected)).updated).toBe(false);
      expect(await factory.getProcessingSyncConfig()).toBeNull();

      await factory.configureSyncRules(updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion }));
      expect((await factory.getSyncConfigStatus()).next?.content.sync_rules_content).toBe(CHANGED_CONFIG);
      expect((await invalid.getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
    }
  );

  describe.runIf(storageVersion >= 3)('incremental invalid sync config', () => {
    /**
     * Activate A, append invalid B, then append and activate changed valid C in the same stream.
     * A must keep replicating and advancing checkpoints while B is ignored and C is pending;
     * C becomes client-visible only after activation.
     */
    test('keeps the active writer and checkpoint working, then activates a changed config', async () => {
      await using factory = await config.factory();
      const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
      const firstStorage = await activate(factory, first, '1/1');
      const before = (await factory.getReplicatingReplicationStreams())[0];
      const invalid = await factory.updateSyncRules(invalidOptions(storageVersion));
      const invalidContent = invalid.syncConfigContent.find((config) => config.sync_rules_content == INVALID_CONFIG)!;
      expect(invalid.replicationStreamId).toBe(first.replicationStreamId);
      expect(invalidContent.syncConfigState).toBe(storage.SyncRuleState.INVALID);

      const after = await factory.getReplicatingReplicationStreams();
      expect(after).toHaveLength(1);
      expect(after[0].replicationJobId).toBe(before.replicationJobId);
      expect(after[0].syncConfigContent.map((config) => config.sync_rules_content)).toEqual([VALID_CONFIG]);
      expect(await factory.getProcessingSyncConfig()).toBeNull();
      await using writer = await firstStorage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.commit('2/1');
      expect((await factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(VALID_CONFIG);
      expect((await factory.getActiveSyncConfig())?.storage).toBeDefined();
      expect((await (await factory.getActiveSyncConfig())!.storage.getCheckpoint()).lsn).toBe('2/1');

      const replacement = await factory.updateSyncRules(updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion }));
      expect(replacement.replicationStreamId).toBe(first.replicationStreamId);
      expect((await factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(VALID_CONFIG);
      expect((await factory.getProcessingSyncConfig())?.content.sync_rules_content).toBe(CHANGED_CONFIG);
      const replicating = (await factory.getReplicatingReplicationStreams())[0];
      expect(replicating.syncConfigContent.map((config) => config.sync_rules_content)).not.toContain(INVALID_CONFIG);
      await activate(factory, replicating, '3/1');
      expect((await factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(CHANGED_CONFIG);
      expect(await factory.getProcessingSyncConfig()).toBeNull();
    });

    /**
     * Activate A, append valid processing B, then deploy invalid C. Stop B while retaining
     * A as the active config with the same replication job and advancing checkpoints.
     */
    test('stops a pending incremental replacement while the active config continues after an invalid deployment', async () => {
      await using factory = await config.factory();
      const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
      const activeStorage = await activate(factory, first, '1/1');
      const activeJobId = (await factory.getReplicatingReplicationStreams())[0].replicationJobId;
      await factory.updateSyncRules(updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion }));
      const pending = (await factory.getProcessingSyncConfig())!.content;
      await factory.updateSyncRules(invalidOptions(storageVersion));
      expect((await factory.getActiveSyncConfig())?.content.sync_rules_content).toBe(VALID_CONFIG);
      expect(await factory.getProcessingSyncConfig()).toBeNull();
      expect((await pending.getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
      expect((await factory.getReplicatingReplicationStreams())[0].replicationJobId).toBe(activeJobId);
      await using writer = await activeStorage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.commit('2/1');
      expect((await (await factory.getActiveSyncConfig())!.storage.getCheckpoint()).lsn).toBe('2/1');
    });

    /**
     * Activate A, deploy invalid B and invalid C, then deploy and activate changed valid D.
     * Exercise both appended and separate-stream invalid attempts: only C appears as next
     * until D replaces it, and historical C remains stopped after D activates.
     */
    test.each([false, true])(
      'returns only the latest invalid attempt (separate stream: %s)',
      async (forceNewReplicationStream) => {
        await using factory = await config.factory();
        expect(await factory.getSyncConfigStatus()).toEqual({ active: null, next: null });
        const first = await factory.updateSyncRules(updateSyncRulesFromYaml(VALID_CONFIG, { storageVersion }));
        const initial = await factory.getSyncConfigStatus();
        expect(initial.active).toBeNull();
        expect(initial.next?.content.sync_rules_content).toBe(VALID_CONFIG);
        expect(initial.next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
        await activate(factory, first, '1/1');

        await factory.updateSyncRules({
          ...invalidOptions(storageVersion),
          forceNewReplicationStream,
          version_label: 'bad-1'
        });
        await factory.updateSyncRules({
          ...invalidOptions(storageVersion),
          forceNewReplicationStream,
          version_label: 'bad-2'
        });
        const invalid = await factory.getSyncConfigStatus();
        expect(invalid.active?.content.sync_rules_content).toBe(VALID_CONFIG);
        expect(invalid.next?.content.version_label).toBe('bad-2');
        expect(invalid.next?.content.syncConfigState).toBe(storage.SyncRuleState.INVALID);
        expect(await factory.getProcessingSyncConfig()).toBeNull();
        expect((await factory.getReplicatingReplicationStreams())[0].syncConfigContent).toHaveLength(1);

        await factory.updateSyncRules(updateSyncRulesFromYaml(CHANGED_CONFIG, { storageVersion }));
        const processing = await factory.getSyncConfigStatus();
        expect(processing.active?.content.sync_rules_content).toBe(VALID_CONFIG);
        expect(processing.next?.content.sync_rules_content).toBe(CHANGED_CONFIG);
        expect(processing.next?.content.syncConfigState).toBe(storage.SyncRuleState.PROCESSING);
        await activate(factory, (await factory.getReplicatingReplicationStreams())[0], '2/1');
        const active = await factory.getSyncConfigStatus();
        expect(active.active?.content.sync_rules_content).toBe(CHANGED_CONFIG);
        expect(active.next).toBeNull();
        expect((await invalid.next!.content.getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
      }
    );
  });
}
