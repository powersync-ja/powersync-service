import {
  AbstractReplicationJob,
  AbstractReplicator,
  AbstractReplicatorOptions,
  getInvalidLocalSyncConfigStatus,
  RouteAPI,
  storage,
  SyncConfigEngine,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
import { describe, expect, test, vi } from 'vitest';
import { BATCH_OPTIONS, bucketRequest, getBatchArray, resolveTestTable, rid } from '../test-utils/general-utils.js';
import { getTestStorage, releaseTestStorageLease } from '../test-utils/leased-storage.js';
import { METRICS_HELPER } from '../test-utils/MetricsHelper.js';

const INVALID_CONFIG = /* yaml */ `
  # Rejected local deployment.
  config:
    edition: 3
  streams:
    rejected:
      query: SELECT FROM
`;

/**
 * Exercise startup and ownership synchronously; storage and its lifecycle remain real.
 * Source ingestion is outside this suite, so tests advance snapshots through the real writer.
 */
class ConfigTestReplicator extends AbstractReplicator {
  readonly syncConfigEngine: SyncConfigEngine;
  constructor(options: Omit<AbstractReplicatorOptions, 'syncConfigEngine'>) {
    const syncConfigEngine = new SyncConfigEngine(
      options.configuration.sync_rules,
      () =>
        ({
          getParseSyncRulesOptions: () => ({ defaultSchema: 'public' })
        }) as RouteAPI
    );
    super({ ...options, syncConfigEngine });
    this.syncConfigEngine = syncConfigEngine;
  }

  createJob(): AbstractReplicationJob {
    throw new Error('This suite drives storage snapshots directly');
  }

  async cleanUp(): Promise<void> {}

  async testConnection() {
    return { connectionDescription: 'test' };
  }

  async initializeConfig() {
    // Run startup admission without entering the continuous replication loop.
    const controller = new AbortController();
    controller.abort();
    this['abortController'] = controller;
    await this['runLoop']();
    this['abortController'] = new AbortController();
  }

  accepts(stream: storage.PersistedReplicationStream) {
    return this.shouldHandleReplicationStream(stream, INVALID_CONFIG, 'rejected', true);
  }

  async cleanStopped(stream: storage.PersistedReplicationStream) {
    await this.terminateStoppedReplicationStream(stream, this.storage.getInstance(stream));
  }
}

/**
 * Shared backend coverage for rejected local configs through replacement activation and cleanup.
 */
export function registerInvalidSyncConfigTests(
  config: Pick<storage.TestStorageConfig, 'factory' | 'tableIdStrings'>,
  storageVersion: number
) {
  const { factory } = config;
  describe('invalid local sync config lifecycle', () => {
    /**
     * An older service persisted invalid B while valid A was active. After upgrading with
     * rejected local input, startup supersedes pending work while preserving active A.
     */
    test.each([{ persistedInvalid: false }, { persistedInvalid: true }])(
      'stops pending work on rejection, including previously persisted invalid work: $persistedInvalid',
      async ({ persistedInvalid }) => {
        await using backend = await factory();
        const valid = /* yaml */ `
          # Existing active deployment.
          config:
            edition: 3
          streams:
            todos:
              query: SELECT id FROM todos
        `;
        const original = await backend.updateSyncRules(
          updateSyncRulesFromYaml(valid, { storageVersion, version_label: 'active' })
        );
        const instance = await getTestStorage(backend, original);
        {
          await using writer = await instance.createWriter(BATCH_OPTIONS);
          await writer.markAllSnapshotDone('1/1');
          await writer.commit('1/1');
        }

        // Persist pending work directly, including invalid input an older service could have stored.
        const invalid = updateSyncRulesFromYaml(persistedInvalid ? INVALID_CONFIG : valid + '\n# Pending B', {
          storageVersion,
          version_label: 'rejected'
        });
        expect(invalid.config.parsed.errors.some((error) => error.type === 'fatal')).toBe(persistedInvalid);
        const persisted = await backend.updateSyncRules(invalid);
        // Incremental storage keeps A and B in one stream, so cancellation must preserve A.
        if (storageVersion >= 3) {
          expect(persisted.replicationStreamId).toBe(original.replicationStreamId);
          expect(persisted.syncConfigContent.map((config) => config.syncConfigState)).toEqual([
            storage.SyncRuleState.ACTIVE,
            storage.SyncRuleState.PROCESSING
          ]);
        }
        const replicator = new ConfigTestReplicator({
          id: 'upgrade-test',
          configuration: {
            connections: [],
            sync_rules: { present: true, exit_on_error: false, version_label: 'rejected', content: INVALID_CONFIG }
          },
          storageEngine: { activeBucketStorage: backend } as storage.StorageEngine,
          metricsEngine: METRICS_HELPER.metricsEngine,
          rateLimiter: { waitUntilAllowed: async () => {}, reportError: () => {}, mayPing: () => false }
        });
        const configure = vi.spyOn(backend, 'configureSyncRules');
        const pendingBefore = await backend.getDeployingSyncConfig();
        expect(pendingBefore).not.toBeNull();
        {
          // Keep the old A/B writer open while startup cancels its pending config.
          await using staleWriter = persistedInvalid
            ? undefined
            : await (await getTestStorage(backend, persisted)).createWriter(BATCH_OPTIONS);
          await replicator.initializeConfig();
          // A writer opened before cancellation must not activate the stopped config.
          if (staleWriter) {
            await staleWriter.markAllSnapshotDone('1/2');
            await staleWriter.commit('1/2');
          }
        }
        expect(configure).not.toHaveBeenCalled();
        expect(await backend.getDeployingSyncConfig()).toBeNull();
        expect((await backend.getActiveSyncConfig())?.content.sync_rules_content).toBe(valid);
        const stream = (await backend.getReplicatingReplicationStreams()).find(
          (stream) => stream.replicationStreamId === original.replicationStreamId
        )!;
        expect(stream).toBeDefined();
        expect(await replicator.accepts(stream)).toBe(true);
        const activeStorage = await getTestStorage(backend, stream);
        {
          await using writer = await activeStorage.createWriter(BATCH_OPTIONS);
          const table = await resolveTestTable(writer, 'todos', ['id'], config);
          await writer.save({
            sourceTable: table,
            tag: storage.SaveOperationTag.INSERT,
            after: { id: 'after-cancellation' },
            afterReplicaId: rid('after-cancellation')
          });
          await writer.markAllSnapshotDone('1/3');
          await writer.commit('1/3');
        }
        const active = (await backend.getActiveSyncConfig())!;
        const checkpoint = await activeStorage.getCheckpoint();
        const data = await getBatchArray(
          activeStorage.getBucketDataBatch(checkpoint, [bucketRequest(active.content, 'todos|0[]')])
        );
        expect(data.flatMap((chunk) => chunk.chunkData.data)).toEqual(
          expect.arrayContaining([expect.objectContaining({ op: 'PUT', object_id: 'after-cancellation' })])
        );
        expect(checkpoint.lsn).toBe('1/3');

        expect(
          getInvalidLocalSyncConfigStatus(await replicator.syncConfigEngine.getConfigured(), true, 'rejected')?.content
        ).toBe(INVALID_CONFIG);
      }
    );

    /**
     * Start with valid config A active, then restart a worker with invalid local config B.
     * B must stay out of storage while remaining visible in diagnostics throughout:
     * - automatic recovery of A after source history loss;
     * Recovery must produce replacement work the worker accepts, activate it, and clean
     * up the old stream without losing B's diagnostics or accepting unrelated deployments.
     */
    test.each([
      { recoveryTiming: 'after startup' },
      { recoveryTiming: 'before startup' },
      { recoveryTiming: 'superseded recovery' },
      { recoveryTiming: 'missing recovery' }
    ] as const)(
      'recovery $recoveryTiming retains rejected diagnostics through activation and cleanup',
      async ({ recoveryTiming }) => {
        await using backend = await factory();
        const content = /* yaml */ `
          # Active stored deployment.
          config:
            edition: 3
          streams:
            todos:
              query: SELECT id FROM todos
        `;
        // Set the initial storage version through deployment options, leaving it out of the YAML.
        // Manual reprocessing can then use the backend's current default; recovery keeps the original version.
        const original = await backend.updateSyncRules(
          updateSyncRulesFromYaml(content, { version_label: 'active', storageVersion })
        );
        // Complete an empty snapshot using a real writer to trigger backend activation.
        async function activate(stream: storage.PersistedReplicationStream) {
          const instance = await getTestStorage(backend, stream);
          await using writer = await instance.createWriter(BATCH_OPTIONS);
          await writer.markAllSnapshotDone('1/1');
          await writer.commit('1/1');
        }
        await activate(original);
        const originalActive = await backend.getActiveSyncConfig();
        expect(originalActive?.content.sync_rules_content).toBe(content);

        // Load rejected config B exactly as startup does; it must not replace stored config A.
        const settings = {
          present: true,
          content: INVALID_CONFIG,
          version_label: 'rejected',
          exit_on_error: false
        };
        const storageEngine = { activeBucketStorage: backend } as storage.StorageEngine;
        const replicator = new ConfigTestReplicator({
          id: 'invalid-config-test',
          configuration: { connections: [], sync_rules: settings },
          storageEngine,
          metricsEngine: METRICS_HELPER.metricsEngine,
          rateLimiter: { waitUntilAllowed: async () => {}, reportError: () => {}, mayPing: () => false }
        });
        if (recoveryTiming !== 'after startup') {
          await backend.restartReplication(original.replicationStreamId);
          expect((await backend.getActiveSyncConfig())?.replicationStream.state).toBe(storage.SyncRuleState.ERRORED);
          if (recoveryTiming === 'superseded recovery') {
            // A newer operator deployment can replace recovery before the rejected startup.
            await backend.updateSyncRules(
              updateSyncRulesFromYaml(content + '\n# Pending deployment', {
                storageVersion,
                version_label: 'unrelated',
                forceNewReplicationStream: true
              })
            );
          } else if (recoveryTiming === 'missing recovery') {
            // Leave A errored without pending recovery; startup must restore runnable work.
            await backend.stopProcessingSyncConfigs();
          }
        }
        const configure = vi.spyOn(backend, 'configureSyncRules');
        await replicator.initializeConfig();
        expect(configure).not.toHaveBeenCalled();
        if (recoveryTiming === 'after startup') {
          expect(await backend.getDeployingSyncConfig()).toBeNull();
        }
        const runnable = await backend.getReplicatingReplicationStreams();
        expect(runnable).toHaveLength(1);
        expect(await replicator.accepts(runnable[0])).toBe(true);
        const assertRejectedDiagnostics = async () => {
          const status = getInvalidLocalSyncConfigStatus(
            await replicator.syncConfigEngine.getConfigured(),
            true,
            settings.version_label
          );
          expect(status?.content).toBe(INVALID_CONFIG);
          expect(status?.errors.some((error) => error.level === 'fatal')).toBe(true);
        };
        await assertRejectedDiagnostics();

        if (recoveryTiming === 'after startup') {
          await backend.restartReplication(original.replicationStreamId);
        }
        // A worker holding invalid B must recognize this replacement as recovery of A.
        const pending = await backend.getDeployingSyncConfig();
        expect(pending?.content.sync_rules_content).toBe(content);
        expect(pending?.content.version_label).toBe('active');
        const replacement = (await backend.getReplicatingReplicationStreams()).find(
          (stream) => stream.replicationStreamId !== original.replicationStreamId
        )!;
        expect(replacement).toBeDefined();
        expect(await replicator.accepts(replacement)).toBe(true);
        await assertRejectedDiagnostics();

        // Activation replaces the active identity, but must leave the local rejection visible.
        await activate(replacement);
        expect((await backend.getActiveSyncConfig())?.content.syncConfigId).toBe(pending?.content.syncConfigId);
        expect(await backend.getDeployingSyncConfig()).toBeNull();
        // Release the old job's lease, then let the replicator acquire the cleanup lock and
        // terminate the superseded stream through the real storage implementation.
        await releaseTestStorageLease(backend, original.replicationStreamId);
        const stopped = await backend.getStoppedReplicationStreams();
        expect(stopped.map((stream) => stream.replicationStreamId)).toContain(original.replicationStreamId);
        for (const stream of stopped) {
          await replicator.cleanStopped(stream);
        }
        expect(await backend.getStoppedReplicationStreams()).toEqual([]);
        expect((await backend.getActiveSyncConfig())?.content.sync_rules_content).toBe(content);
        await assertRejectedDiagnostics();

        // A deployment from another worker must not be mistaken for recovery of the active config.
        const unrelated = await backend.updateSyncRules(
          updateSyncRulesFromYaml(content, { version_label: 'another-deployment', forceNewReplicationStream: true })
        );
        expect(await replicator.accepts(unrelated)).toBe(false);
        await assertRejectedDiagnostics();
      }
    );
  });
}
