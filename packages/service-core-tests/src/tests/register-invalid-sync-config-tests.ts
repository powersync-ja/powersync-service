import {
  AbstractReplicationJob,
  AbstractReplicator,
  AbstractReplicatorOptions,
  BasicRouterRequest,
  Context,
  getInvalidLocalSyncConfigStatus,
  loadSyncConfig,
  routes,
  storage,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
import { describe, expect, test, vi } from 'vitest';
import { BATCH_OPTIONS } from '../test-utils/general-utils.js';
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
  constructor(options: AbstractReplicatorOptions) {
    super(options);
  }

  createJob(): AbstractReplicationJob {
    throw new Error('This suite drives storage snapshots directly');
  }

  async cleanUp(): Promise<void> {}

  async testConnection() {
    return { connectionDescription: 'test' };
  }

  async initializeConfig() {
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
export function registerInvalidSyncConfigTests(factory: storage.TestStorageFactory, storageVersion: number) {
  describe('invalid local sync config lifecycle', () => {
    /**
     * An older service persisted invalid B while valid A was active. After upgrading with
     * the same local B, startup must leave storage intact and retain ownership of B's stream.
     * With incremental storage A and B share that stream, so rejecting it would also block A.
     * This checks compatibility ownership, not successful evaluation of invalid B.
     */
    test('retains ownership of an invalid deployment persisted before upgrade', async () => {
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

      // Bypass the new startup guard to reproduce a deployment written by an older service.
      const invalid = updateSyncRulesFromYaml(INVALID_CONFIG, { storageVersion, version_label: 'rejected' });
      expect(invalid.config.parsed.errors.some((error) => error.type === 'fatal')).toBe(true);
      const persisted = await backend.updateSyncRules(invalid);
      if (storageVersion >= 3) {
        expect(persisted.replicationStreamId).toBe(original.replicationStreamId);
        expect(persisted.syncConfigContent.map((config) => config.syncConfigState)).toEqual([
          storage.SyncRuleState.ACTIVE,
          storage.SyncRuleState.PROCESSING
        ]);
      }
      const loaded = await loadSyncConfig({ present: true, content: INVALID_CONFIG, exit_on_error: false });
      const replicator = new ConfigTestReplicator({
        id: 'upgrade-test',
        configuration: {
          connections: [],
          sync_rules: { present: true, exit_on_error: false, version_label: 'rejected', loaded }
        },
        storageEngine: { activeBucketStorage: backend } as storage.StorageEngine,
        metricsEngine: METRICS_HELPER.metricsEngine,
        rateLimiter: { waitUntilAllowed: async () => {}, reportError: () => {}, mayPing: () => false }
      });
      const configure = vi.spyOn(backend, 'configureSyncRules');
      const pendingBefore = await backend.getDeployingSyncConfig();
      await replicator.initializeConfig();
      expect(configure).not.toHaveBeenCalled();
      expect((await backend.getDeployingSyncConfig())?.content.syncConfigId).toBe(pendingBefore?.content.syncConfigId);
      expect((await backend.getActiveSyncConfig())?.content.sync_rules_content).toBe(valid);
      const stream = (await backend.getReplicatingReplicationStreams()).find(
        (stream) => stream.replicationStreamId === persisted.replicationStreamId
      )!;
      expect(stream).toBeDefined();
      expect(await replicator.accepts(stream)).toBe(true);
      expect(getInvalidLocalSyncConfigStatus(loaded, true, 'rejected')?.content).toBe(INVALID_CONFIG);
    });

    /**
     * Start with valid config A active, then restart a worker with invalid local config B.
     * B must stay out of storage while remaining visible in diagnostics throughout:
     * - automatic recovery of A after source history loss;
     * - an operator rebuilding A through the admin reprocess endpoint;
     * - the equivalent request through the Sync Rules compatibility endpoint.
     * Each path must produce replacement work the worker accepts, activate it, and clean
     * up the old stream without losing B's diagnostics or accepting unrelated deployments.
     */
    test.each(['recovery', 'admin reprocess', 'sync config reprocess'] as const)(
      '%s retains rejected diagnostics through activation and cleanup',
      async (operation) => {
        await using backend = await factory();
        const content = /* yaml */ `
          # Active stored deployment.
          config:
            edition: 3
          streams:
            todos:
              query: SELECT id FROM todos
        `;
        // Select the initial storage version outside the YAML. Reprocessing can then upgrade
        // to the backend default, while recovery preserves the existing storage version.
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
          exit_on_error: false,
          loaded: await loadSyncConfig({ present: true, content: INVALID_CONFIG, exit_on_error: false })
        };
        const storageEngine = { activeBucketStorage: backend } as storage.StorageEngine;
        const replicator = new ConfigTestReplicator({
          id: 'invalid-config-test',
          configuration: { connections: [], sync_rules: settings },
          storageEngine,
          metricsEngine: METRICS_HELPER.metricsEngine,
          rateLimiter: { waitUntilAllowed: async () => {}, reportError: () => {}, mayPing: () => false }
        });
        const configure = vi.spyOn(backend, 'configureSyncRules');
        await replicator.initializeConfig();
        expect(configure).not.toHaveBeenCalled();
        expect(await backend.getDeployingSyncConfig()).toBeNull();
        expect(await replicator.accepts((await backend.getReplicatingReplicationStreams())[0])).toBe(true);
        const assertRejectedDiagnostics = () => {
          const status = getInvalidLocalSyncConfigStatus(settings.loaded, true, settings.version_label);
          expect(status?.content).toBe(INVALID_CONFIG);
          expect(status?.errors.some((error) => error.level === 'fatal')).toBe(true);
        };
        assertRejectedDiagnostics();

        if (operation === 'recovery') {
          // Simulate the storage restart requested after a lost source position. The old
          // active config becomes errored but must still identify the work being recovered.
          await backend.restartReplication(original.replicationStreamId);
        } else {
          // Exercise each public reprocessing handler while B is loaded. Both must rebuild A.
          // Only the source adapter is a fixture; both routes operate on the real backend.
          const context = {
            service_context: {
              configuration: { sync_rules: settings },
              storageEngine,
              routerEngine: {
                getAPI: () => ({
                  getParseSyncRulesOptions: () => ({ defaultSchema: 'public' }),
                  getSourceConfig: async () => ({ id: 'test', tag: 'default' })
                })
              }
            }
          } as unknown as Context;
          const route =
            operation === 'admin reprocess' ? routes.endpoints.reprocess : routes.endpoints.reprocessSyncRules;
          await route.handler({ context, request: {} as BasicRouterRequest, params: {} });
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
        assertRejectedDiagnostics();

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
        assertRejectedDiagnostics();

        // A deployment from another worker must not be mistaken for recovery of the active config.
        const unrelated = await backend.updateSyncRules(
          updateSyncRulesFromYaml(content, { version_label: 'another-deployment', forceNewReplicationStream: true })
        );
        expect(await replicator.accepts(unrelated)).toBe(false);
        assertRejectedDiagnostics();
      }
    );
  });
}
