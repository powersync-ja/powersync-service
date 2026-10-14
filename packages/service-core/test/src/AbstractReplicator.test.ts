import { getInvalidLocalSyncConfigStatus } from '@/api/diagnostics.js';
import type { RouteAPI } from '@/api/RouteAPI.js';
import { assertSourceCapabilities } from '@/api/source-capabilities.js';
import { BasicRouterRequest, Context, storage } from '@/index.js';
import { AbstractReplicationJob } from '@/replication/AbstractReplicationJob.js';
import { AbstractReplicator, AbstractReplicatorOptions, CreateJobOptions } from '@/replication/AbstractReplicator.js';
import { reprocess } from '@/routes/endpoints/admin.js';
import { reprocessSyncRules } from '@/routes/endpoints/sync-rules.js';
import { PersistedReplicationStream } from '@/storage/PersistedReplicationStream.js';
import type { ReplicationLock } from '@/storage/ReplicationLock.js';
import { SyncRulesBucketStorage } from '@/storage/SyncRulesBucketStorage.js';
import { LoadedSyncConfig, loadSyncConfig } from '@/util/config/sync-rules/LoadedSyncConfig.js';
import { describe, expect, it, vi } from 'vitest';
import { mockServiceContext } from './routes/mocks.js';

class TestReplicator extends AbstractReplicator {
  constructor(
    private readonly cleanup: (storage: SyncRulesBucketStorage) => Promise<void>,
    options: AbstractReplicatorOptions = {
      id: 'test',
      storageEngine: {} as AbstractReplicatorOptions['storageEngine'],
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      configuration: { connections: [], sync_rules: { present: false, exit_on_error: true } },
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    }
  ) {
    super(options);
  }

  createJob(_options: CreateJobOptions): AbstractReplicationJob {
    throw new Error('Not implemented');
  }

  cleanUp(storage: SyncRulesBucketStorage): Promise<void> {
    return this.cleanup(storage);
  }

  async testConnection() {
    return { connectionDescription: 'test' };
  }

  terminateStoppedStream(
    replicationStream: PersistedReplicationStream,
    syncRuleStorage: SyncRulesBucketStorage
  ): Promise<void> {
    return this.terminateStoppedReplicationStream(replicationStream, syncRuleStorage);
  }

  addClearingJob(replicationStreamId: number, promise: Promise<void>): void {
    this.clearingJobs.set(replicationStreamId, promise);
  }

  get heartbeatIntervalNanosForTest(): bigint | null {
    return (this as any).heartbeatIntervalNanos;
  }

  async runStartupForTest(): Promise<void> {
    const controller = new AbortController();
    controller.abort();
    (this as any).abortController = controller;
    await (this as any).runLoop();
  }

  async refreshForTest(configuredLock?: ReplicationLock): Promise<{ replicationJobStarted: boolean }> {
    (this as any).abortController = new AbortController();
    return (this as any).refresh({
      configuredLock,
      loadedVersionLabel: undefined
    });
  }

  shouldHandleStreamForTest(
    replicationStream: PersistedReplicationStream,
    loadedSyncRules: string | undefined,
    loadedVersionLabel: string | undefined,
    invalid = false
  ): Promise<boolean> {
    return (this as any).shouldHandleReplicationStream(replicationStream, loadedSyncRules, loadedVersionLabel, invalid);
  }

  get loggerForTest() {
    return this.logger;
  }
}

describe('AbstractReplicator startup sync config', () => {
  it.each([true, false])('respects exit_on_error=%s for unsupported file-loaded filters', async (exitOnError) => {
    const yaml = /* yaml */ `
      # sync config fixture.
      config:
        edition: 3
        source_table_options:
          orders:
            mongodb_filter_expression: { $eq: ['$$doc.active', true] }
      streams:
        orders:
          query: SELECT * FROM orders
    `;
    const configureSyncRules = vi.fn(
      async (_options: import('@/storage/BucketStorageFactory.js').UpdateSyncRulesOptions) => ({ updated: false })
    );
    const replicator = new TestReplicator(async () => {}, {
      id: 'test',
      storageEngine: {
        activeBucketStorage: { configureSyncRules }
      } as unknown as AbstractReplicatorOptions['storageEngine'],
      configuration: {
        connections: [],
        sync_rules: {
          present: true,
          exit_on_error: exitOnError,
          loaded: await loadSyncConfig({ present: true, content: yaml, exit_on_error: exitOnError }, [
            { type: 'postgresql' }
          ])
        }
      },
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    });
    const loaded = (replicator as any).options.configuration.sync_rules.loaded;
    expect(getInvalidLocalSyncConfigStatus(loaded, true)?.errors).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          level: 'fatal',
          message: expect.stringContaining('only supported by MongoDB sources')
        })
      ])
    );
    if (exitOnError) {
      await expect(replicator.runStartupForTest()).rejects.toThrow('only supported by MongoDB sources');
      expect(configureSyncRules).not.toHaveBeenCalled();
    } else {
      await expect(replicator.runStartupForTest()).resolves.toBeUndefined();
      expect(configureSyncRules).not.toHaveBeenCalled();
    }
  });

  it.each(
    [true, false].flatMap((exitOnError) =>
      (['warning', 'valid', 'unexpected error'] as const).map((result) => ({ exitOnError, result }))
    )
  )(
    'checks source capabilities before persistence: $result, exit_on_error=$exitOnError',
    async ({ result, exitOnError }) => {
      const yaml = /* yaml */ `
        # sync config fixture.
        config:
          edition: 3
        streams:
          orders:
            query: SELECT * FROM orders
      `;
      const calls: string[] = [];
      const configureSyncRules = vi.fn(async () => {
        calls.push('persist');
        return { updated: false };
      });
      const replicator = new TestReplicator(async () => {}, {
        id: 'test',
        storageEngine: {
          activeBucketStorage: { configureSyncRules }
        } as unknown as AbstractReplicatorOptions['storageEngine'],
        configuration: {
          connections: [],
          sync_rules: {
            present: true,
            exit_on_error: exitOnError,
            loaded: await loadSyncConfig({ present: true, content: yaml, exit_on_error: exitOnError }, [
              { type: 'postgresql' }
            ])
          }
        },
        metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
        rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
      });
      replicator.registerSourceCapabilitiesAssertion((config) =>
        assertSourceCapabilities(
          {
            async validateSourceCapabilities() {
              calls.push('validate');
              if (result === 'unexpected error') {
                throw new Error('Source validation failed');
              }
              return result === 'warning' ? [{ level: 'warning', message: 'Missing index' }] : [];
            }
          } as unknown as RouteAPI,
          config
        )
      );

      if (result === 'unexpected error' && exitOnError) {
        await expect(replicator.runStartupForTest()).rejects.toThrow('Source validation failed');
        expect(calls).toEqual(['validate']);
      } else {
        await replicator.runStartupForTest();
        expect(calls).toEqual(['validate', 'persist']);
      }
    }
  );

  it.each([true, false])('retains source-table options with exit_on_error=%s', async (exitOnError) => {
    const yaml = /* yaml */ `
      {
        config: { edition: 3, source_table_options: { orders: { mongodb_filter_expression: 'disabled' } } },
        streams: { orders: { query: SELECT * FROM orders } }
      }
    `;
    const configureSyncRules = vi.fn(async () => ({ updated: false }));
    const replicator = new TestReplicator(async () => {}, {
      id: 'test',
      storageEngine: {
        activeBucketStorage: { configureSyncRules }
      } as unknown as AbstractReplicatorOptions['storageEngine'],
      configuration: {
        connections: [],
        sync_rules: {
          present: true,
          exit_on_error: exitOnError,
          version_label: 'v2',
          loaded: await loadSyncConfig({ present: true, content: yaml, exit_on_error: exitOnError })
        }
      },
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    });

    await replicator.runStartupForTest();

    expect(configureSyncRules).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        lock: true,
        version_label: 'v2',
        config: expect.objectContaining({
          parsed: expect.objectContaining({
            errors: [],
            config: expect.objectContaining({
              plan: expect.objectContaining({
                sourceTableConfig: { orders: { mongodb_filter_expression: 'disabled' } }
              })
            })
          }),
          plan: expect.objectContaining({
            plan: expect.objectContaining({
              version: 3,
              sourceTableConfig: { orders: { mongodb_filter_expression: 'disabled' } }
            })
          })
        })
      })
    );
  });
});

describe('AbstractReplicator heartbeat interval', () => {
  const options: AbstractReplicatorOptions = {
    id: 'test',
    storageEngine: {} as AbstractReplicatorOptions['storageEngine'],
    metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
    configuration: { connections: [], sync_rules: { present: false, exit_on_error: true } },
    rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
  };

  it.each([undefined, null])('uses the default for %s', (heartbeatIntervalSeconds) => {
    const replicator = new TestReplicator(async () => {}, { ...options, heartbeatIntervalSeconds });

    expect(replicator.heartbeatIntervalNanosForTest).toBe(60_000_000_000n);
  });

  it('disables the heartbeat interval with 0', () => {
    const replicator = new TestReplicator(async () => {}, { ...options, heartbeatIntervalSeconds: 0 });

    expect(replicator.heartbeatIntervalNanosForTest).toBeNull();
  });

  it('converts a positive heartbeat interval to nanoseconds', () => {
    const replicator = new TestReplicator(async () => {}, { ...options, heartbeatIntervalSeconds: 5 });

    expect(replicator.heartbeatIntervalNanosForTest).toBe(5_000_000_000n);
  });
});

describe('AbstractReplicator stopped stream cleanup', () => {
  it('holds the replication stream lock across source and storage cleanup', async () => {
    const calls: string[] = [];
    const release = vi.fn(async () => {
      calls.push('release');
    });
    const replicationStream = {
      async lock() {
        calls.push('lock');
        return { sync_rules_id: 1, release };
      }
    } as unknown as PersistedReplicationStream;
    const syncRuleStorage = {
      logger: { info: vi.fn() },
      async terminate() {
        calls.push('terminate');
      }
    } as unknown as SyncRulesBucketStorage;
    const replicator = new TestReplicator(async () => {
      calls.push('cleanup');
    });

    await replicator.terminateStoppedStream(replicationStream, syncRuleStorage);

    expect(calls).toEqual(['lock', 'cleanup', 'terminate', 'release']);
    expect(release).toHaveBeenCalledOnce();
  });

  it('releases the replication stream lock when cleanup fails', async () => {
    const cleanupError = new Error('cleanup failed');
    const release = vi.fn(async () => {});
    const replicationStream = {
      async lock() {
        return { sync_rules_id: 1, release };
      }
    } as unknown as PersistedReplicationStream;
    const terminate = vi.fn(async () => {});
    const syncRuleStorage = {
      logger: { info: vi.fn() },
      terminate
    } as unknown as SyncRulesBucketStorage;
    const replicator = new TestReplicator(async () => {
      throw cleanupError;
    });

    await expect(replicator.terminateStoppedStream(replicationStream, syncRuleStorage)).rejects.toBe(cleanupError);

    expect(terminate).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalledOnce();
  });

  it('waits for stopped stream cleanup when stopping', async () => {
    let finishCleanup: () => void;
    const cleanup = new Promise<void>((resolve) => {
      finishCleanup = resolve;
    });
    const replicator = new TestReplicator(async () => {});
    replicator.addClearingJob(1, cleanup);

    let stopped = false;
    const stop = replicator.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    expect(stopped).toBe(false);

    finishCleanup!();
    await stop;
    expect(stopped).toBe(true);
  });
});

describe('AbstractReplicator rolling deployment config matching', () => {
  const processingStream = (version_label: string | undefined): PersistedReplicationStream =>
    ({
      syncConfigContent: [
        {
          syncConfigState: 'PROCESSING',
          sync_rules_content: 'bucket_definitions: {}',
          version_label
        }
      ]
    }) as unknown as PersistedReplicationStream;

  it('requires both YAML and version label to match a processing config', async () => {
    const replicator = new TestReplicator(async () => {});
    const stream = processingStream('v2');

    expect(await replicator.shouldHandleStreamForTest(stream, 'bucket_definitions: {}', 'v2')).toBe(true);
    expect(await replicator.shouldHandleStreamForTest(stream, 'bucket_definitions: {}', 'v1')).toBe(false);
    expect(await replicator.shouldHandleStreamForTest(stream, 'bucket_definitions: {}', undefined)).toBe(false);
  });
});

const VALID_CONFIG = /* yaml */ `
  # Active configuration.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT id FROM todos
`;
const INVALID_CONFIG = /* yaml */ `
  # Rejected configured sync config.
  config:
    edition: 3
  streams:
    broken:
      query: SELECT FROM
`;

async function makeStartupReplicator(content: string, exitOnError = false, snapshot?: LoadedSyncConfig) {
  const configureSyncRules = vi.fn(async (_options: storage.UpdateSyncRulesOptions) => ({ updated: false }));
  const getActiveSyncConfig = vi.fn();
  const replicator = new TestReplicator(async () => {}, {
    id: 'test',
    storageEngine: {
      activeBucketStorage: { configureSyncRules, getActiveSyncConfig }
    } as unknown as AbstractReplicatorOptions['storageEngine'],
    configuration: {
      connections: [],
      sync_rules: {
        present: true,
        content,
        exit_on_error: exitOnError,
        version_label: 'local',
        loaded:
          arguments.length >= 3
            ? snapshot
            : await loadSyncConfig({ present: true, content, exit_on_error: exitOnError, version_label: 'local' })
      }
    },
    metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
    rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
  });
  return { replicator, configureSyncRules, getActiveSyncConfig };
}

describe('invalid configured sync config', () => {
  it.each([INVALID_CONFIG, 'streams: [', '# empty sync config\n'])(
    'logs and skips rejected YAML without storing it',
    async (yaml) => {
      const { replicator, configureSyncRules } = await makeStartupReplicator(yaml);
      const errors = vi.spyOn(replicator.loggerForTest, 'error');
      await replicator.runStartupForTest();
      expect(configureSyncRules).not.toHaveBeenCalled();
      expect(errors).toHaveBeenCalledWith(expect.stringContaining('Sync config error:'));
      errors.mockRestore();
    }
  );

  it('uses the same compiled snapshot for deployment and diagnostics', async () => {
    const loaded = await loadSyncConfig({
      present: true,
      content: VALID_CONFIG,
      exit_on_error: false
    });
    const { replicator, configureSyncRules } = await makeStartupReplicator(INVALID_CONFIG, false, loaded);
    await replicator.runStartupForTest();
    expect(loaded != null && 'compiled' in loaded).toBe(true);
    if (loaded != null && 'compiled' in loaded) {
      expect(configureSyncRules.mock.calls[0][0].config).toBe(loaded.compiled);
    }
    expect(getInvalidLocalSyncConfigStatus(loaded, true)).toBeUndefined();
  });

  it('rejects a file read failure rather than treating it as API mode', async () => {
    const error = new Error('File unavailable');
    const loaded: LoadedSyncConfig = { error };
    const { replicator, configureSyncRules } = await makeStartupReplicator('', false, loaded);
    await expect(replicator.runStartupForTest()).rejects.toBe(error);
    expect(configureSyncRules).not.toHaveBeenCalled();
    expect(getInvalidLocalSyncConfigStatus(loaded, true)).toBeUndefined();
  });

  it('accepts stored processing before configured content is supplied', async () => {
    const loaded = await loadSyncConfig({ present: false, exit_on_error: true });
    const { replicator, configureSyncRules, getActiveSyncConfig } = await makeStartupReplicator('', true, loaded);
    await replicator.runStartupForTest();
    expect(configureSyncRules).not.toHaveBeenCalled();
    const content = {
      sync_rules_content: VALID_CONFIG,
      version_label: 'active',
      syncConfigState: storage.SyncRuleState.PROCESSING
    };
    const stream = { syncConfigContent: [content] } as unknown as PersistedReplicationStream;
    getActiveSyncConfig.mockResolvedValue(null);
    expect(await replicator.shouldHandleStreamForTest(stream, undefined, undefined)).toBe(true);
    expect(getActiveSyncConfig).not.toHaveBeenCalled();
  });

  it.each([INVALID_CONFIG, '# empty sync config\n'])(
    'still fails startup with exit_on_error enabled',
    async (content) => {
      const { replicator, configureSyncRules } = await makeStartupReplicator(content, true);
      await expect(replicator.runStartupForTest()).rejects.toThrow();
      expect(configureSyncRules).not.toHaveBeenCalled();
    }
  );

  it('persists valid config, including warnings', async () => {
    const { replicator, configureSyncRules } = await makeStartupReplicator(VALID_CONFIG);
    await replicator.runStartupForTest();
    expect(configureSyncRules).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        version_label: 'local',
        config: expect.objectContaining({ yaml: VALID_CONFIG })
      })
    );
  });

  it.each(['matching', 'other-yaml', 'other-label', 'other-storage', 'no-active'])(
    'only accepts recovery matching the active config: %s',
    async (scenario) => {
      const { replicator, getActiveSyncConfig } = await makeStartupReplicator(INVALID_CONFIG);
      const active = { sync_rules_content: VALID_CONFIG, version_label: 'active', storageVersion: 2 };
      getActiveSyncConfig.mockResolvedValue(scenario == 'no-active' ? null : { content: active });
      const processing = {
        ...active,
        syncConfigState: storage.SyncRuleState.PROCESSING,
        ...(scenario == 'other-yaml' ? { sync_rules_content: INVALID_CONFIG } : {}),
        ...(scenario == 'other-label' ? { version_label: 'different' } : {}),
        ...(scenario == 'other-storage' ? { storageVersion: 4 } : {})
      };
      const stream = { syncConfigContent: [processing] } as unknown as PersistedReplicationStream;
      expect(await replicator.shouldHandleStreamForTest(stream, INVALID_CONFIG, 'local', true)).toBe(
        scenario == 'matching' || scenario == 'other-storage'
      );
    }
  );

  it.each([reprocess, reprocessSyncRules])('accepts a storage upgrade requested by $path', async (route) => {
    const { replicator, getActiveSyncConfig } = await makeStartupReplicator(INVALID_CONFIG);
    const active = { sync_rules_content: VALID_CONFIG, version_label: 'active', storageVersion: 2 };
    getActiveSyncConfig.mockResolvedValue({ content: active });
    let replacement: PersistedReplicationStream | undefined;
    const updateSyncRules = vi.fn(async (options: storage.UpdateSyncRulesOptions) => {
      // Model the backend selecting its v4 default for YAML without a storage_version.
      expect(options.storageVersion).toBeUndefined();
      expect(options.config.parsed.config.storageVersion).toBeUndefined();
      replacement = {
        replicationStreamName: 'replacement',
        syncConfigContent: [
          {
            syncConfigState: storage.SyncRuleState.PROCESSING,
            sync_rules_content: options.config.yaml,
            version_label: options.version_label,
            storageVersion: 4
          }
        ]
      } as unknown as PersistedReplicationStream;
      return replacement;
    });
    const service = mockServiceContext(null);
    Object.assign(service.storageEngine.activeBucketStorage, {
      getActiveSyncConfig,
      getDeployingSyncConfig: async () => null,
      updateSyncRules
    });
    await route.handler({
      context: { service_context: service } as Context,
      request: { headers: {}, hostname: '', protocol: 'http' } as BasicRouterRequest,
      params: {}
    });
    expect(updateSyncRules).toHaveBeenCalledOnce();
    expect(await replicator.shouldHandleStreamForTest(replacement!, INVALID_CONFIG, 'local', true)).toBe(true);
  });

  it('keeps active-only streams running without consulting recovery state', async () => {
    const { replicator, getActiveSyncConfig } = await makeStartupReplicator(INVALID_CONFIG);
    const stream = {
      syncConfigContent: [{ syncConfigState: storage.SyncRuleState.ACTIVE }]
    } as unknown as PersistedReplicationStream;
    expect(await replicator.shouldHandleStreamForTest(stream, INVALID_CONFIG, 'local', true)).toBe(true);
    expect(getActiveSyncConfig).not.toHaveBeenCalled();
  });
});
