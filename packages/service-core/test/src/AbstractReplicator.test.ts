import type { RouteAPI } from '@/api/RouteAPI.js';
import { getInvalidLocalSyncConfigStatus } from '@/api/diagnostics.js';
import { BasicRouterRequest, Context, storage } from '@/index.js';
import { AbstractReplicationJob } from '@/replication/AbstractReplicationJob.js';
import { AbstractReplicator, AbstractReplicatorOptions, CreateJobOptions } from '@/replication/AbstractReplicator.js';
import { reprocess } from '@/routes/endpoints/admin.js';
import { reprocessSyncRules } from '@/routes/endpoints/sync-rules.js';
import { PersistedReplicationStream } from '@/storage/PersistedReplicationStream.js';
import type { ReplicationLock } from '@/storage/ReplicationLock.js';
import { SyncRulesBucketStorage } from '@/storage/SyncRulesBucketStorage.js';
import { SyncConfigEngine, SyncConfigResultStatus } from '@/system/SyncConfigEngine.js';
import type { SyncRulesConfig } from '@/util/config/types.js';
import * as syncConfigParser from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { mockServiceContext } from './routes/mocks.js';

type TestReplicatorOptions = Omit<AbstractReplicatorOptions, 'syncConfigEngine'> & {
  syncConfigEngine?: SyncConfigEngine;
};
class TestReplicator extends AbstractReplicator {
  readonly source: RouteAPI;
  readonly syncConfigEngineForTest: SyncConfigEngine;
  constructor(
    private readonly cleanup: (storage: SyncRulesBucketStorage) => Promise<void>,
    options: TestReplicatorOptions = {
      id: 'test',
      storageEngine: {} as AbstractReplicatorOptions['storageEngine'],
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      configuration: { connections: [], sync_rules: { present: false, exit_on_error: true } },
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    }
  ) {
    const source = {
      getParseSyncRulesOptions: () => ({
        defaultSchema: 'public',
        connections: Object.fromEntries(
          (options.configuration.connections ?? []).map((c) => [c.tag ?? 'default', { type: c.type }])
        )
      })
    } as RouteAPI;
    const syncConfigEngine =
      options.syncConfigEngine ?? new SyncConfigEngine(options.configuration.sync_rules, () => source);
    super({ ...options, syncConfigEngine });
    this.source = source;
    this.syncConfigEngineForTest = syncConfigEngine;
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
    // Keep startup admission real, but skip the continuous replication loop.
    controller.abort();
    (this as any).abortController = controller;
    await (this as any).runLoop();
  }

  async refreshForTest(
    configuredLock?: ReplicationLock,
    loadedSyncConfig?: string,
    invalid = false
  ): Promise<{ replicationJobStarted: boolean }> {
    (this as any).abortController = new AbortController();
    return (this as any).refresh({
      configuredLock,
      loadedSyncConfig,
      loadedSyncConfigInvalid: invalid,
      loadedVersionLabel: undefined
    });
  }

  shouldHandleStreamForTest(
    replicationStream: PersistedReplicationStream,
    loadedSyncRules: string | undefined,
    loadedVersionLabel: string | undefined,
    invalid = false
  ): Promise<boolean> {
    return this.shouldHandleReplicationStream(replicationStream, loadedSyncRules, loadedVersionLabel, invalid);
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
        activeBucketStorage: {
          configureSyncRules,
          stopProcessingSyncConfigs: vi.fn(async () => {}),
          getActiveSyncConfig: async () => null
        }
      } as unknown as AbstractReplicatorOptions['storageEngine'],
      configuration: {
        connections: [{ type: 'postgresql' }],
        sync_rules: {
          present: true,
          exit_on_error: exitOnError,
          content: yaml
        }
      },
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    });
    const loaded = await replicator.syncConfigEngineForTest.getConfigured();
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
      (['warning', 'valid', 'fatal', 'unexpected error'] as const).map((result) => ({ exitOnError, result }))
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
          activeBucketStorage: {
            configureSyncRules,
            stopProcessingSyncConfigs: vi.fn(async () => {}),
            getActiveSyncConfig: async () => null
          }
        } as unknown as AbstractReplicatorOptions['storageEngine'],
        configuration: {
          connections: [{ type: 'postgresql' }, { type: 'mongodb', tag: 'archive' }],
          sync_rules: {
            present: true,
            exit_on_error: exitOnError,
            content: yaml
          }
        },
        metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
        rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
      });
      using parse = vi.spyOn(syncConfigParser, 'parseSyncConfig');
      using loggedError = vi.spyOn(replicator.loggerForTest, 'error');
      using loggedWarning = vi.spyOn(replicator.loggerForTest, 'warn');
      replicator.source.validateSourceCapabilities = async () => {
        calls.push('validate');
        if (result === 'unexpected error') {
          throw new Error('Source validation failed');
        }
        return result === 'fatal'
          ? [{ level: 'fatal', message: 'Source validation failed' }]
          : result === 'warning'
            ? [{ level: 'warning', message: 'Missing index' }]
            : [];
      };

      const rejected = result === 'fatal' || result === 'unexpected error';
      if (rejected && exitOnError) {
        await expect(replicator.runStartupForTest()).rejects.toThrow('Source validation failed');
        expect(calls).toEqual(['validate']);
      } else {
        await replicator.runStartupForTest();
        expect(calls).toEqual(rejected ? ['validate'] : ['validate', 'persist']);
      }
      // Source diagnostics must use the same logging levels and prefixes as parser diagnostics.
      if (rejected) {
        expect(configureSyncRules).not.toHaveBeenCalled();
        expect(loggedError).toHaveBeenCalledWith('Sync config error: Source validation failed');
      } else if (result === 'warning') {
        expect(loggedWarning).toHaveBeenCalledWith('Sync config warning: Missing index');
      }
      expect(parse).toHaveBeenCalledWith(
        yaml,
        expect.objectContaining({
          connections: { default: { type: 'postgresql' }, archive: { type: 'mongodb' } }
        })
      );
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
        activeBucketStorage: {
          configureSyncRules,
          stopProcessingSyncConfigs: vi.fn(async () => {}),
          getActiveSyncConfig: async () => null
        }
      } as unknown as AbstractReplicatorOptions['storageEngine'],
      configuration: {
        connections: [],
        sync_rules: {
          present: true,
          exit_on_error: exitOnError,
          version_label: 'v2',
          content: yaml
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
  const options: TestReplicatorOptions = {
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

async function makeStartupReplicator(content: string, exitOnError = false, settings?: SyncRulesConfig) {
  const configureSyncRules = vi.fn(async (_options: storage.UpdateSyncRulesOptions) => ({ updated: false }));
  const getActiveSyncConfig = vi.fn();
  const stopProcessingSyncConfigs = vi.fn(async () => {});
  const restartReplication = vi.fn(async (_streamId: number) => {});
  const replicator = new TestReplicator(async () => {}, {
    id: 'test',
    storageEngine: {
      activeBucketStorage: {
        configureSyncRules,
        getActiveSyncConfig,
        stopProcessingSyncConfigs,
        restartReplication,
        getReplicatingReplicationStreams: async () => [],
        getStoppedReplicationStreams: async () => []
      }
    } as unknown as AbstractReplicatorOptions['storageEngine'],
    configuration: {
      connections: [],
      sync_rules: settings ?? { present: true, content, exit_on_error: exitOnError, version_label: 'local' }
    },
    metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
    rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
  });
  return { replicator, configureSyncRules, getActiveSyncConfig, stopProcessingSyncConfigs, restartReplication };
}

describe('invalid configured sync config', () => {
  it('cancels pending work once at startup without cancelling again on refresh', async () => {
    const { replicator, stopProcessingSyncConfigs } = await makeStartupReplicator(INVALID_CONFIG);
    await replicator.runStartupForTest();
    // Later refreshes must leave deployments submitted by a newer runner alone.
    await replicator.refreshForTest(undefined, INVALID_CONFIG, true);
    await replicator.refreshForTest(undefined, INVALID_CONFIG, true);
    expect(stopProcessingSyncConfigs).toHaveBeenCalledOnce();
  });

  it('does not cancel pending work when exit_on_error is enabled', async () => {
    const { replicator, stopProcessingSyncConfigs } = await makeStartupReplicator(INVALID_CONFIG, true);
    await expect(replicator.runStartupForTest()).rejects.toThrow();
    expect(stopProcessingSyncConfigs).not.toHaveBeenCalled();
  });
  it.each([
    { inputKind: 'invalid query', yaml: INVALID_CONFIG },
    { inputKind: 'malformed YAML', yaml: 'streams: [' },
    { inputKind: 'empty config', yaml: '# empty sync config\n' }
  ])('logs and skips rejected YAML without storing it: $inputKind', async ({ yaml }) => {
    const { replicator, configureSyncRules } = await makeStartupReplicator(yaml);
    const errors = vi.spyOn(replicator.loggerForTest, 'error');
    await replicator.runStartupForTest();
    expect(configureSyncRules).not.toHaveBeenCalled();
    expect(errors).toHaveBeenCalledWith(expect.stringContaining('Sync config error:'));
    errors.mockRestore();
  });

  it('uses the same compiled snapshot for deployment and diagnostics', async () => {
    const { replicator, configureSyncRules } = await makeStartupReplicator(VALID_CONFIG);
    await replicator.runStartupForTest();
    const result = await replicator.syncConfigEngineForTest.getConfigured();
    expect(result.status).toBe(SyncConfigResultStatus.ACCEPTED);
    if (result.status === SyncConfigResultStatus.ACCEPTED) {
      expect(configureSyncRules.mock.calls[0][0].config).toBe(result.compiled);
    }
    expect(getInvalidLocalSyncConfigStatus(result, true)).toBeUndefined();
  });

  it('rejects a file read failure rather than treating it as API mode', async () => {
    const { replicator, configureSyncRules } = await makeStartupReplicator('', false, {
      present: true,
      path: '/nonexistent-powersync-engine-test/sync-config.yaml',
      exit_on_error: false
    });
    const result = await replicator.syncConfigEngineForTest.getConfigured();
    expect(result.status).toBe(SyncConfigResultStatus.UNAVAILABLE);
    // Missing configured input is a startup failure, not permission to use API-managed deployments.
    if (result.status !== SyncConfigResultStatus.UNAVAILABLE) {
      throw new Error('Expected input load failure');
    }
    await expect(replicator.runStartupForTest()).rejects.toBe(result.error);
    expect(configureSyncRules).not.toHaveBeenCalled();
    expect(
      getInvalidLocalSyncConfigStatus(await replicator.syncConfigEngineForTest.getConfigured(), true)
    ).toBeUndefined();
  });

  it('accepts stored processing before configured content is supplied', async () => {
    const { replicator, configureSyncRules, getActiveSyncConfig } = await makeStartupReplicator('', true, {
      present: false,
      exit_on_error: true
    });
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

  it.each([
    { inputKind: 'invalid query', content: INVALID_CONFIG },
    { inputKind: 'empty config', content: '# empty sync config\n' }
  ])('still fails startup with exit_on_error enabled: $inputKind', async ({ content }) => {
    const { replicator, configureSyncRules } = await makeStartupReplicator(content, true);
    await expect(replicator.runStartupForTest()).rejects.toThrow();
    expect(configureSyncRules).not.toHaveBeenCalled();
  });

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

  /**
   * Previously persisted invalid work remains owned only when both local YAML and label match.
   * This compatibility path also works without an active config (a rejected first deployment).
   */
  it.each([{ deploymentMatch: 'matching' }, { deploymentMatch: 'other-yaml' }, { deploymentMatch: 'other-label' }])(
    'checks ownership of previously persisted invalid config: $deploymentMatch',
    async ({ deploymentMatch }) => {
      const { replicator, getActiveSyncConfig } = await makeStartupReplicator(INVALID_CONFIG);
      getActiveSyncConfig.mockResolvedValue(null);
      const stream = {
        syncConfigContent: [
          {
            syncConfigState: storage.SyncRuleState.PROCESSING,
            sync_rules_content: deploymentMatch === 'other-yaml' ? VALID_CONFIG : INVALID_CONFIG,
            version_label: deploymentMatch === 'other-label' ? 'another-worker' : 'local'
          }
        ]
      } as unknown as PersistedReplicationStream;
      expect(await replicator.shouldHandleStreamForTest(stream, INVALID_CONFIG, 'local', true)).toBe(
        deploymentMatch === 'matching'
      );
      if (deploymentMatch === 'matching') {
        expect(getActiveSyncConfig).not.toHaveBeenCalled();
      }
    }
  );

  it.each([
    { recoveryMatch: 'matching' },
    { recoveryMatch: 'other-yaml' },
    { recoveryMatch: 'other-label' },
    { recoveryMatch: 'other-storage' },
    { recoveryMatch: 'no-active' }
  ])('only accepts recovery matching the active config: $recoveryMatch', async ({ recoveryMatch }) => {
    const { replicator, getActiveSyncConfig } = await makeStartupReplicator(INVALID_CONFIG);
    const active = { sync_rules_content: VALID_CONFIG, version_label: 'active', storageVersion: 2 };
    getActiveSyncConfig.mockResolvedValue(recoveryMatch == 'no-active' ? null : { content: active });
    const processing = {
      ...active,
      syncConfigState: storage.SyncRuleState.PROCESSING,
      ...(recoveryMatch == 'other-yaml' ? { sync_rules_content: INVALID_CONFIG } : {}),
      ...(recoveryMatch == 'other-label' ? { version_label: 'different' } : {}),
      // A storage upgrade does not change which YAML deployment the replacement belongs to.
      ...(recoveryMatch == 'other-storage' ? { storageVersion: 4 } : {})
    };
    const stream = { syncConfigContent: [processing] } as unknown as PersistedReplicationStream;
    expect(await replicator.shouldHandleStreamForTest(stream, INVALID_CONFIG, 'local', true)).toBe(
      recoveryMatch == 'matching' || recoveryMatch == 'other-storage'
    );
  });

  it.each([{ route: reprocess }, { route: reprocessSyncRules }])(
    'accepts a storage upgrade requested by $route.path',
    async ({ route }) => {
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
      // A separate API runner submits the rebuild; the replicator still holds rejected configured YAML.
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
    }
  );

  it('keeps active-only streams running without consulting recovery state', async () => {
    const { replicator, getActiveSyncConfig } = await makeStartupReplicator(INVALID_CONFIG);
    const stream = {
      syncConfigContent: [{ syncConfigState: storage.SyncRuleState.ACTIVE }]
    } as unknown as PersistedReplicationStream;
    expect(await replicator.shouldHandleStreamForTest(stream, INVALID_CONFIG, 'local', true)).toBe(true);
    expect(getActiveSyncConfig).not.toHaveBeenCalled();
  });
});
