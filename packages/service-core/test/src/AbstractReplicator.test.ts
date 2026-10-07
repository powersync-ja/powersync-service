import type { RouteAPI } from '@/api/RouteAPI.js';
import { assertSourceCapabilities, validateNoMongoFilterExpressions } from '@/api/source-capabilities.js';
import { AbstractReplicationJob } from '@/replication/AbstractReplicationJob.js';
import { AbstractReplicator, AbstractReplicatorOptions, CreateJobOptions } from '@/replication/AbstractReplicator.js';
import { PersistedReplicationStream } from '@/storage/PersistedReplicationStream.js';
import type { ReplicationLock } from '@/storage/ReplicationLock.js';
import { SyncRulesBucketStorage } from '@/storage/SyncRulesBucketStorage.js';
import type { SyncConfig } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';

class TestReplicator extends AbstractReplicator {
  constructor(
    private readonly cleanup: (storage: SyncRulesBucketStorage) => Promise<void>,
    options: AbstractReplicatorOptions = {
      id: 'test',
      storageEngine: {} as AbstractReplicatorOptions['storageEngine'],
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      syncRuleProvider: {} as AbstractReplicatorOptions['syncRuleProvider'],
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
      loadedVersionLabel: this.syncRuleProvider.versionLabel
    });
  }

  shouldHandleStreamForTest(
    replicationStream: PersistedReplicationStream,
    loadedSyncRules: string | undefined,
    loadedVersionLabel: string | undefined
  ): boolean {
    return (this as any).shouldHandleReplicationStream(replicationStream, loadedSyncRules, loadedVersionLabel);
  }
}

describe('AbstractReplicator startup sync config', () => {
  it.each([true, false])('respects exit_on_error=%s for unsupported file-loaded filters', async (exitOnError) => {
    const yaml = /* yaml */ `
      # Sync config fixture.
      config:
        edition: 3
        source_table_options:
          orders:
            mongodb_filter_expression: { $eq: ['$$doc.active', true] }
      streams:
        orders:
          query: SELECT * FROM orders
    `;
    const configureSyncRules = vi.fn(async () => ({ updated: false }));
    const replicator = new TestReplicator(async () => {}, {
      id: 'test',
      storageEngine: {
        activeBucketStorage: { configureSyncRules }
      } as unknown as AbstractReplicatorOptions['storageEngine'],
      syncRuleProvider: { get: async () => yaml, exitOnError, versionLabel: undefined },
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    });
    replicator.registerSourceCapabilitiesAssertion((config) =>
      assertSourceCapabilities(
        {
          async validateSourceCapabilities(config: SyncConfig) {
            return validateNoMongoFilterExpressions(config, 'default');
          }
        } as unknown as RouteAPI,
        config
      )
    );

    if (exitOnError) {
      await expect(replicator.runStartupForTest()).rejects.toThrow('only supported by MongoDB sources');
      expect(configureSyncRules).not.toHaveBeenCalled();
    } else {
      await expect(replicator.runStartupForTest()).resolves.toBeUndefined();
      expect(configureSyncRules).toHaveBeenCalledOnce();
    }
  });

  it('passes source validation to replication jobs after persisting a file-loaded config', async () => {
    const yaml = /* yaml */ `
      # Sync config fixture.
      config:
        edition: 3
      streams:
        orders:
          query: SELECT * FROM orders
    `;
    const lock = { sync_rules_id: 1, release: vi.fn(async () => {}) } as ReplicationLock;
    const stream = {
      replicationStreamId: 1,
      replicationStreamName: 'test',
      replicationJobId: 'test-1',
      state: 'PROCESSING'
    } as unknown as PersistedReplicationStream;
    const bucketStorage = {} as SyncRulesBucketStorage;
    const configureSyncRules = vi.fn(async () => ({ updated: true, lock }));
    const replicator = new TestReplicator(async () => {}, {
      id: 'test',
      storageEngine: {
        activeBucketStorage: {
          configureSyncRules,
          getReplicatingReplicationStreams: async () => [stream],
          getStoppedReplicationStreams: async () => [],
          getInstance: () => bucketStorage
        }
      } as unknown as AbstractReplicatorOptions['storageEngine'],
      syncRuleProvider: { get: async () => yaml, exitOnError: false, versionLabel: undefined },
      metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
      rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
    });
    const assertion = vi.fn(async (): Promise<void> => {
      throw new Error('Source capability unavailable');
    });
    replicator.registerSourceCapabilitiesAssertion(assertion);
    const start = vi.fn();
    const createJob = vi.spyOn(replicator, 'createJob').mockReturnValue({ start } as unknown as AbstractReplicationJob);

    await replicator.runStartupForTest();
    expect(configureSyncRules).toHaveBeenCalledOnce();
    await expect(replicator.refreshForTest(lock)).resolves.toEqual({ replicationJobStarted: true });
    expect(createJob).toHaveBeenCalledExactlyOnceWith({
      lock,
      storage: bucketStorage,
      assertSourceCapabilities: assertion
    });
    expect(assertion).toHaveBeenCalledOnce();
    expect(start).toHaveBeenCalledOnce();
  });

  it.each(
    [true, false].flatMap((exitOnError) =>
      (['warning', 'valid', 'unexpected error'] as const).map((result) => ({ exitOnError, result }))
    )
  )(
    'checks source capabilities before persistence: $result, exit_on_error=$exitOnError',
    async ({ result, exitOnError }) => {
      const yaml = /* yaml */ `
        # Sync config fixture.
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
        syncRuleProvider: { get: async () => yaml, exitOnError, versionLabel: undefined },
        metricsEngine: {} as AbstractReplicatorOptions['metricsEngine'],
        rateLimiter: {} as AbstractReplicatorOptions['rateLimiter']
      });
      replicator.registerSourceCapabilitiesAssertion((config) =>
        assertSourceCapabilities(
          {
            async validateSourceCapabilities() {
              calls.push('validate');
              if (result === 'unexpected error') throw new Error('Source validation failed');
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
      syncRuleProvider: { get: async () => yaml, exitOnError, versionLabel: 'v2' },
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
    syncRuleProvider: {} as AbstractReplicatorOptions['syncRuleProvider'],
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

  it('requires both YAML and version label to match a processing config', () => {
    const replicator = new TestReplicator(async () => {});
    const stream = processingStream('v2');

    expect(replicator.shouldHandleStreamForTest(stream, 'bucket_definitions: {}', 'v2')).toBe(true);
    expect(replicator.shouldHandleStreamForTest(stream, 'bucket_definitions: {}', 'v1')).toBe(false);
    expect(replicator.shouldHandleStreamForTest(stream, 'bucket_definitions: {}', undefined)).toBe(false);
  });
});
