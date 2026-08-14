import { AbstractReplicationJob } from '@/replication/AbstractReplicationJob.js';
import { AbstractReplicator, AbstractReplicatorOptions, CreateJobOptions } from '@/replication/AbstractReplicator.js';
import { PersistedReplicationStream } from '@/storage/PersistedReplicationStream.js';
import { SyncRulesBucketStorage } from '@/storage/SyncRulesBucketStorage.js';
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
}

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
