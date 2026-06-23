import { replication, storage } from '@/index.js';
import { describe, expect, test, vi } from 'vitest';

class TestLock implements storage.ReplicationLock {
  releaseCount = 0;

  constructor(public sync_rules_id: number) {}

  async release(): Promise<void> {
    this.releaseCount++;
  }
}

class TestReplicationStream extends storage.PersistedReplicationStream {
  readonly syncConfigContent: readonly storage.PersistedSyncConfigContent[] = [];
  readonly current_lock: storage.ReplicationLock | null = null;
  lockCount = 0;

  constructor(
    options: {
      streamId: number;
      jobId: string;
      state?: storage.SyncRuleState;
    },
    private readonly testLock = new TestLock(options.streamId)
  ) {
    super({
      replicationStreamId: options.streamId,
      replicationStreamName: `stream_${options.streamId}`,
      replicationJobId: options.jobId,
      state: options.state ?? storage.SyncRuleState.PROCESSING,
      storageVersion: storage.CURRENT_STORAGE_VERSION
    });
  }

  async lock(): Promise<storage.ReplicationLock> {
    this.lockCount++;
    return this.testLock;
  }

  parsed(): storage.ParsedSyncConfigSet {
    throw new Error('Not implemented');
  }
}

class TestBucketStorageFactory {
  streams: storage.PersistedReplicationStream[] = [];
  readonly getInstance = vi.fn((stream: storage.PersistedReplicationStream) => {
    return {
      replicationStreamId: stream.replicationStreamId,
      replicationStreamName: stream.replicationStreamName,
      storageConfig: stream.getStorageConfig(),
      logger: stream.logger
    } as storage.SyncRulesBucketStorage;
  });

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    return this.streams;
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    return [];
  }
}

class TestReplicationJob extends replication.AbstractReplicationJob {
  constructor(options: replication.AbstractReplicationJobOptions) {
    super(options);
  }

  async replicate(): Promise<void> {
    await new Promise<void>((resolve) => {
      this.abortController.signal.addEventListener('abort', () => resolve(), { once: true });
    });
  }

  async keepAlive(): Promise<void> {}

  getReplicationLagMillis(): number | undefined {
    return undefined;
  }
}

class TestReplicator extends replication.AbstractReplicator<TestReplicationJob> {
  readonly jobs: TestReplicationJob[] = [];

  constructor(private readonly bucketStorage: TestBucketStorageFactory) {
    super({
      id: 'test',
      storageEngine: {
        activeBucketStorage: bucketStorage
      } as unknown as storage.StorageEngine,
      metricsEngine: null as any,
      syncRuleProvider: null as any,
      rateLimiter: {
        mayPing: () => false,
        waitUntilAllowed: async () => {},
        reportError: () => {}
      }
    });
    (this as any).abortController = new AbortController();
  }

  createJob(options: replication.CreateJobOptions): TestReplicationJob {
    const job = new TestReplicationJob({
      id: `job_${this.jobs.length}`,
      storage: options.storage,
      metrics: null as any,
      lock: options.lock,
      rateLimiter: this.rateLimiter
    });
    this.jobs.push(job);
    return job;
  }

  async cleanUp(): Promise<void> {}

  async testConnection(): Promise<replication.ConnectionTestResult> {
    return { connectionDescription: 'test' };
  }

  async refreshOnce(options?: { configured_lock?: storage.ReplicationLock }): Promise<void> {
    await this.refresh(options);
  }

  get stoppedForTest() {
    return this.stopped;
  }
}

describe('AbstractReplicator handover mode', () => {
  test('does not start new replication jobs after accepting jobs stops', async () => {
    const factory = new TestBucketStorageFactory();
    const stream = new TestReplicationStream({ streamId: 1, jobId: '1:a' });
    factory.streams = [stream];
    const replicator = new TestReplicator(factory);

    replicator.stopAcceptingReplicationJobs();
    await replicator.refreshOnce();

    expect(stream.lockCount).toBe(0);
    expect(factory.getInstance).not.toHaveBeenCalled();
    expect(replicator.jobs).toHaveLength(0);
    expect(replicator.stoppedForTest).toBe(true);
  });

  test('keeps existing matching replication jobs after accepting jobs stops', async () => {
    const factory = new TestBucketStorageFactory();
    const lock = new TestLock(1);
    const stream = new TestReplicationStream({ streamId: 1, jobId: '1:a' }, lock);
    factory.streams = [stream];
    const replicator = new TestReplicator(factory);

    await replicator.refreshOnce();
    replicator.stopAcceptingReplicationJobs();
    await replicator.refreshOnce();

    expect(stream.lockCount).toBe(1);
    expect(factory.getInstance).toHaveBeenCalledTimes(1);
    expect(replicator.jobs).toHaveLength(1);
    expect(lock.releaseCount).toBe(0);
    expect(replicator.stoppedForTest).toBe(false);

    await replicator.stop();
  });

  test('stops an existing job whose persisted job id changed without starting the replacement', async () => {
    const factory = new TestBucketStorageFactory();
    const oldLock = new TestLock(1);
    const oldStream = new TestReplicationStream({ streamId: 1, jobId: '1:old' }, oldLock);
    factory.streams = [oldStream];
    const replicator = new TestReplicator(factory);

    await replicator.refreshOnce();

    const newStream = new TestReplicationStream({ streamId: 1, jobId: '1:new' });
    factory.streams = [newStream];
    replicator.stopAcceptingReplicationJobs();
    await replicator.refreshOnce();

    expect(oldLock.releaseCount).toBe(1);
    expect(newStream.lockCount).toBe(0);
    expect(factory.getInstance).toHaveBeenCalledTimes(1);
    expect(replicator.jobs).toHaveLength(1);
    expect(replicator.stoppedForTest).toBe(true);
  });

  test('releases a configured lock when accepting jobs stops before the first refresh', async () => {
    const factory = new TestBucketStorageFactory();
    const stream = new TestReplicationStream({ streamId: 1, jobId: '1:a' });
    const configuredLock = new TestLock(1);
    factory.streams = [stream];
    const replicator = new TestReplicator(factory);

    replicator.stopAcceptingReplicationJobs();
    await replicator.refreshOnce({ configured_lock: configuredLock });

    expect(configuredLock.releaseCount).toBe(1);
    expect(stream.lockCount).toBe(0);
    expect(replicator.jobs).toHaveLength(0);
    expect(replicator.stoppedForTest).toBe(true);
  });
});
