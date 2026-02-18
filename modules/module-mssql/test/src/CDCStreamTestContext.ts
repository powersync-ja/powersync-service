import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  LEGACY_STORAGE_VERSION,
  OplogEntry,
  storage,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { METRICS_HELPER, test_utils } from '@powersync/service-core-tests';
import { clearTestDb, getClientCheckpoint, TEST_CONNECTION_OPTIONS } from './util.js';
import { CDCStream, CDCStreamOptions } from '@module/replication/CDCStream.js';
import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';
import timers from 'timers/promises';

/**
 * Tests operating on the change data capture need to configure the stream and manage asynchronous
 * replication, which gets a little tricky.
 *
 * This wraps all the context required for testing, and tears it down afterward
 * by using `await using`.
 */
export class CDCStreamTestContext implements AsyncDisposable {
  private _cdcStream?: CDCStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;
  private snapshotPromise?: Promise<void>;
  private replicationDone = false;

  static async open(
    factory: (options: storage.TestStorageOptions) => Promise<BucketStorageFactory>,
    options?: { doNotClear?: boolean; cdcStreamOptions?: Partial<CDCStreamOptions> }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new MSSQLConnectionManager(TEST_CONNECTION_OPTIONS, {});

    if (!options?.doNotClear) {
      await clearTestDb(connectionManager);
    }

    return new CDCStreamTestContext(f, connectionManager, options?.cdcStreamOptions);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MSSQLConnectionManager,
    private cdcStreamOptions?: Partial<CDCStreamOptions>
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  async [Symbol.asyncDispose]() {
    try {
      await this.dispose();
    } catch (err) {
      console.error('Error disposing CDCStreamTestContext', err);
    }
  }

  async dispose() {
    this.abortController.abort();
    await this.snapshotPromise;
    await this.streamPromise;
    await this.connectionManager.end();
    await this.factory?.[Symbol.asyncDispose]();
  }

  get connectionTag() {
    return this.connectionManager.connectionTag;
  }

  async updateSyncRules(content: string) {
    const syncRules = await this.factory.updateSyncRules({
      content: content,
      validate: true,
      storageVersion: LEGACY_STORAGE_VERSION
    });
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncRules = await this.factory.getNextSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Next sync rules not available`);
    }

    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadActiveSyncRules() {
    const syncRules = await this.factory.getActiveSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Active sync rules not available`);
    }

    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  get cdcStream() {
    if (this.storage == null) {
      throw new Error('updateSyncRules() first');
    }
    if (this._cdcStream) {
      return this._cdcStream;
    }
    const options: CDCStreamOptions = {
      storage: this.storage,
      metrics: METRICS_HELPER.metricsEngine,
      connections: this.connectionManager,
      abortSignal: this.abortController.signal,
      additionalConfig: {
        pollingBatchSize: 10,
        pollingIntervalMs: 1000,
        trustServerCertificate: true
      },
      ...this.cdcStreamOptions
    };
    this._cdcStream = new CDCStream(options);
    return this._cdcStream!;
  }

  /**
   * Replicate a snapshot, start streaming, and wait for a consistent checkpoint.
   */
  async initializeReplication() {
    await this.replicateSnapshot();
    // TODO: renable this.startStreaming();
    // Make sure we're up to date
    await this.getCheckpoint();
  }

  async replicateSnapshot() {
    await this.cdcStream.initReplication();
    this.replicationDone = true;
  }

  // TODO: Enable once streaming is implemented
  startStreaming() {
    if (!this.replicationDone) {
      throw new Error('Call replicateSnapshot() before startStreaming()');
    }
    this.streamPromise = this.cdcStream.streamChanges();
    // Wait for the replication to start before returning.
    // This avoids a bunch of unpredictable race conditions that appear in testing
    return new Promise<void>(async (resolve) => {
      while (this.cdcStream.isStartingReplication) {
        await timers.setTimeout(50);
      }

      resolve();
    });
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.connectionManager, this.factory, { timeout: options?.timeout ?? 15_000 }),
      this.streamPromise
    ]);
    if (checkpoint == null) {
      // This indicates an issue with the test setup - streamingPromise completed instead
      // of getClientCheckpoint()
      throw new Error('Test failure - streamingPromise completed');
    }
    return checkpoint;
  }

  async getBucketsDataBatch(buckets: Record<string, InternalOpId>, options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, InternalOpId>(Object.entries(buckets));
    return test_utils.fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  /**
   * This waits for a client checkpoint.
   */
  async getBucketData(bucket: string, start?: InternalOpId | string | undefined, options?: { timeout?: number }) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    const checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, InternalOpId>([[bucket, start]]);
    let data: OplogEntry[] = [];
    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);

      const batches = await test_utils.fromAsync(batch);
      data = data.concat(batches[0]?.chunkData.data ?? []);
      if (batches.length == 0 || !batches[0]!.chunkData.has_more) {
        break;
      }
      map.set(bucket, BigInt(batches[0]!.chunkData.next_after));
    }
    return data;
  }

  /**
   * This does not wait for a client checkpoint.
   */
  async getCurrentBucketData(bucket: string, start?: InternalOpId | string | undefined) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    const { checkpoint } = await this.storage!.getCheckpoint();
    const map = new Map<string, InternalOpId>([[bucket, start]]);
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await test_utils.fromAsync(batch);
    return batches[0]?.chunkData.data ?? [];
  }
}
