import { PgManager } from '@module/replication/PgManager.js';
import { PUBLICATION_NAME, WalStream, WalStreamOptions } from '@module/replication/WalStream.js';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  OplogEntry,
  storage,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { METRICS_HELPER, test_utils } from '@powersync/service-core-tests';
import * as pgwire from '@powersync/service-jpgwire';
import { clearTestDb, getClientCheckpoint, TEST_CONNECTION_OPTIONS } from './util.js';

export class WalStreamTestContext implements AsyncDisposable {
  private _walStream?: WalStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;
  private replicationConnection?: pgwire.PgConnection;
  private snapshotPromise?: Promise<void>;

  /**
   * Tests operating on the wal stream need to configure the stream and manage asynchronous
   * replication, which gets a little tricky.
   *
   * This configures all the context, and tears it down afterwards.
   */
  static async open(
    factory: (options: storage.TestStorageOptions) => Promise<BucketStorageFactory>,
    options?: { doNotClear?: boolean; walStreamOptions?: Partial<WalStreamOptions> }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new PgManager(TEST_CONNECTION_OPTIONS, {});

    if (!options?.doNotClear) {
      await clearTestDb(connectionManager.pool);
    }

    return new WalStreamTestContext(f, connectionManager, options?.walStreamOptions);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: PgManager,
    private walStreamOptions?: Partial<WalStreamOptions>
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  async [Symbol.asyncDispose]() {
    await this.dispose();
  }

  async dispose() {
    this.abortController.abort();
    await this.snapshotPromise;
    await this.streamPromise;
    await this.connectionManager.destroy();
    await this.factory?.[Symbol.asyncDispose]();
  }

  get pool() {
    return this.connectionManager.pool;
  }

  get connectionTag() {
    return this.connectionManager.connectionTag;
  }

  get publicationName() {
    return PUBLICATION_NAME;
  }

  async updateSyncRules(content: string) {
    const syncRules = await this.factory.updateSyncRules({ content: content, validate: true });
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

  get walStream() {
    if (this.storage == null) {
      throw new Error('updateSyncRules() first');
    }
    if (this._walStream) {
      return this._walStream;
    }
    const options: WalStreamOptions = {
      storage: this.storage,
      metrics: METRICS_HELPER.metricsEngine,
      connections: this.connectionManager,
      abort_signal: this.abortController.signal,
      ...this.walStreamOptions
    };
    this._walStream = new WalStream(options);
    return this._walStream!;
  }

  /**
   * Replicate a snapshot, start streaming, and wait for a consistent checkpoint.
   */
  async initializeReplication() {
    await this.replicateSnapshot();
    this.startStreaming();
    // Make sure we're up to date
    await this.getCheckpoint();
  }

  async replicateSnapshot() {
    const promise = (async () => {
      this.replicationConnection = await this.connectionManager.replicationConnection();
      await this.walStream.initReplication(this.replicationConnection);
    })();
    this.snapshotPromise = promise.catch((e) => e);
    await promise;
  }

  startStreaming() {
    if (this.replicationConnection == null) {
      throw new Error('Call replicateSnapshot() before startStreaming()');
    }
    this.streamPromise = this.walStream.streamChanges(this.replicationConnection!);
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.pool, this.factory, { timeout: options?.timeout ?? 15_000 }),
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
