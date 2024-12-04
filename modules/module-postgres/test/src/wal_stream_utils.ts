import { fromAsync } from '@core-tests/stream_utils.js';
import { PgManager } from '@module/replication/PgManager.js';
import { PUBLICATION_NAME, WalStream, WalStreamOptions } from '@module/replication/WalStream.js';
import { BucketStorageFactory, SyncRulesBucketStorage } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import { clearTestDb, getClientCheckpoint, TEST_CONNECTION_OPTIONS } from './util.js';

export class WalStreamTestContext implements AsyncDisposable {
  private _walStream?: WalStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;
  private replicationConnection?: pgwire.PgConnection;

  /**
   * Tests operating on the wal stream need to configure the stream and manage asynchronous
   * replication, which gets a little tricky.
   *
   * This configures all the context, and tears it down afterwards.
   */
  static async open(factory: () => Promise<BucketStorageFactory>) {
    const f = await factory();
    const connectionManager = new PgManager(TEST_CONNECTION_OPTIONS, {});

    await clearTestDb(connectionManager.pool);

    return new WalStreamTestContext(f, connectionManager);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: PgManager
  ) {}

  async [Symbol.asyncDispose]() {
    this.abortController.abort();
    await this.streamPromise;
    await this.connectionManager.destroy();
    this.storage?.[Symbol.dispose]();
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
    const syncRules = await this.factory.updateSyncRules({ content: content });
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
      connections: this.connectionManager,
      abort_signal: this.abortController.signal
    };
    this._walStream = new WalStream(options);
    return this._walStream!;
  }

  async replicateSnapshot() {
    this.replicationConnection = await this.connectionManager.replicationConnection();
    await this.walStream.initReplication(this.replicationConnection);
    await this.storage!.autoActivate();
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
    if (typeof checkpoint == undefined) {
      // This indicates an issue with the test setup - streamingPromise completed instead
      // of getClientCheckpoint()
      throw new Error('Test failure - streamingPromise completed');
    }
    return checkpoint as string;
  }

  async getBucketsDataBatch(buckets: Record<string, string>, options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, string>(Object.entries(buckets));
    return fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  /**
   * This waits for a client checkpoint.
   */
  async getBucketData(bucket: string, start?: string, options?: { timeout?: number }) {
    start ??= '0';
    const checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, string>([[bucket, start]]);
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await fromAsync(batch);
    return batches[0]?.batch.data ?? [];
  }

  /**
   * This does not wait for a client checkpoint.
   */
  async getCurrentBucketData(bucket: string, start?: string) {
    start ??= '0';
    const { checkpoint } = await this.storage!.getCheckpoint();
    const map = new Map<string, string>([[bucket, start]]);
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await fromAsync(batch);
    return batches[0]?.batch.data ?? [];
  }
}
