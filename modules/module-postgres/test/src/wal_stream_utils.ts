import { BucketStorageFactory, SyncRulesBucketStorage } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import { getClientCheckpoint } from '../../src/util/utils.js';
import { TEST_CONNECTION_OPTIONS, clearTestDb } from './util.js';

/**
 * Tests operating on the wal stream need to configure the stream and manage asynchronous
 * replication, which gets a little tricky.
 *
 * This wraps a test in a function that configures all the context, and tears it down afterwards.
 */
export function walStreamTest(
  factory: () => Promise<BucketStorageFactory>,
  test: (context: WalStreamTestContext) => Promise<void>
): () => Promise<void> {
  return async () => {
    const f = await factory();
    const pool = pgwire.connectPgWirePool(TEST_CONNECTION_OPTIONS, {});

    await clearTestDb(pool);
    const context = new WalStreamTestContext(f, pool);
    try {
      await test(context);
    } finally {
      await context.dispose();
    }
  };
}

export class WalStreamTestContext {
  private _walStream?: WalStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;
  private replicationConnection?: pgwire.PgConnection;

  constructor(public factory: BucketStorageFactory, public connections: pgwire.PgClient) {}

  async dispose() {
    this.abortController.abort();
    await this.streamPromise;
    this.connections.destroy();
  }

  get pool() {
    return this.connections;
  }

  async updateSyncRules(content: string) {
    const syncRules = await this.factory.updateSyncRules({ content: content });
    this.storage = this.factory.getInstance(syncRules.parsed());
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
      factory: this.factory,
      connections: this.connections,
      abort_signal: this.abortController.signal
    };
    this._walStream = new WalStream(options);
    return this._walStream!;
  }

  async replicateSnapshot() {
    this.replicationConnection = await this.connections.replicationConnection();
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
      getClientCheckpoint(this.connections.pool, this.factory, { timeout: options?.timeout ?? 15_000 }),
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

  async getBucketData(bucket: string, start?: string, options?: { timeout?: number }) {
    start ??= '0';
    let checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, string>([[bucket, start]]);
    const batch = await this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await fromAsync(batch);
    return batches[0]?.batch.data ?? [];
  }
}
