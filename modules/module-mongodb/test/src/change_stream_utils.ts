import { ActiveCheckpoint, BucketStorageFactory, OpId, SyncRulesBucketStorage } from '@powersync/service-core';

import { TEST_CONNECTION_OPTIONS, clearTestDb } from './util.js';
import { fromAsync } from '@core-tests/stream_utils.js';
import { MongoManager } from '@module/replication/MongoManager.js';
import { ChangeStream, ChangeStreamOptions } from '@module/replication/ChangeStream.js';
import * as mongo from 'mongodb';
import { createCheckpoint } from '@module/replication/MongoRelation.js';

/**
 * Tests operating on the wal stream need to configure the stream and manage asynchronous
 * replication, which gets a little tricky.
 *
 * This wraps a test in a function that configures all the context, and tears it down afterwards.
 */
export function walStreamTest(
  factory: () => Promise<BucketStorageFactory>,
  test: (context: ChangeStreamTestContext) => Promise<void>
): () => Promise<void> {
  return async () => {
    const f = await factory();
    const connectionManager = new MongoManager(TEST_CONNECTION_OPTIONS);

    await clearTestDb(connectionManager.db);
    const context = new ChangeStreamTestContext(f, connectionManager);
    try {
      await test(context);
    } finally {
      await context.dispose();
    }
  };
}

export class ChangeStreamTestContext {
  private _walStream?: ChangeStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MongoManager
  ) {}

  async dispose() {
    this.abortController.abort();
    await this.streamPromise?.catch((e) => e);
    await this.connectionManager.destroy();
  }

  get client() {
    return this.connectionManager.client;
  }

  get db() {
    return this.connectionManager.db;
  }

  get connectionTag() {
    return this.connectionManager.connectionTag;
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
    const options: ChangeStreamOptions = {
      storage: this.storage,
      connections: this.connectionManager,
      abort_signal: this.abortController.signal
    };
    this._walStream = new ChangeStream(options);
    return this._walStream!;
  }

  async replicateSnapshot() {
    await this.walStream.initReplication();
    await this.storage!.autoActivate();
  }

  startStreaming() {
    this.streamPromise = this.walStream.streamChanges();
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.client, this.db, this.factory, { timeout: options?.timeout ?? 15_000 }),
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
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await fromAsync(batch);
    return batches[0]?.batch.data ?? [];
  }
}

export async function getClientCheckpoint(
  client: mongo.MongoClient,
  db: mongo.Db,
  bucketStorage: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<OpId> {
  const start = Date.now();
  const lsn = await createCheckpoint(client, db);
  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;
  let lastCp: ActiveCheckpoint | null = null;

  while (Date.now() - start < timeout) {
    const cp = await bucketStorage.getActiveCheckpoint();
    lastCp = cp;
    if (!cp.hasSyncRules()) {
      throw new Error('No sync rules available');
    }
    if (cp.lsn && cp.lsn >= lsn) {
      return cp.checkpoint;
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error(`Timeout while waiting for checkpoint ${lsn}. Last checkpoint: ${lastCp?.lsn}`);
}
