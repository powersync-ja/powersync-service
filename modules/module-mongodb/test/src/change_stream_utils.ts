import { mongo } from '@powersync/lib-service-mongodb';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  ProtocolOpId,
  ReplicationCheckpoint,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { METRICS_HELPER, test_utils } from '@powersync/service-core-tests';

import { ChangeStream, ChangeStreamOptions } from '@module/replication/ChangeStream.js';
import { MongoManager } from '@module/replication/MongoManager.js';
import { createCheckpoint, STANDALONE_CHECKPOINT_ID } from '@module/replication/MongoRelation.js';
import { NormalizedMongoConnectionConfig } from '@module/types/types.js';

import { TEST_CONNECTION_OPTIONS, clearTestDb } from './util.js';

export class ChangeStreamTestContext {
  private _walStream?: ChangeStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;

  /**
   * Tests operating on the mongo change stream need to configure the stream and manage asynchronous
   * replication, which gets a little tricky.
   *
   * This configures all the context, and tears it down afterwards.
   */
  static async open(factory: () => Promise<BucketStorageFactory>, options?: Partial<NormalizedMongoConnectionConfig>) {
    const f = await factory();
    const connectionManager = new MongoManager({ ...TEST_CONNECTION_OPTIONS, ...options });

    await clearTestDb(connectionManager.db);
    return new ChangeStreamTestContext(f, connectionManager);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MongoManager
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  async dispose() {
    this.abortController.abort();
    await this.streamPromise?.catch((e) => e);
    await this.connectionManager.destroy();
    await this.factory[Symbol.asyncDispose]();
  }

  async [Symbol.asyncDispose]() {
    await this.dispose();
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
    const syncRules = await this.factory.updateSyncRules({ content: content, validate: true });
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
      metrics: METRICS_HELPER.metricsEngine,
      connections: this.connectionManager,
      abort_signal: this.abortController.signal,
      // Specifically reduce this from the default for tests on MongoDB <= 6.0, otherwise it can take
      // a long time to abort the stream.
      maxAwaitTimeMS: 200
    };
    this._walStream = new ChangeStream(options);
    return this._walStream!;
  }

  async replicateSnapshot() {
    await this.walStream.initReplication();
    await this.storage!.autoActivate();
  }

  startStreaming() {
    return (this.streamPromise = this.walStream.streamChanges());
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.client, this.db, this.factory, { timeout: options?.timeout ?? 15_000 }),
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

  async getBucketData(
    bucket: string,
    start?: ProtocolOpId | InternalOpId | undefined,
    options?: { timeout?: number; limit?: number; chunkLimitBytes?: number }
  ) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    let checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, InternalOpId>([[bucket, start]]);
    const batch = this.storage!.getBucketDataBatch(checkpoint, map, {
      limit: options?.limit,
      chunkLimitBytes: options?.chunkLimitBytes
    });
    const batches = await test_utils.fromAsync(batch);
    return batches[0]?.chunkData.data ?? [];
  }

  async getChecksums(buckets: string[], options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    return this.storage!.getChecksums(checkpoint, buckets);
  }

  async getChecksum(bucket: string, options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    const map = await this.storage!.getChecksums(checkpoint, [bucket]);
    return map.get(bucket);
  }
}

export async function getClientCheckpoint(
  client: mongo.MongoClient,
  db: mongo.Db,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<InternalOpId> {
  const start = Date.now();
  const lsn = await createCheckpoint(client, db, STANDALONE_CHECKPOINT_ID);
  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;
  let lastCp: ReplicationCheckpoint | null = null;

  while (Date.now() - start < timeout) {
    const storage = await storageFactory.getActiveStorage();
    const cp = await storage?.getCheckpoint();
    if (cp == null) {
      throw new Error('No sync rules available');
    }
    lastCp = cp;
    if (cp.lsn && cp.lsn >= lsn) {
      return cp.checkpoint;
    }
    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error(`Timeout while waiting for checkpoint ${lsn}. Last checkpoint: ${lastCp?.lsn}`);
}

export async function setSnapshotHistorySeconds(client: mongo.MongoClient, seconds: number) {
  const { minSnapshotHistoryWindowInSeconds: currentValue } = await client
    .db('admin')
    .command({ getParameter: 1, minSnapshotHistoryWindowInSeconds: 1 });

  await client.db('admin').command({ setParameter: 1, minSnapshotHistoryWindowInSeconds: seconds });

  return {
    async [Symbol.asyncDispose]() {
      await client.db('admin').command({ setParameter: 1, minSnapshotHistoryWindowInSeconds: currentValue });
    }
  };
}
