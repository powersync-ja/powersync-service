import { mongo } from '@powersync/lib-service-mongodb';
import {
  BucketChecksumRequest,
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  OplogEntry,
  ProtocolOpId,
  ReplicationCheckpoint,
  settledPromise,
  SyncRulesBucketStorage,
  TestStorageOptions,
  unsettledPromise
} from '@powersync/service-core';
import { bucketRequest, METRICS_HELPER, test_utils } from '@powersync/service-core-tests';

import { ChangeStream, ChangeStreamOptions } from '@module/replication/ChangeStream.js';
import { MongoManager } from '@module/replication/MongoManager.js';
import { createCheckpoint, STANDALONE_CHECKPOINT_ID } from '@module/replication/MongoRelation.js';
import { NormalizedMongoConnectionConfig } from '@module/types/types.js';

import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';

export class ChangeStreamTestContext {
  private _walStream?: ChangeStream;
  private abortController = new AbortController();
  private settledReplicationPromise?: Promise<PromiseSettledResult<void>>;
  public storage?: SyncRulesBucketStorage;

  /**
   * Tests operating on the mongo change stream need to configure the stream and manage asynchronous
   * replication, which gets a little tricky.
   *
   * This configures all the context, and tears it down afterwards.
   */
  static async open(
    factory: (options: TestStorageOptions) => Promise<BucketStorageFactory>,
    options?: {
      doNotClear?: boolean;
      mongoOptions?: Partial<NormalizedMongoConnectionConfig>;
      streamOptions?: Partial<ChangeStreamOptions>;
    }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new MongoManager({ ...TEST_CONNECTION_OPTIONS, ...options?.mongoOptions });

    if (!options?.doNotClear) {
      await clearTestDb(connectionManager.db);
    }
    return new ChangeStreamTestContext(f, connectionManager, options?.streamOptions);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MongoManager,
    private streamOptions?: Partial<ChangeStreamOptions>
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  /**
   * Abort snapshot and/or replication, without actively closing connections.
   */
  abort(cause?: Error) {
    this.abortController.abort(cause);
  }

  async dispose() {
    this.abort(new Error('Disposing test context'));
    await this.settledReplicationPromise;
    await this.factory[Symbol.asyncDispose]();
    await this.connectionManager.end();
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

  async loadNextSyncRules() {
    const syncRules = await this.factory.getNextSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Next sync rules not available`);
    }

    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  get streamer() {
    if (this.storage == null) {
      throw new Error('updateSyncRules() first');
    }
    if (this._walStream) {
      return this._walStream;
    }
    const options: ChangeStreamOptions = {
      factory: this.factory,
      streams: [{ storage: this.storage }],
      metrics: METRICS_HELPER.metricsEngine,
      connections: this.connectionManager,
      abort_signal: this.abortController.signal,
      logger: this.streamOptions?.logger,
      // Specifically reduce this from the default for tests on MongoDB <= 6.0, otherwise it can take
      // a long time to abort the stream.
      maxAwaitTimeMS: this.streamOptions?.maxAwaitTimeMS ?? 200,
      snapshotChunkLength: this.streamOptions?.snapshotChunkLength
    };
    this._walStream = new ChangeStream(options);
    return this._walStream!;
  }

  /**
   * Replicate a snapshot, start streaming, and wait for a consistent checkpoint.
   */
  async initializeReplication() {
    await this.replicateSnapshot();
    // Make sure we're up to date
    await this.getCheckpoint();
  }

  /**
   * Replicate the initial snapshot, and start streaming.
   */
  async replicateSnapshot() {
    // Use a settledPromise to avoid unhandled rejections
    this.settledReplicationPromise ??= settledPromise(this.streamer.replicate());
    try {
      await Promise.race([unsettledPromise(this.settledReplicationPromise), this.streamer.waitForInitialSnapshot()]);
    } catch (e) {
      if (e instanceof ReplicationAbortedError && e.cause != null) {
        // Edge case for tests: replicate() can throw an error, but we'd receive the ReplicationAbortedError from
        // waitForInitialSnapshot() first. In that case, prioritize the cause.
        throw e.cause;
      }
      throw e;
    }
  }

  /**
   * A snapshot is not consistent until streaming replication has caught up.
   * We simulate that for tests.
   * Do not use if there are any writes performed while doing the snapshot, as that
   * would result in inconsistent data.
   */
  async markSnapshotConsistent() {
    const checkpoint = await createCheckpoint(this.client, this.db, STANDALONE_CHECKPOINT_ID);

    await this.storage!.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.keepalive(checkpoint);
    });
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.client, this.db, this.factory, { timeout: options?.timeout ?? 15_000 }),
      unsettledPromise(this.settledReplicationPromise!)
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
    const syncRules = this.storage!.getParsedSyncRules({ defaultSchema: 'n/a' });
    const map = Object.entries(buckets).map(([bucket, start]) => bucketRequest(syncRules, bucket, start));
    return test_utils.fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  async getBucketData(bucket: string, start?: ProtocolOpId | InternalOpId | undefined, options?: { timeout?: number }) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    const syncRules = this.storage!.getParsedSyncRules({ defaultSchema: 'n/a' });
    const checkpoint = await this.getCheckpoint(options);
    let map = [bucketRequest(syncRules, bucket, start)];
    let data: OplogEntry[] = [];
    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);

      const batches = await test_utils.fromAsync(batch);
      data = data.concat(batches[0]?.chunkData.data ?? []);
      if (batches.length == 0 || !batches[0]!.chunkData.has_more) {
        break;
      }
      map = [bucketRequest(syncRules, bucket, BigInt(batches[0]!.chunkData.next_after))];
    }
    return data;
  }

  async getChecksum(request: BucketChecksumRequest, options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    const map = await this.storage!.getChecksums(checkpoint, [request]);
    return map.get(request.bucket);
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
    if (cp != null) {
      lastCp = cp;
      if (cp.lsn && cp.lsn >= lsn) {
        return cp.checkpoint;
      }
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
