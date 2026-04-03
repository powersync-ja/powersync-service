import { mongo } from '@powersync/lib-service-mongodb';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  LEGACY_STORAGE_VERSION,
  OplogEntry,
  ProtocolOpId,
  ReplicationCheckpoint,
  storage,
  STORAGE_VERSION_CONFIG,
  SyncRulesBucketStorage,
  TestStorageOptions,
  updateSyncRulesFromYaml,
  utils
} from '@powersync/service-core';
import { bucketRequest, METRICS_HELPER, test_utils } from '@powersync/service-core-tests';

import { ChangeStream, ChangeStreamOptions } from '@module/replication/ChangeStream.js';
import { MongoManager } from '@module/replication/MongoManager.js';
import { createCheckpoint, STANDALONE_CHECKPOINT_ID } from '@module/replication/MongoRelation.js';
import { NormalizedMongoConnectionConfig } from '@module/types/types.js';

import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';

export class ChangeStreamTestContext {
  private _walStream?: ChangeStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<PromiseSettledResult<void>>;
  private syncRulesId?: number;
  private syncRulesContent?: storage.PersistedSyncRulesContent;
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
      storageVersion?: number;
      mongoOptions?: Partial<NormalizedMongoConnectionConfig>;
      streamOptions?: Partial<ChangeStreamOptions>;
    }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new MongoManager({ ...TEST_CONNECTION_OPTIONS, ...options?.mongoOptions });

    if (!options?.doNotClear) {
      await clearTestDb(connectionManager.db);
    }

    const storageVersion = options?.storageVersion ?? LEGACY_STORAGE_VERSION;
    const versionedBuckets = STORAGE_VERSION_CONFIG[storageVersion]?.versionedBuckets ?? false;

    return new ChangeStreamTestContext(f, connectionManager, options?.streamOptions, storageVersion, versionedBuckets);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MongoManager,
    private streamOptions: Partial<ChangeStreamOptions> = {},
    private storageVersion: number = LEGACY_STORAGE_VERSION,
    private versionedBuckets: boolean = STORAGE_VERSION_CONFIG[storageVersion]?.versionedBuckets ?? false
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  /**
   * Abort snapshot and/or replication, without actively closing connections.
   */
  abort() {
    this.abortController.abort();
  }

  async dispose() {
    this.abort();
    await this.streamPromise?.catch((e) => e);
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
    const syncRules = await this.factory.updateSyncRules(
      updateSyncRulesFromYaml(content, { validate: true, storageVersion: this.storageVersion })
    );
    this.syncRulesId = syncRules.id;
    this.syncRulesContent = syncRules;
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncRules = await this.factory.getNextSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Next sync rules not available`);
    }

    this.syncRulesId = syncRules.id;
    this.syncRulesContent = syncRules;
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  private getSyncRulesContent(): storage.PersistedSyncRulesContent {
    if (this.syncRulesContent == null) {
      throw new Error('Sync rules not configured - call updateSyncRules() first');
    }
    return this.syncRulesContent;
  }

  get streamer() {
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
      maxAwaitTimeMS: this.streamOptions?.maxAwaitTimeMS ?? 200,
      snapshotChunkLength: this.streamOptions?.snapshotChunkLength,
      cosmosDbMode: this.streamOptions?.cosmosDbMode,
      keepaliveIntervalMs: this.streamOptions?.keepaliveIntervalMs
    };
    this._walStream = new ChangeStream(options);
    return this._walStream!;
  }

  async replicateSnapshot() {
    await this.streamer.initReplication();
  }

  /**
   * A snapshot is not consistent until streaming replication has caught up.
   * We simulate that for tests.
   * Do not use if there are any writes performed while doing the snapshot, as that
   * would result in inconsistent data.
   */
  async markSnapshotConsistent() {
    const checkpoint = await createCheckpoint(this.client, this.db, STANDALONE_CHECKPOINT_ID);

    await using writer = await this.storage!.createWriter(test_utils.BATCH_OPTIONS);
    await writer.keepalive(checkpoint);
    await writer.flush();
  }

  startStreaming() {
    this.streamPromise = this.streamer
      .streamChanges()
      .then(() => ({ status: 'fulfilled', value: undefined }) satisfies PromiseFulfilledResult<void>)
      .catch((reason) => ({ status: 'rejected', reason }) satisfies PromiseRejectedResult);
    return this.streamPromise;
  }

  async getCheckpoint(options?: { timeout?: number }) {
    const isCosmosDb = this.streamOptions?.cosmosDbMode ?? false;
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.client, this.db, this.factory, {
        timeout: options?.timeout ?? 15_000,
        isCosmosDb
      }),
      this.streamPromise?.then((e) => {
        if (e.status == 'rejected') {
          throw e.reason;
        }
      })
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
    const syncRules = this.getSyncRulesContent();
    const map = Object.entries(buckets).map(([bucket, start]) => bucketRequest(syncRules, bucket, start));
    return test_utils.fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  async getBucketData(bucket: string, start?: ProtocolOpId | InternalOpId | undefined, options?: { timeout?: number }) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    const syncRules = this.getSyncRulesContent();
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

  async getChecksums(buckets: string[], options?: { timeout?: number }): Promise<utils.ChecksumMap> {
    let checkpoint = await this.getCheckpoint(options);
    const syncRules = this.getSyncRulesContent();
    const versionedBuckets = buckets.map((bucket) => bucketRequest(syncRules, bucket, 0n));
    const checksums = await this.storage!.getChecksums(checkpoint, versionedBuckets);

    const unversioned: utils.ChecksumMap = new Map();
    for (let i = 0; i < buckets.length; i++) {
      unversioned.set(buckets[i], checksums.get(versionedBuckets[i].bucket)!);
    }
    return unversioned;
  }

  async getChecksum(bucket: string, options?: { timeout?: number }) {
    const checksums = await this.getChecksums([bucket], options);
    return checksums.get(bucket);
  }
}

export async function getClientCheckpoint(
  client: mongo.MongoClient,
  db: mongo.Db,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number; isCosmosDb?: boolean }
): Promise<InternalOpId> {
  const start = Date.now();
  const lsn = await createCheckpoint(client, db, STANDALONE_CHECKPOINT_ID, {
    isCosmosDb: options?.isCosmosDb
  });
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
