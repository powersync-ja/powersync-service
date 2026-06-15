import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  LEGACY_STORAGE_VERSION,
  OplogEntry,
  ProtocolOpId,
  ReplicationCheckpoint,
  settledPromise,
  storage,
  SyncRulesBucketStorage,
  TestStorageOptions,
  unsettledPromise,
  updateSyncRulesFromYaml,
  utils
} from '@powersync/service-core';
import { bucketRequest, METRICS_HELPER, test_utils } from '@powersync/service-core-tests';

import { CosmosDBLSN } from '@module/common/CosmosDBLSN.js';
import { ChangeStream, ChangeStreamOptions } from '@module/replication/ChangeStream.js';
import { MongoManager } from '@module/replication/MongoManager.js';
import {
  createCheckpoint,
  createCosmosCheckpointLsn,
  STANDALONE_CHECKPOINT_ID
} from '@module/replication/MongoRelation.js';
import { NormalizedMongoConnectionConfig } from '@module/types/types.js';

import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';

export class ChangeStreamTestContext {
  private _walStream?: ChangeStream;
  private abortController = new AbortController();
  private settledReplicationPromise?: Promise<PromiseSettledResult<void>>;
  private syncRulesContent?: storage.PersistedSyncConfigContent;
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
      /**
       * Optional override for tests that need to force a mode. By default the
       * test context detects Cosmos DB from the hello response.
       */
      cosmosDbMode?: boolean;
    }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new MongoManager({ ...TEST_CONNECTION_OPTIONS, ...options?.mongoOptions });

    if (!options?.doNotClear) {
      await clearTestDb(connectionManager.db);
    }

    const storageVersion = options?.storageVersion ?? LEGACY_STORAGE_VERSION;
    const cosmosDbMode = options?.cosmosDbMode ?? (await detectCosmosDb(connectionManager.db));

    return new ChangeStreamTestContext(f, connectionManager, options?.streamOptions, storageVersion, cosmosDbMode);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MongoManager,
    private streamOptions: Partial<ChangeStreamOptions> = {},
    private storageVersion: number = LEGACY_STORAGE_VERSION,
    private cosmosDbMode: boolean = false

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
    const replicationStream = await this.factory.updateSyncRules(
      updateSyncRulesFromYaml(content, { validate: true, storageVersion: this.storageVersion })
    );
    this.syncRulesContent = replicationStream.syncConfigContent[0];
    this.storage = this.factory.getInstance(replicationStream);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncConfig = await this.factory.getDeployingSyncConfig();
    if (syncConfig == null) {
      throw new Error(`Next sync config not available`);
    }

    this.syncRulesContent = syncConfig.content;
    this.storage = syncConfig.storage;
    return this.storage!;
  }

  async loadActiveSyncRules() {
    const syncConfig = await this.factory.getActiveSyncConfig();
    if (syncConfig == null) {
      throw new Error(`Active sync config not found`);
    }

    this.syncRulesContent = syncConfig.content;
    this.storage = syncConfig.storage;
    return this.storage!;
  }

  private getSyncConfigContent(): storage.PersistedSyncConfigContent {
    if (this.syncRulesContent == null) {
      throw new Error('Sync config not configured - call updateSyncRules() first');
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
      keepaliveIntervalMs: this.streamOptions?.keepaliveIntervalMs,
      storageHooks: this.streamOptions?.storageHooks,
      snapshotHooks: this.streamOptions?.snapshotHooks,
      logger: this.streamOptions?.logger
    };
    this._walStream = new ChangeStream(options);
    return this._walStream!;
  }

  async replicateSnapshot() {
    this.settledReplicationPromise ??= settledPromise(this.streamer.replicate());
    try {
      await Promise.race([unsettledPromise(this.settledReplicationPromise), this.streamer.waitForInitialSnapshot()]);
    } catch (e) {
      if (e instanceof ReplicationAbortedError && e.cause != null) {
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
    let checkpoint: string;
    if (this.cosmosDbMode) {
      const sentinelCheckpoint = CosmosDBLSN.fromSerialized(await createCosmosCheckpointLsn(this.client, this.db));
      const status = await this.storage!.getStatus();
      const resumeFrom = status.checkpoint_lsn ?? status.snapshot_lsn;
      const resumeToken = resumeFrom ? CosmosDBLSN.fromSerialized(resumeFrom).resumeToken : null;

      // This helper artificially marks the snapshot as consistent without
      // waiting for the stream to observe the sentinel. Keep the sentinel as the
      // comparable position, but carry forward the existing snapshot resume
      // token so later Cosmos streaming still resumes from a real token.
      checkpoint = new CosmosDBLSN({
        sentinel: sentinelCheckpoint.sentinel,
        resume_token: resumeToken
      }).comparable;
    } else {
      checkpoint = await createCheckpoint(this.client, this.db, STANDALONE_CHECKPOINT_ID);
    }

    await using writer = await this.storage!.createWriter(test_utils.BATCH_OPTIONS);
    await writer.keepalive(checkpoint);
    await writer.flush();
  }

  startStreaming() {
    this.settledReplicationPromise ??= settledPromise(this.streamer.replicate());
    return this.settledReplicationPromise;
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.client, this.db, this.factory, { timeout: options?.timeout ?? 15_000 }),
      this.settledReplicationPromise == null ? undefined : unsettledPromise(this.settledReplicationPromise)
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
    const syncConfigContent = this.getSyncConfigContent();
    const map = Object.entries(buckets).map(([bucket, start]) => bucketRequest(syncConfigContent, bucket, start));
    return test_utils.fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  async getBucketData(bucket: string, start?: ProtocolOpId | InternalOpId | undefined, options?: { timeout?: number }) {
    const checkpoint = await this.getCheckpoint(options);
    return this.getBucketDataAtCheckpoint(bucket, checkpoint, start);
  }

  async getBucketDataAtLatestCheckpoint(bucket: string, start?: ProtocolOpId | InternalOpId | undefined) {
    if (this.storage == null) {
      throw new Error('updateSyncRules() first');
    }

    const checkpoint = await this.storage.getCheckpoint();
    return this.getBucketDataAtCheckpoint(bucket, checkpoint.checkpoint, start);
  }

  async getBucketDataAtCheckpoint(
    bucket: string,
    checkpoint: InternalOpId,
    start?: ProtocolOpId | InternalOpId | undefined
  ) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    const syncConfigContent = this.getSyncConfigContent();
    let map = [bucketRequest(syncConfigContent, bucket, start)];
    let data: OplogEntry[] = [];
    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);

      const batches = await test_utils.fromAsync(batch);
      data = data.concat(batches[0]?.chunkData.data ?? []);
      if (batches.length == 0 || !batches[0]!.chunkData.has_more) {
        break;
      }
      map = [bucketRequest(syncConfigContent, bucket, BigInt(batches[0]!.chunkData.next_after))];
    }
    return data;
  }

  async getChecksums(buckets: string[], options?: { timeout?: number }): Promise<utils.ChecksumMap> {
    let checkpoint = await this.getCheckpoint(options);
    const syncConfigContent = this.getSyncConfigContent();
    const versionedBuckets = buckets.map((bucket) => bucketRequest(syncConfigContent, bucket, 0n));
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

async function detectCosmosDb(db: mongo.Db) {
  const hello = await db.command({ hello: 1 });
  return hello.internal?.cosmos_versions != null || hello.internal?.documentdb_versions != null;
}

export async function getClientCheckpoint(
  client: mongo.MongoClient,
  db: mongo.Db,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number; cosmosDbMode?: boolean; storage?: SyncRulesBucketStorage }
): Promise<InternalOpId> {
  const start = Date.now();
  const cosmosDbMode = options?.cosmosDbMode ?? (await detectCosmosDb(db));

  const lsn = cosmosDbMode
    ? await createCosmosCheckpointLsn(client, db)
    : await createCheckpoint(client, db, STANDALONE_CHECKPOINT_ID);
  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;
  let lastCosmosCheckpointCreated = Date.now();
  let lastCp: ReplicationCheckpoint | null = null;

  while (Date.now() - start < timeout) {
    const storage = (await storageFactory.getActiveSyncConfig())?.storage;
    const cp = await storage?.getCheckpoint();
    if (cp != null) {
      lastCp = cp;
      if (cp.lsn && cp.lsn >= lsn) {
        return cp.checkpoint;
      }
    }

    if (cosmosDbMode && Date.now() - lastCosmosCheckpointCreated >= 1_000) {
      // Cosmos streams can open from "now" when the stored LSN has no resume
      // token. If the first standalone checkpoint was written before the
      // cursor was actually established, nudge the stream with another
      // sentinel. Keep waiting for the original target LSN: any later sentinel
      // commit will compare greater than it.
      await createCosmosCheckpointLsn(client, db);
      lastCosmosCheckpointCreated = Date.now();
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
