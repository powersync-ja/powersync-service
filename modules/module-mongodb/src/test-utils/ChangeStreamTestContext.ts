import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import {
  BucketDataRequest,
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  isBatchEnd,
  LEGACY_STORAGE_VERSION,
  OplogEntry,
  ProtocolOpId,
  ReplicationCheckpoint,
  settledPromise,
  storage,
  SyncRulesBucketStorage,
  TestStorageFactory,
  unsettledPromise,
  updateSyncRulesFromConfig,
  utils
} from '@powersync/service-core';
import { setTimeout } from 'node:timers/promises';

import { SentinelLSN } from '../common/SentinelLSN.js';
import { ChangeStream, ChangeStreamOptions } from '../replication/ChangeStream.js';
import { MongoManager } from '../replication/MongoManager.js';
import {
  createCheckpoint,
  createSentinelCheckpointLsn,
  STANDALONE_CHECKPOINT_ID
} from '../replication/MongoRelation.js';
import { detectDocumentDb } from '../replication/replication-utils.js';
import { NormalizedMongoConnectionConfig } from '../types/types.js';

/**
 * Stream settings callers may override while the context owns connections, storage, metrics and cancellation.
 */
export type TestChangeStreamOptions = Omit<ChangeStreamOptions, 'connections' | 'storage' | 'metrics' | 'abort_signal'>;

export interface ChangeStreamTestContextOptions {
  factory: TestStorageFactory;
  connectionOptions: NormalizedMongoConnectionConfig;
  metrics: ChangeStreamOptions['metrics'];
  /**
   * Preserve both source data and bucket storage when reopening an interrupted replication attempt.
   */
  doNotClear?: boolean;
  storageVersion?: number;
  streamOptions?: TestChangeStreamOptions;
  /**
   * Defaults to detecting DocumentDB from the source's hello response.
   */
  documentDbMode?: boolean;
}

export interface ChangeStreamTestContextResources {
  factory: BucketStorageFactory;
  connectionManager: MongoManager;
  metrics: ChangeStreamOptions['metrics'];
  storageVersion?: number;
  streamOptions?: TestChangeStreamOptions;
  documentDbMode?: boolean;
}

/**
 * Owns source/storage connections and real replication. Callers supply their environment and storage implementation.
 */
export class ChangeStreamTestContext implements AsyncDisposable {
  private _walStream?: ChangeStream;
  private abortController = new AbortController();
  private settledReplicationPromise?: Promise<PromiseSettledResult<void>>;
  private syncRulesContent?: storage.PersistedSyncConfigContent;
  private readonly replicationLeases = new Map<number, Promise<storage.ReplicationLock>>();
  public storage?: SyncRulesBucketStorage;

  /**
   * Open a fresh fixture by default; use doNotClear to resume its persisted source and storage state.
   */
  static async open(options: ChangeStreamTestContextOptions): Promise<ChangeStreamTestContext> {
    return this.openWith({ options, createContext: (resources) => new ChangeStreamTestContext(resources) });
  }

  /**
   * Share resource setup and failure cleanup with subclasses that supply their own context and defaults.
   */
  protected static async openWith<T extends ChangeStreamTestContext>({
    options,
    createContext
  }: {
    options: ChangeStreamTestContextOptions;
    createContext: (resources: ChangeStreamTestContextResources) => T | Promise<T>;
  }): Promise<T> {
    const factory = await options.factory({ doNotClear: options.doNotClear });
    try {
      const connectionManager = new MongoManager(options.connectionOptions);
      try {
        if (!options.doNotClear) {
          await connectionManager.db.dropDatabase();
        }
        const documentDbMode = options.documentDbMode ?? (await detectDocumentDb(connectionManager.db));
        // Await subclass setup so failures still close the shared connections below.
        return await createContext({ ...options, factory, connectionManager, documentDbMode });
      } catch (error) {
        await connectionManager.end();
        throw error;
      }
    } catch (error) {
      await factory[Symbol.asyncDispose]();
      throw error;
    }
  }

  readonly factory: BucketStorageFactory;
  readonly connectionManager: MongoManager;
  protected readonly metrics: ChangeStreamOptions['metrics'];
  protected readonly streamOptions: TestChangeStreamOptions;
  protected readonly storageVersion: number;
  private readonly documentDbMode: boolean;

  /**
   * Takes ownership of an already configured storage factory and source connection.
   */
  constructor({
    factory,
    connectionManager,
    metrics,
    streamOptions = {},
    storageVersion = LEGACY_STORAGE_VERSION,
    documentDbMode = false
  }: ChangeStreamTestContextResources) {
    this.factory = factory;
    this.connectionManager = connectionManager;
    this.metrics = metrics;
    this.streamOptions = streamOptions;
    this.storageVersion = storageVersion;
    this.documentDbMode = documentDbMode;
    createCoreReplicationMetrics(metrics);
    initializeCoreReplicationMetrics(metrics);
    const unregister = factory.registerListener({
      beforeDispose: async () => {
        unregister();
        try {
          for (const lease of this.replicationLeases.values()) {
            await (await lease).release();
          }
        } finally {
          this.replicationLeases.clear();
        }
      }
    });
  }

  /**
   * Abort snapshot and/or replication, without actively closing connections.
   */
  abort(cause?: Error) {
    this.abortController.abort(cause);
  }

  /**
   * Drain replication before inspecting its final persisted state or closing connections.
   */
  async stop(cause?: Error) {
    this.abort(cause);
    await this.settledReplicationPromise;
  }

  async dispose() {
    await this.stop(new Error('Disposing test context'));
    try {
      await this.factory[Symbol.asyncDispose]();
    } finally {
      await this.connectionManager.end();
    }
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
      // Deployment and resume must use the same parser, including caller-registered extensions.
      updateSyncRulesFromConfig(
        this.factory.syncConfigParser.parseContent(content, {
          defaultSchema: this.db.databaseName,
          throwOnError: true
        }),
        { storageVersion: this.storageVersion, defaultSchema: this.db.databaseName }
      )
    );
    this.syncRulesContent = replicationStream.syncConfigContent[0];
    this.storage = await this.getWritableStorage(replicationStream);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncConfig = await this.factory.getDeployingSyncConfig();
    if (syncConfig == null) {
      throw new Error(`Next sync config not available`);
    }

    this.syncRulesContent = syncConfig.content;
    this.storage = await this.getWritableStorage(syncConfig.replicationStream);
    return this.storage!;
  }

  async loadActiveSyncRules() {
    const syncConfig = await this.factory.getActiveSyncConfig();
    if (syncConfig == null) {
      throw new Error(`Active sync config not found`);
    }

    this.syncRulesContent = syncConfig.content;
    this.storage = await this.getWritableStorage(syncConfig.replicationStream);
    return this.storage!;
  }

  /**
   * Reuse a replication lease per stream until the owning factory is disposed.
   */
  private async getWritableStorage(stream: storage.PersistedReplicationStream): Promise<SyncRulesBucketStorage> {
    let pending = this.replicationLeases.get(stream.replicationStreamId);
    if (pending == null) {
      pending = stream.lock();
      this.replicationLeases.set(stream.replicationStreamId, pending);
    }
    let replicationLock: storage.ReplicationLock;
    try {
      replicationLock = await pending;
    } catch (error) {
      this.replicationLeases.delete(stream.replicationStreamId);
      throw error;
    }
    return this.factory.getInstance(stream, { replicationLock });
  }

  getSyncConfigContent(): storage.PersistedSyncConfigContent {
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
      // Forward all supported stream options, including custom query providers and snapshot/storage hooks.
      ...this.streamOptions,
      storage: this.storage,
      metrics: this.metrics,
      connections: this.connectionManager,
      abort_signal: this.abortController.signal,
      // A shorter await also keeps aborts responsive on MongoDB <= 6.0.
      maxAwaitTimeMS: this.streamOptions.maxAwaitTimeMS ?? 200
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
    if (this.documentDbMode) {
      const sentinelCheckpoint = SentinelLSN.fromSerialized(await createSentinelCheckpointLsn(this.client, this.db));
      const status = await this.storage!.getStatus();
      const resumeFrom = status.resumeLsn;
      const resumeToken = resumeFrom ? SentinelLSN.fromSerialized(resumeFrom).resumeToken : null;

      // This helper artificially marks the snapshot as consistent without
      // waiting for the stream to observe the sentinel. Keep the sentinel as the
      // comparable position, but carry forward the existing snapshot resume
      // token so later DocumentDB streaming still resumes from a real token.
      checkpoint = new SentinelLSN({
        sentinel: sentinelCheckpoint.sentinel,
        resume_token: resumeToken
      }).comparable;
    } else {
      checkpoint = await createCheckpoint(this.db, STANDALONE_CHECKPOINT_ID);
    }

    await using writer = await this.storage!.createWriter({
      zeroLSN: '0000000000000000',
      defaultSchema: this.db.databaseName,
      storeCurrentData: true
    });
    await writer.keepalive(checkpoint);
    await writer.flush();
  }

  startStreaming() {
    this.settledReplicationPromise ??= settledPromise(this.streamer.replicate());
    return this.settledReplicationPromise;
  }

  /**
   * Wait for this stream to publish a real source marker; stop polling if replication exits first.
   */
  async getCheckpoint(options?: { timeout?: number }): Promise<ReplicationCheckpoint> {
    if (this.settledReplicationPromise == null) {
      throw new Error('Start replication before requesting a checkpoint.');
    }
    const polling = new AbortController();
    const wait = getClientCheckpoint({
      client: this.client,
      db: this.db,
      storageFactory: this.factory,
      storage: this.storage,
      timeout: options?.timeout ?? 15_000,
      documentDbMode: this.documentDbMode,
      signal: polling.signal
    });
    try {
      return await Promise.race([
        wait,
        unsettledPromise(this.settledReplicationPromise).then(() => {
          throw new Error('Replication exited before publishing the requested checkpoint.');
        })
      ]);
    } finally {
      polling.abort();
      await wait.catch(() => {});
    }
  }

  async getBucketsDataBatch(buckets: Record<string, InternalOpId>, options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    const map = Object.entries(buckets).map(([bucket, start]) => this.bucketRequest(bucket, start));
    const chunks = [];
    for await (const chunk of this.storage!.getBucketDataBatch(checkpoint, map)) {
      chunks.push(chunk);
    }
    return chunks;
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
    return this.getBucketDataAtCheckpoint(bucket, checkpoint, start);
  }

  async getBucketDataAtCheckpoint(
    bucket: string,
    checkpoint: ReplicationCheckpoint,
    start?: ProtocolOpId | InternalOpId | undefined
  ) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    let map = [this.bucketRequest(bucket, start)];
    let data: OplogEntry[] = [];
    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);

      const chunks = [];
      for await (const chunk of batch) {
        chunks.push(chunk);
      }
      if (chunks.length == 0) {
        break;
      }
      for (let chunk of chunks) {
        if (isBatchEnd(chunk)) {
          if (!chunk.hasMore) {
            return data;
          }
        } else {
          data = data.concat(chunk.chunkData.data ?? []);
          map = [this.bucketRequest(bucket, BigInt(chunk.chunkData.next_after))];
          if (!chunk.chunkData.has_more) {
            return data;
          }
        }
      }
    }
    return data;
  }

  /**
   * Resolve a readable test bucket such as global[] to its persisted, versioned bucket name.
   */
  private bucketRequest(bucket: string, start: InternalOpId): BucketDataRequest {
    const parsed = this.getSyncConfigContent().parsed({ defaultSchema: this.db.databaseName });
    const parameterStart = bucket.indexOf('[');
    const definitionName = bucket.substring(0, parameterStart);
    const source = parsed.syncConfigs
      .flatMap((config) => config.config.bucketDataSources)
      .find((source) => source.uniqueName == definitionName);
    if (source == null) {
      throw new Error(`Unknown bucket '${bucket}'.`);
    }
    return {
      bucket: parsed.hydrationState.getBucketSourceScope(source).bucketPrefix + bucket.substring(parameterStart),
      source,
      start
    };
  }

  async getChecksums(buckets: string[], options?: { timeout?: number }): Promise<utils.ChecksumMap> {
    let checkpoint = await this.getCheckpoint(options);
    const versionedBuckets = buckets.map((bucket) => this.bucketRequest(bucket, 0n));
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

export async function getClientCheckpoint({
  client,
  db,
  storageFactory,
  timeout = 50_000,
  documentDbMode: forcedDocumentDbMode,
  storage: expectedStorage,
  signal
}: {
  client: mongo.MongoClient;
  db: mongo.Db;
  storageFactory: BucketStorageFactory;
  timeout?: number;
  documentDbMode?: boolean;
  /**
   * Ignore checkpoints from another active stream while this one is still deploying.
   */
  storage?: SyncRulesBucketStorage;
  signal?: AbortSignal;
}): Promise<ReplicationCheckpoint> {
  const start = Date.now();
  const documentDbMode = forcedDocumentDbMode ?? (await detectDocumentDb(db));

  const lsn = documentDbMode
    ? await createSentinelCheckpointLsn(client, db)
    : await createCheckpoint(db, STANDALONE_CHECKPOINT_ID);
  // The marker establishes a source position. Wait until replication publishes a client-visible checkpoint past it.

  // DocumentDB: the streaming loop skips standalone checkpoint events while a
  // batch barrier is pending (see ChangeStream.ts), so a single sentinel bump
  // can be missed on an idle stream with no later event to advance the
  // checkpoint. Periodically re-bump the sentinel so a standalone event
  // eventually commits past `lsn`, mirroring getSnapshotLsn's retry loop.
  const NUDGE_INTERVAL_MS = 1000;
  let lastNudge = Date.now();
  let lastCp: ReplicationCheckpoint | null = null;

  while (Date.now() - start < timeout) {
    signal?.throwIfAborted();
    const activeStorage = (await storageFactory.getActiveSyncConfig())?.storage;
    const cp =
      expectedStorage == null || activeStorage?.replicationStreamId == expectedStorage.replicationStreamId
        ? await activeStorage?.getCheckpoint()
        : undefined;
    if (cp != null) {
      lastCp = cp;
      if (cp.lsn && cp.lsn >= lsn) {
        return cp;
      }
    }

    if (documentDbMode && Date.now() - lastNudge >= NUDGE_INTERVAL_MS) {
      await createSentinelCheckpointLsn(client, db);
      lastNudge = Date.now();
    }

    await setTimeout(30, undefined, { signal });
  }

  throw new Error(`Timeout while waiting for checkpoint ${lsn}. Last checkpoint: ${lastCp?.lsn}`);
}
