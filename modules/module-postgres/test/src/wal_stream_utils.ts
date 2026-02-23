import { PgManager } from '@module/replication/PgManager.js';
import { PUBLICATION_NAME, WalStream, WalStreamOptions } from '@module/replication/WalStream.js';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  LEGACY_STORAGE_VERSION,
  OplogEntry,
  settledPromise,
  storage,
  STORAGE_VERSION_CONFIG,
  SyncRulesBucketStorage,
  unsettledPromise
} from '@powersync/service-core';
import { bucketRequest, METRICS_HELPER, test_utils } from '@powersync/service-core-tests';
import * as pgwire from '@powersync/service-jpgwire';
import { clearTestDb, getClientCheckpoint, TEST_CONNECTION_OPTIONS } from './util.js';

export class WalStreamTestContext implements AsyncDisposable {
  private _walStream?: WalStream;
  private abortController = new AbortController();
  private syncRulesId?: number;
  public storage?: SyncRulesBucketStorage;
  private settledReplicationPromise?: Promise<PromiseSettledResult<void>>;

  /**
   * Tests operating on the wal stream need to configure the stream and manage asynchronous
   * replication, which gets a little tricky.
   *
   * This configures all the context, and tears it down afterwards.
   */
  static async open(
    factory: (options: storage.TestStorageOptions) => Promise<BucketStorageFactory>,
    options?: { doNotClear?: boolean; storageVersion?: number; walStreamOptions?: Partial<WalStreamOptions> }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new PgManager(TEST_CONNECTION_OPTIONS, {});

    if (!options?.doNotClear) {
      await clearTestDb(connectionManager.pool);
    }

    const storageVersion = options?.storageVersion ?? LEGACY_STORAGE_VERSION;
    const versionedBuckets = STORAGE_VERSION_CONFIG[storageVersion]?.versionedBuckets ?? false;

    return new WalStreamTestContext(f, connectionManager, options?.walStreamOptions, storageVersion, versionedBuckets);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: PgManager,
    private walStreamOptions?: Partial<WalStreamOptions>,
    private storageVersion: number = LEGACY_STORAGE_VERSION,
    private versionedBuckets: boolean = STORAGE_VERSION_CONFIG[storageVersion]?.versionedBuckets ?? false
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  async [Symbol.asyncDispose]() {
    await this.dispose();
  }

  async dispose() {
    this.abortController.abort();
    try {
      await this.settledReplicationPromise;
      await this.connectionManager.destroy();
      await this.factory?.[Symbol.asyncDispose]();
    } catch (e) {
      // Throwing here may result in SuppressedError. The underlying errors often don't show up
      // in the test output, so we log it here.
      // If we could get vitest to log SuppressedError.error and SuppressedError.suppressed, we
      // could remove this.
      console.error('Error during WalStreamTestContext dispose', e);
      throw e;
    }
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
    const syncRules = await this.factory.updateSyncRules({
      content: content,
      validate: true,
      storageVersion: this.storageVersion
    });
    this.syncRulesId = syncRules.id;
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncRules = await this.factory.getNextSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Next sync rules not available`);
    }

    this.syncRulesId = syncRules.id;
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadActiveSyncRules() {
    const syncRules = await this.factory.getActiveSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Active sync rules not available`);
    }

    this.syncRulesId = syncRules.id;
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
    // Make sure we're up to date
    await this.getCheckpoint();
  }

  /**
   * Replicate the initial snapshot, and start streaming.
   */
  async replicateSnapshot() {
    // Use a settledPromise to avoid unhandled rejections
    this.settledReplicationPromise = settledPromise(this.walStream.replicate());
    try {
      await Promise.race([unsettledPromise(this.settledReplicationPromise), this.walStream.waitForInitialSnapshot()]);
    } catch (e) {
      if (e instanceof ReplicationAbortedError && e.cause != null) {
        // Edge case for tests: replicate() can throw an error, but we'd receive the ReplicationAbortedError from
        // waitForInitialSnapshot() first. In that case, prioritize the cause, e.g. MissingReplicationSlotError.
        // This is not a concern for production use, since we only use waitForInitialSnapshot() in tests.
        throw e.cause;
      }
      throw e;
    }
  }

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      getClientCheckpoint(this.pool, this.factory, { timeout: options?.timeout ?? 15_000 }),
      unsettledPromise(this.settledReplicationPromise!)
    ]);
    if (checkpoint == null) {
      // This indicates an issue with the test setup - replicationPromise completed instead
      // of getClientCheckpoint()
      throw new Error('Test failure - replicationPromise completed');
    }
    return checkpoint;
  }

  async getBucketsDataBatch(buckets: Record<string, InternalOpId>, options?: { timeout?: number }) {
    let checkpoint = await this.getCheckpoint(options);
    const syncRules = this.storage!.getParsedSyncRules({ defaultSchema: 'n/a' });
    const map = Object.entries(buckets).map(([bucket, start]) => bucketRequest(syncRules, bucket, start));
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

  async getChecksums(buckets: string[], options?: { timeout?: number }) {
    const checkpoint = await this.getCheckpoint(options);
    const syncRules = this.storage!.getParsedSyncRules({ defaultSchema: 'n/a' });
    const versionedBuckets = buckets.map((bucket) => bucketRequest(syncRules, bucket));
    const checksums = await this.storage!.getChecksums(checkpoint, versionedBuckets);

    const unversioned = new Map();
    for (let i = 0; i < buckets.length; i++) {
      unversioned.set(buckets[i], checksums.get(versionedBuckets[i].bucket)!);
    }

    return unversioned;
  }

  async getChecksum(bucket: string, options?: { timeout?: number }) {
    const checksums = await this.getChecksums([bucket], options);
    return checksums.get(bucket);
  }

  /**
   * This does not wait for a client checkpoint.
   */
  async getCurrentBucketData(bucket: string, start?: InternalOpId | string | undefined) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    const syncRules = this.storage!.getParsedSyncRules({ defaultSchema: 'n/a' });
    const { checkpoint } = await this.storage!.getCheckpoint();
    const map = [bucketRequest(syncRules, bucket, start)];
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await test_utils.fromAsync(batch);
    return batches[0]?.chunkData.data ?? [];
  }
}

export async function withMaxWalSize(db: pgwire.PgClient, size: string) {
  try {
    const r1 = await db.query(`SHOW max_slot_wal_keep_size`);

    await db.query(`ALTER SYSTEM SET max_slot_wal_keep_size = '100MB'`);
    await db.query(`SELECT pg_reload_conf()`);

    const oldSize = r1.results[0].rows[0].decodeWithoutCustomTypes(0);

    return {
      [Symbol.asyncDispose]: async () => {
        await db.query(`ALTER SYSTEM SET max_slot_wal_keep_size = '${oldSize}'`);
        await db.query(`SELECT pg_reload_conf()`);
      }
    };
  } catch (e) {
    const err = new Error(`Failed to configure max_slot_wal_keep_size for test`);
    err.cause = e;
    throw err;
  }
}
