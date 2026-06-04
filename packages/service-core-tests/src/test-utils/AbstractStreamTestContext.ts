import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import {
  BucketStorageFactory,
  InternalOpId,
  settledPromise,
  storage,
  SyncRulesBucketStorage,
  unsettledPromise,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
import { StorageDataHelpers } from './StorageDataHelpers.js';
import { bucketRequest } from './general-utils.js';
import { fromAsync } from './stream_utils.js';

export abstract class AbstractStreamTestContext implements AsyncDisposable {
  protected abortController = new AbortController();
  protected syncRulesContent?: storage.PersistedSyncRulesContent;
  public storage?: SyncRulesBucketStorage;
  protected settledReplicationPromise?: Promise<PromiseSettledResult<void>>;

  abstract get factory(): BucketStorageFactory;
  protected abstract get storageVersion(): number;

  async [Symbol.asyncDispose]() {
    await this.dispose();
  }

  protected abstract _dispose(): Promise<void>;

  async dispose() {
    this.abortController.abort();
    try {
      await this.settledReplicationPromise;
      await this._dispose();
      await this.factory?.[Symbol.asyncDispose]();
    } catch (e) {
      // Throwing here may result in SuppressedError. The underlying errors often don't show up
      // in the test output, so we log it here.
      // If we could get vitest to log SuppressedError.error and SuppressedError.suppressed, we
      // could remove this.
      console.error('Error during ConvexStreamTestContext dispose', e);
      throw e;
    }
  }

  async updateSyncRules(content: string) {
    const syncRules = await this.factory.updateSyncRules(
      updateSyncRulesFromYaml(content, { validate: true, storageVersion: this.storageVersion })
    );
    this.syncRulesContent = syncRules;
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncRules = await this.factory.getDeployingSyncConfigContent();
    if (syncRules == null) {
      throw new Error(`Next sync rules not available`);
    }

    this.syncRulesContent = syncRules;
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadActiveSyncRules() {
    const syncRules = await this.factory.getActiveSyncConfigContent();
    if (syncRules == null) {
      throw new Error(`Active sync rules not available`);
    }

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

  /**
   * Replicate a snapshot, start streaming, and wait for a consistent checkpoint.
   */
  async initializeReplication() {
    await this.replicateSnapshot();
    // Make sure we're up to date
    await this.getCheckpoint();
  }

  protected abstract triggerReplication(): Promise<void>;
  protected abstract waitForInitialSnapshot(): Promise<void>;

  /**
   * Replicate the initial snapshot, and start streaming.
   */
  async replicateSnapshot() {
    // Use a settledPromise to avoid unhandled rejections
    this.settledReplicationPromise = settledPromise(this.triggerReplication());
    try {
      await Promise.race([unsettledPromise(this.settledReplicationPromise), this.waitForInitialSnapshot()]);
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

  abstract getClientCheckpoint(options?: { timeout?: number }): Promise<bigint>;

  async getCheckpoint(options?: { timeout?: number }) {
    let checkpoint = await Promise.race([
      this.getClientCheckpoint(options),
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
    const helpers = new StorageDataHelpers(this.storage!, this.getSyncRulesContent());
    const checkpoint = await this.getCheckpoint(options);
    return helpers.getBucketsDataBatch(buckets, checkpoint);
  }

  /**
   * This waits for a client checkpoint.
   */
  async getBucketData(bucket: string, start?: InternalOpId | string | undefined, options?: { timeout?: number }) {
    const helpers = new StorageDataHelpers(this.storage!, this.getSyncRulesContent());
    const checkpoint = await this.getCheckpoint(options);
    return helpers.getBucketData(bucket, checkpoint, start);
  }

  async getChecksums(buckets: string[], options?: { timeout?: number }) {
    const checkpoint = await this.getCheckpoint(options);
    const syncRules = this.getSyncRulesContent();
    const versionedBuckets = buckets.map((bucket) => bucketRequest(syncRules, bucket, 0n));
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
    const syncRules = this.getSyncRulesContent();
    const { checkpoint } = await this.storage!.getCheckpoint();
    const map = [bucketRequest(syncRules, bucket, start)];
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await fromAsync(batch);
    return batches[0]?.chunkData.data ?? [];
  }
}
