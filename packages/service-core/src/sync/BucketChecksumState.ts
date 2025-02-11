import { BucketDescription, RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';

import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { BucketParameterQuerier } from '@powersync/service-sync-rules/src/BucketParameterQuerier.js';
import { BucketSyncState } from './sync.js';

/**
 * Represents the state of the checksums and data for a specific connection.
 *
 * Handles incrementally re-computing checkpoints.
 */
export class BucketChecksumState {
  private readonly bucketStorage: BucketChecksumStateStorage;

  /**
   * Bucket state of bucket id -> op_id.
   * This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
   */
  public dataBuckets = new Map<string, BucketSyncState>();

  /**
   * Last checksums sent to the client. We keep this to calculate checkpoint diffs.
   */
  private lastChecksums: util.ChecksumMap | null = null;
  private lastWriteCheckpoint: bigint | null = null;

  private readonly parameterState: BucketParameterState;

  constructor(options: BucketChecksumStateOptions) {
    this.bucketStorage = options.bucketStorage;
    this.parameterState = new BucketParameterState(options.bucketStorage, options.syncRules, options.syncParams);
    this.dataBuckets = options.initialBucketState;
  }

  async buildNextCheckpointLine(next: storage.WriteCheckpoint): Promise<CheckpointLine | null> {
    const { writeCheckpoint, base } = next;
    const user_id = this.parameterState.syncParams.user_id;

    const storage = this.bucketStorage;

    const { buckets: allBuckets, updatedBuckets } = await this.parameterState.getCheckpointUpdate(base.checkpoint);

    let dataBucketsNew = new Map<string, BucketSyncState>();
    for (let bucket of allBuckets) {
      dataBucketsNew.set(bucket.bucket, {
        description: bucket,
        start_op_id: this.dataBuckets.get(bucket.bucket)?.start_op_id ?? '0'
      });
    }
    this.dataBuckets = dataBucketsNew;

    let checksumMap: util.ChecksumMap;
    if (updatedBuckets != null) {
      if (this.lastChecksums == null) {
        throw new ServiceAssertionError(`Bucket diff received without existing checksums`);
      }

      // Re-check updated buckets only
      let checksumLookups: string[] = [];

      let newChecksums = new Map<string, util.BucketChecksum>();
      for (let bucket of dataBucketsNew.keys()) {
        if (!updatedBuckets.has(bucket)) {
          const existing = this.lastChecksums.get(bucket);
          if (existing == null) {
            // If this happens, it means updatedBuckets did not correctly include all new buckets
            throw new ServiceAssertionError(`Existing checksum not found for bucket ${bucket}`);
          }
          // Bucket is not specifically updated, and we have a previous checksum
          newChecksums.set(bucket, existing);
        } else {
          checksumLookups.push(bucket);
        }
      }

      let updatedChecksums = await storage.getChecksums(base.checkpoint, checksumLookups);
      for (let [bucket, value] of updatedChecksums.entries()) {
        newChecksums.set(bucket, value);
      }
      checksumMap = newChecksums;
    } else {
      // Re-check all buckets
      const bucketList = [...dataBucketsNew.keys()];
      checksumMap = await storage.getChecksums(base.checkpoint, bucketList);
    }
    // Subset of buckets for which there may be new data in this batch.
    let bucketsToFetch: BucketDescription[];

    let checkpointLine: util.StreamingSyncCheckpointDiff | util.StreamingSyncCheckpoint;

    if (this.lastChecksums) {
      // TODO: If updatedBuckets is present, we can use that to more efficiently calculate a diff,
      // and avoid any unnecessary loops through the entire list of buckets.
      const diff = util.checksumsDiff(this.lastChecksums, checksumMap);

      if (
        this.lastWriteCheckpoint == writeCheckpoint &&
        diff.removedBuckets.length == 0 &&
        diff.updatedBuckets.length == 0
      ) {
        // No changes - don't send anything to the client
        return null;
      }

      const updatedBucketDescriptions = diff.updatedBuckets.map((e) => ({
        ...e,
        priority: this.dataBuckets.get(e.bucket)!.description!.priority
      }));
      bucketsToFetch = updatedBucketDescriptions;

      let message = `Updated checkpoint: ${base.checkpoint} | `;
      message += `write: ${writeCheckpoint} | `;
      message += `buckets: ${allBuckets.length} | `;
      message += `updated: ${limitedBuckets(diff.updatedBuckets, 20)} | `;
      message += `removed: ${limitedBuckets(diff.removedBuckets, 20)}`;
      logger.info(message, {
        checkpoint: base.checkpoint,
        user_id: user_id,
        buckets: allBuckets.length,
        updated: diff.updatedBuckets.length,
        removed: diff.removedBuckets.length
      });

      checkpointLine = {
        checkpoint_diff: {
          last_op_id: base.checkpoint,
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          removed_buckets: diff.removedBuckets,
          updated_buckets: updatedBucketDescriptions
        }
      } satisfies util.StreamingSyncCheckpointDiff;
    } else {
      let message = `New checkpoint: ${base.checkpoint} | write: ${writeCheckpoint} | `;
      message += `buckets: ${allBuckets.length} ${limitedBuckets(allBuckets, 20)}`;
      logger.info(message, { checkpoint: base.checkpoint, user_id: user_id, buckets: allBuckets.length });
      bucketsToFetch = allBuckets;
      checkpointLine = {
        checkpoint: {
          last_op_id: base.checkpoint,
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          buckets: [...checksumMap.values()].map((e) => ({
            ...e,
            priority: this.dataBuckets.get(e.bucket)!.description!.priority
          }))
        }
      } satisfies util.StreamingSyncCheckpoint;
    }

    this.lastChecksums = checksumMap;
    this.lastWriteCheckpoint = writeCheckpoint;

    return {
      checkpointLine,
      bucketsToFetch
    };
  }

  get checkpointFilter() {
    return this.parameterState.checkpointFilter;
  }

  getFilteredBucketStates(bucketsToFetch: BucketDescription[]): Map<string, string> {
    const filtered = new Map<string, string>();
    for (let bucket of bucketsToFetch) {
      const state = this.dataBuckets.get(bucket.bucket);
      if (state) {
        filtered.set(bucket.bucket, state.start_op_id);
      }
    }
    return filtered;
  }

  updateState(bucket: string, nextAfter: string) {
    const state = this.dataBuckets.get(bucket);
    if (state) {
      state.start_op_id = nextAfter;
    }
  }
}

export interface CheckpointUpdate {
  /**
   * All buckets forming part of the checkpoint.
   */
  buckets: BucketDescription[];

  /**
   * If present, a set of buckets that have been updated since the last checkpoint.
   *
   * If null, assume that any bucket in `buckets` may have been updated.
   */
  updatedBuckets: Set<string> | null;
}

export class BucketParameterState {
  public readonly bucketStorage: BucketChecksumStateStorage;
  public readonly syncRules: SqlSyncRules;
  public readonly syncParams: RequestParameters;
  private readonly querier: BucketParameterQuerier;
  private readonly staticBuckets: Map<string, BucketDescription>;

  private pendingBuckets = new Set<string>();
  // First time we're called, we need to fetch all buckets.
  private invalidated: boolean = true;

  constructor(bucketStorage: BucketChecksumStateStorage, syncRules: SqlSyncRules, syncParams: RequestParameters) {
    this.bucketStorage = bucketStorage;
    this.syncRules = syncRules;
    this.syncParams = syncParams;

    this.querier = syncRules.getBucketParameterQuerier(this.syncParams);
    this.staticBuckets = new Map<string, BucketDescription>(this.querier.staticBuckets.map((b) => [b.bucket, b]));
  }

  checkpointFilter = (event: storage.WatchFilterEvent): boolean => {
    if (this.invalidated) {
      return true;
    } else if (event.invalidate) {
      this.invalidated = true;
      this.pendingBuckets.clear();
      return true;
    } else if (event.bucket != null) {
      const querier = this.querier;
      const staticBuckets = this.staticBuckets;
      if (querier.hasDynamicBuckets) {
        // TODO: Check if the dynamic buckets may match this one
        this.invalidated = true;
        this.pendingBuckets.clear();
        return true;
      }

      if (staticBuckets.has(event.bucket)) {
        this.pendingBuckets.add(event.bucket);
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  };

  async getCheckpointUpdate(checkpoint: util.OpId): Promise<CheckpointUpdate> {
    const querier = this.querier;
    let update: CheckpointUpdate;
    if (querier.hasDynamicBuckets) {
      update = await this.getCheckpointUpdateDynamic(checkpoint);
    } else {
      update = await this.getCheckpointUpdateStatic();
    }

    if (update.buckets.length > 1000) {
      // TODO: Limit number of buckets even before we get to this point
      const error = new ServiceError(ErrorCode.PSYNC_S2305, `Too many buckets: ${update.buckets.length}`);
      logger.error(error.message, {
        checkpoint: checkpoint,
        user_id: this.syncParams.user_id,
        buckets: update.buckets.length
      });

      throw error;
    }
    return update;
  }

  private async getCheckpointUpdateStatic(): Promise<CheckpointUpdate> {
    const querier = this.querier;

    // This case is optimized to track pending buckets.
    const staticBuckets = querier.staticBuckets;
    let updatedBuckets: Set<string> | null = this.pendingBuckets;
    if (this.invalidated) {
      // Invalidation event, for example on a sync rule update.
      // We do not have the individual updated buckets in this case.
      updatedBuckets = null;
    }

    this.pendingBuckets = new Set();
    this.invalidated = false;

    return {
      buckets: staticBuckets,
      updatedBuckets
    };
  }

  private async getCheckpointUpdateDynamic(checkpoint: util.OpId): Promise<CheckpointUpdate> {
    const querier = this.querier;
    const storage = this.bucketStorage;
    const staticBuckets = querier.staticBuckets;
    const dynamicBuckets = await querier.queryDynamicBucketDescriptions({
      getParameterSets(lookups) {
        return storage.getParameterSets(checkpoint, lookups);
      }
    });
    const allBuckets = [...staticBuckets, ...dynamicBuckets];

    return {
      buckets: allBuckets,
      // We cannot track individual bucket updates for dynamic lookups yet
      updatedBuckets: null
    };
  }
}

export interface CheckpointLine {
  checkpointLine: util.StreamingSyncCheckpointDiff | util.StreamingSyncCheckpoint;
  bucketsToFetch: BucketDescription[];
}

export interface BucketChecksumStateOptions {
  bucketStorage: BucketChecksumStateStorage;
  syncRules: SqlSyncRules;
  syncParams: RequestParameters;
  initialBucketState: Map<string, BucketSyncState>;
}

// Use a more specific type to simplify testing
export type BucketChecksumStateStorage = Pick<storage.SyncRulesBucketStorage, 'getChecksums' | 'getParameterSets'>;

function limitedBuckets(buckets: string[] | { bucket: string }[], limit: number) {
  buckets = buckets.map((b) => {
    if (typeof b != 'string') {
      return b.bucket;
    } else {
      return b;
    }
  });
  if (buckets.length <= limit) {
    return JSON.stringify(buckets);
  }
  const limited = buckets.slice(0, limit);
  return `${JSON.stringify(limited)}...`;
}
