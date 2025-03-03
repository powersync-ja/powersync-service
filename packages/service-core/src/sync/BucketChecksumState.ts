import { BucketDescription, RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';

import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { BucketParameterQuerier } from '@powersync/service-sync-rules/src/BucketParameterQuerier.js';
import { BucketSyncState } from './sync.js';

export interface BucketChecksumStateOptions {
  bucketStorage: BucketChecksumStateStorage;
  syncRules: SqlSyncRules;
  syncParams: RequestParameters;
  initialBucketPositions?: { name: string; after: util.InternalOpId }[];
}

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
  public bucketDataPositions = new Map<string, BucketSyncState>();

  /**
   * Last checksums sent to the client. We keep this to calculate checkpoint diffs.
   */
  private lastChecksums: util.ChecksumMap | null = null;
  private lastWriteCheckpoint: bigint | null = null;

  private readonly parameterState: BucketParameterState;

  /**
   * Keep track of buckets that need to be downloaded. This is specifically relevant when
   * partial checkpoints are sent.
   */
  private pendingBucketDownloads = new Set<string>();

  constructor(options: BucketChecksumStateOptions) {
    this.bucketStorage = options.bucketStorage;
    this.parameterState = new BucketParameterState(options.bucketStorage, options.syncRules, options.syncParams);
    this.bucketDataPositions = new Map();

    for (let { name, after: start } of options.initialBucketPositions ?? []) {
      this.bucketDataPositions.set(name, { start_op_id: start });
    }
  }

  async buildNextCheckpointLine(next: storage.StorageCheckpointUpdate): Promise<CheckpointLine | null> {
    const { writeCheckpoint, base } = next;
    const user_id = this.parameterState.syncParams.user_id;

    const storage = this.bucketStorage;

    const update = await this.parameterState.getCheckpointUpdate(next);
    if (update == null) {
      return null;
    }

    const { buckets: allBuckets, updatedBuckets } = update;

    let dataBucketsNew = new Map<string, BucketSyncState>();
    for (let bucket of allBuckets) {
      dataBucketsNew.set(bucket.bucket, {
        description: bucket,
        start_op_id: this.bucketDataPositions.get(bucket.bucket)?.start_op_id ?? 0n
      });
    }
    this.bucketDataPositions = dataBucketsNew;

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

      let generateBucketsToFetch = new Set<string>();
      for (let bucket of diff.updatedBuckets) {
        generateBucketsToFetch.add(bucket.bucket);
      }
      for (let bucket of this.pendingBucketDownloads) {
        // Bucket from a previous checkpoint that hasn't been downloaded yet.
        // If we still have this bucket, include it in the list of buckets to fetch.
        if (checksumMap.has(bucket)) {
          generateBucketsToFetch.add(bucket);
        }
      }

      const updatedBucketDescriptions = diff.updatedBuckets.map((e) => ({
        ...e,
        priority: this.bucketDataPositions.get(e.bucket)!.description!.priority
      }));
      bucketsToFetch = [...generateBucketsToFetch].map((b) => {
        return {
          bucket: b,
          priority: this.bucketDataPositions.get(b)!.description!.priority
        };
      });

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
          last_op_id: util.internalToExternalOpId(base.checkpoint),
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
          last_op_id: util.internalToExternalOpId(base.checkpoint),
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          buckets: [...checksumMap.values()].map((e) => ({
            ...e,
            priority: this.bucketDataPositions.get(e.bucket)!.description!.priority
          }))
        }
      } satisfies util.StreamingSyncCheckpoint;
    }

    this.lastChecksums = checksumMap;
    this.lastWriteCheckpoint = writeCheckpoint;
    this.pendingBucketDownloads = new Set(bucketsToFetch.map((b) => b.bucket));

    return {
      checkpointLine,
      bucketsToFetch
    };
  }

  /**
   * Get bucket positions to sync, given the list of buckets.
   *
   * @param bucketsToFetch List of buckets to fetch, typically from buildNextCheckpointLine, or a subset of that
   * @returns
   */
  getFilteredBucketPositions(bucketsToFetch: BucketDescription[]): Map<string, util.InternalOpId> {
    const filtered = new Map<string, util.InternalOpId>();
    for (let bucket of bucketsToFetch) {
      const state = this.bucketDataPositions.get(bucket.bucket);
      if (state) {
        filtered.set(bucket.bucket, state.start_op_id);
      }
    }
    return filtered;
  }

  /**
   * Update the position of bucket data the client has.
   *
   * @param bucket the bucket name
   * @param nextAfter sync operations >= this value in the next batch
   */
  updateBucketPosition(options: { bucket: string; nextAfter: util.InternalOpId; hasMore: boolean }) {
    const state = this.bucketDataPositions.get(options.bucket);
    if (state) {
      state.start_op_id = options.nextAfter;
    }
    if (!options.hasMore) {
      this.pendingBucketDownloads.delete(options.bucket);
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

  constructor(bucketStorage: BucketChecksumStateStorage, syncRules: SqlSyncRules, syncParams: RequestParameters) {
    this.bucketStorage = bucketStorage;
    this.syncRules = syncRules;
    this.syncParams = syncParams;

    this.querier = syncRules.getBucketParameterQuerier(this.syncParams);
    this.staticBuckets = new Map<string, BucketDescription>(this.querier.staticBuckets.map((b) => [b.bucket, b]));
  }

  async getCheckpointUpdate(checkpoint: storage.StorageCheckpointUpdate): Promise<CheckpointUpdate | null> {
    const querier = this.querier;
    let update: CheckpointUpdate | null;
    if (querier.hasDynamicBuckets) {
      update = await this.getCheckpointUpdateDynamic(checkpoint);
    } else {
      update = await this.getCheckpointUpdateStatic(checkpoint);
    }

    if (update == null) {
      return null;
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

  /**
   * For static buckets, we can keep track of which buckets have been updated.
   */
  private async getCheckpointUpdateStatic(
    checkpoint: storage.StorageCheckpointUpdate
  ): Promise<CheckpointUpdate | null> {
    const querier = this.querier;
    const update = checkpoint.update;

    if (update.invalidateDataBuckets) {
      return {
        buckets: querier.staticBuckets,
        updatedBuckets: null
      };
    }

    let updatedBuckets = new Set<string>();

    for (let bucket of update.updatedDataBuckets ?? []) {
      if (this.staticBuckets.has(bucket)) {
        updatedBuckets.add(bucket);
      }
    }

    if (updatedBuckets.size == 0) {
      // No change - skip this checkpoint
      return null;
    }

    return {
      buckets: querier.staticBuckets,
      updatedBuckets
    };
  }

  /**
   * For dynamic buckets, we need to re-query the list of buckets every time.
   */
  private async getCheckpointUpdateDynamic(
    checkpoint: storage.StorageCheckpointUpdate
  ): Promise<CheckpointUpdate | null> {
    const querier = this.querier;
    const storage = this.bucketStorage;
    const staticBuckets = querier.staticBuckets;
    const update = checkpoint.update;

    let hasChange = false;
    if (update.invalidateDataBuckets || update.updatedDataBuckets?.length > 0) {
      hasChange = true;
    } else if (update.invalidateParameterBuckets) {
      hasChange = true;
    } else {
      for (let bucket of update.updatedParameterBucketDefinitions ?? []) {
        if (querier.dynamicBucketDefinitions.has(bucket)) {
          hasChange = true;
          break;
        }
      }
    }

    if (!hasChange) {
      return null;
    }

    const dynamicBuckets = await querier.queryDynamicBucketDescriptions({
      getParameterSets(lookups) {
        return storage.getParameterSets(checkpoint.base.checkpoint, lookups);
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
