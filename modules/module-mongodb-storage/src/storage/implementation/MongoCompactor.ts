import * as timers from 'node:timers/promises';

import { isMongoServerError, mongo, MONGO_OPERATION_TIMEOUT_MS } from '@powersync/lib-service-mongodb';
import {
  logger as defaultLogger,
  Logger,
  ReplicationAssertionError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
import { InternalOpId, isPartialChecksum, storage } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';

import { BucketKey } from './common/BucketDataDoc.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { BucketStateDocumentBase } from './models.js';
import type { MongoSyncBucketStorage } from './MongoSyncBucketStorage.js';
import { isRetryableObjectStorageError } from './v3/object-storage/ObjectStorage.js';

export interface CurrentBucketState {
  /** Bucket name */
  bucket: string;
  definitionId: BucketDefinitionId;
  /**
   * Rows seen in the bucket, with the last op_id of each.
   */
  seen: Map<string, InternalOpId>;
  /**
   * Estimated memory usage of the seen Map.
   */
  trackingSize: number;
  /**
   * Last (lowest) seen op_id that is not a PUT.
   */
  lastNotPut: InternalOpId | null;
  /**
   * Number of REMOVE/MOVE operations seen since lastNotPut.
   */
  opsSincePut: number;
  /**
   * Incrementally-updated checksum, up to maxOpId.
   */
  checksum: number;
  /**
   * Op count for the checksum.
   */
  opCount: number;
  /**
   * Byte size of ops covered by the checksum.
   */
  opBytes: number;
}

export interface MongoCompactOptions extends storage.CompactOptions {
  /**
   * Only merge adjacent V3 bucket-data chunks. This is used after initial
   * replication, where reading every operation would defeat the purpose of
   * the lightweight pass.
   */
  compactChunksOnly?: boolean;
}

const DEFAULT_CLEAR_BATCH_LIMIT = 5000;
const DEFAULT_MOVE_BATCH_LIMIT = 2000;
const DEFAULT_MOVE_BATCH_QUERY_LIMIT = 10_000;
const DEFAULT_MOVE_BATCH_BYTE_LIMIT = 16 * 1024 * 1024;
const DEFAULT_MIN_BUCKET_CHANGES = 10;
const DEFAULT_MIN_CHANGE_RATIO = 0.1;
const DIRTY_BUCKET_SCAN_BATCH_SIZE = 2_000;
/** This default is primarily for tests. */
const DEFAULT_MEMORY_LIMIT_MB = 64;
const COMPACTION_RETRY_LIMIT = 3;
const COMPACTION_RETRY_DELAY_MS = 1_000;

export class ConcurrentCompactionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConcurrentCompactionError';
  }
}

export interface DirtyBucket {
  bucket: string;
  definitionId: BucketDefinitionId | null;
  estimatedCount: number;
  dirtyRatio?: number;
}

export abstract class MongoCompactor {
  protected bucketStateUpdates: mongo.AnyBulkWriteOperation<BucketStateDocumentBase>[] = [];

  protected readonly idLimitBytes: number;
  protected readonly moveBatchLimit: number;
  protected readonly moveBatchQueryLimit: number;
  protected readonly moveBatchByteLimit: number;
  protected readonly clearBatchLimit: number;
  protected readonly minBucketChanges: number;
  protected readonly minChangeRatio: number;
  protected readonly maxOpId: bigint;
  protected readonly buckets: string[] | undefined;
  protected readonly deleteCheckpointRequestsBefore: Date | undefined;
  protected readonly signal?: AbortSignal;
  protected readonly group_id: number;
  protected readonly compactChunksOnly: boolean;
  protected compactedBucketCount = 0;

  protected readonly logger: Logger;

  constructor(
    protected readonly storage: MongoSyncBucketStorage,
    protected readonly db: VersionedPowerSyncMongo,
    options: MongoCompactOptions
  ) {
    this.group_id = storage.replicationStreamId;
    this.idLimitBytes = (options.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
    this.moveBatchLimit = options.moveBatchLimit ?? DEFAULT_MOVE_BATCH_LIMIT;
    this.moveBatchQueryLimit = options.moveBatchQueryLimit ?? DEFAULT_MOVE_BATCH_QUERY_LIMIT;
    this.moveBatchByteLimit = options.moveBatchByteLimit ?? DEFAULT_MOVE_BATCH_BYTE_LIMIT;
    this.clearBatchLimit = options.clearBatchLimit ?? DEFAULT_CLEAR_BATCH_LIMIT;
    if (this.clearBatchLimit < 2) {
      throw new ReplicationAssertionError('clearBatchLimit must be >= 2');
    }
    this.minBucketChanges = options.minBucketChanges ?? DEFAULT_MIN_BUCKET_CHANGES;
    this.minChangeRatio = options.minChangeRatio ?? DEFAULT_MIN_CHANGE_RATIO;
    this.maxOpId = options.maxOpId ?? 0n;
    this.buckets = options.compactBuckets;
    this.deleteCheckpointRequestsBefore = options.deleteCheckpointRequestsBefore;
    this.signal = options.signal;
    this.compactChunksOnly = options.compactChunksOnly ?? false;
    this.logger = options.logger ?? defaultLogger;
  }

  /**
   * Compact buckets by converting operations into MOVE and/or CLEAR operations.
   *
   * See /docs/storage/compacting-operations.md for details.
   */
  async compact(): Promise<number> {
    await this.deleteOldCheckpointRequests();

    if (this.buckets) {
      for (const bucket of this.buckets) {
        // We can make this more efficient later on by iterating through the buckets in a single query.
        // That makes batching more tricky, so we leave for later.
        await this.compactSingleBucketRetried(bucket);
      }
    } else {
      await this.compactDirtyBuckets();
    }

    return this.compactedBucketCount;
  }

  private async deleteOldCheckpointRequests() {
    if (this.deleteCheckpointRequestsBefore == null) {
      return;
    }

    this.signal?.throwIfAborted();
    // The explicit $exists guarantees the query is a subset of the
    // checkpoint_requested_at partial index's filter, so the planner can use it.
    // Keep pending requests during replication lag so a retry doesn't advance the associated
    // LSN even further.
    await this.db.write_checkpoints.deleteMany({
      checkpoint_requested_at: { $exists: true, $lt: this.deleteCheckpointRequestsBefore },
      processed_at_lsn: { $ne: null }
    });
    await this.db.custom_write_checkpoints.deleteMany({
      checkpoint_requested_at: { $exists: true, $lt: this.deleteCheckpointRequestsBefore }
    });
  }

  protected async *dirtyBucketBatchesForCollection<TCollectionBucketState extends BucketStateDocumentBase>(
    collection: mongo.Collection<TCollectionBucketState>,
    lastId: TCollectionBucketState['_id'],
    maxId: TCollectionBucketState['_id'],
    options: {
      minBucketChanges: number;
      minChangeRatio: number;
    },
    getDefinitionId: (state: TCollectionBucketState) => BucketDefinitionId | null
  ): AsyncGenerator<DirtyBucket[]> {
    // Paginate through the bucket state collection using cursor-based scanning.
    while (true) {
      // To avoid timeouts from too many buckets not meeting the minBucketChanges criteria, use an aggregation pipeline
      // to scan a fixed batch of buckets at a time, but only return buckets that meet the criteria.
      const [result] = await collection
        .aggregate<{
          buckets: TCollectionBucketState[];
          cursor: Pick<TCollectionBucketState, '_id'>[];
        }>(
          [
            {
              $match: {
                _id: { $gt: lastId, $lt: maxId }
              }
            },
            {
              $sort: { _id: 1 }
            },
            {
              // Scan a fixed number of docs each query so sparse matches don't block progress.
              $limit: DIRTY_BUCKET_SCAN_BATCH_SIZE
            },
            {
              $facet: {
                buckets: [
                  {
                    $match: {
                      'estimate_since_compact.count': { $gte: options.minBucketChanges }
                    }
                  },
                  {
                    $project: {
                      _id: 1,
                      estimate_since_compact: 1,
                      compacted_state: 1
                    }
                  }
                ],
                // This is used for the next query.
                cursor: [{ $sort: { _id: -1 } }, { $limit: 1 }, { $project: { _id: 1 } }]
              }
            }
          ],
          { maxTimeMS: MONGO_OPERATION_TIMEOUT_MS }
        )
        .toArray();

      const cursor = result?.cursor?.[0];
      if (cursor == null) {
        break;
      }
      lastId = cursor._id;

      const mapped = (result?.buckets ?? []).map((bucketState) => {
        // The numbers, specifically the bytes, could be a bigint. Convert to Number to allow calculating ratios.
        // BigInt precision is not needed here since this is only an estimate.
        const updatedCount = bucketState.estimate_since_compact?.count ?? 0;
        const totalCount = (bucketState.compacted_state?.count ?? 0) + updatedCount;
        const updatedBytes = Number(bucketState.estimate_since_compact?.bytes ?? 0);
        const totalBytes = Number(bucketState.compacted_state?.bytes ?? 0) + updatedBytes;
        const dirtyChangeNumber = totalCount > 0 ? updatedCount / totalCount : 0;
        const dirtyChangeBytes = totalBytes > 0 ? updatedBytes / totalBytes : 0;
        return {
          bucket: bucketState._id.b,
          definitionId: getDefinitionId(bucketState),
          estimatedCount: totalCount,
          dirtyRatio: Math.max(dirtyChangeNumber, dirtyChangeBytes)
        };
      });

      yield mapped.filter(
        (bucket) => bucket.estimatedCount >= options.minBucketChanges && bucket.dirtyRatio >= options.minChangeRatio
      );
    }
  }

  protected async dirtyBucketBatchForChecksumsForCollection<TBucketState extends BucketStateDocumentBase>(
    collection: mongo.Collection<TBucketState>,
    filter: mongo.Filter<TBucketState>,
    getDefinitionId: (state: mongo.WithId<TBucketState>) => BucketDefinitionId | null
  ): Promise<DirtyBucket[]> {
    const dirtyBuckets = await collection
      .find(filter, {
        projection: {
          _id: 1,
          estimate_since_compact: 1,
          compacted_state: 1
        },
        sort: {
          'estimate_since_compact.count': -1
        },
        limit: 200,
        maxTimeMS: MONGO_OPERATION_TIMEOUT_MS
      })
      .toArray();

    return dirtyBuckets.map((bucket) => ({
      bucket: bucket._id.b,
      definitionId: getDefinitionId(bucket),
      estimatedCount: Number(bucket.estimate_since_compact!.count) + Number(bucket.compacted_state?.count ?? 0)
    }));
  }

  public abstract dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]>;

  public abstract dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]>;

  protected async compactDirtyBuckets() {
    for await (const buckets of this.dirtyBucketBatches({
      minBucketChanges: this.minBucketChanges,
      minChangeRatio: this.minChangeRatio
    })) {
      this.signal?.throwIfAborted();
      if (buckets.length == 0) {
        continue;
      }

      for (const { bucket, definitionId } of buckets) {
        await this.compactSingleBucketRetried(bucket, definitionId);
      }
    }
  }

  /**
   * Compaction for a single bucket, with retries on failure.
   *
   * A compaction can race another compactor after its initial scan. Restarting
   * the bucket is safe because each replacement is transactional. Object
   * storage paths are also prepared with lifecycle markers, so a retry can
   * safely overwrite or eventually clean up uploads from the failed attempt.
   */
  protected async compactSingleBucketRetried(bucket: string, definitionId: BucketDefinitionId | null = null) {
    let retryCount = 0;
    while (true) {
      this.signal?.throwIfAborted();
      // Do not carry queued bucket state writes from a failed attempt into the rescan.
      this.bucketStateUpdates = [];
      try {
        await this.compactSingleBucket(bucket, definitionId);
        return;
      } catch (e) {
        if (this.signal?.aborted) {
          throw e;
        }

        const retryReason = compactionRetryReason(e);
        if (retryReason == null) {
          throw e;
        }
        if (retryCount >= COMPACTION_RETRY_LIMIT) {
          throw new Error(
            `Failed to compact bucket ${bucket} after ${retryCount + 1} attempts (${retryReason}): ${errorMessage(e)}`,
            { cause: e }
          );
        }

        retryCount++;
        const delay = COMPACTION_RETRY_DELAY_MS * 2 ** (retryCount - 1);
        this.logger.warn(
          `Error compacting bucket ${bucket} (${retryReason}); retrying in ${delay}ms ` +
            `(attempt ${retryCount + 1}/${COMPACTION_RETRY_LIMIT + 1})`,
          e
        );
        await timers.setTimeout(delay, undefined, this.signal ? { signal: this.signal } : undefined);
      }
    }
  }

  protected abstract compactSingleBucket(bucket: string, definitionId?: BucketDefinitionId | null): Promise<void>;

  protected collectBucketStateUpdates(
    state: CurrentBucketState,
    compactedOpId: InternalOpId
  ): mongo.AnyBulkWriteOperation<BucketStateDocumentBase> {
    if (state.opCount < 0) {
      throw new ServiceAssertionError(
        `Invalid opCount: ${state.opCount} checksum ${state.checksum} opsSincePut: ${state.opsSincePut} maxOpId: ${this.maxOpId}`
      );
    }
    return {
      updateOne: {
        filter: this.bucketStateFilter(state.bucket, state.definitionId),
        update: {
          $set: {
            compacted_state: {
              op_id: compactedOpId,
              count: state.opCount,
              checksum: BigInt(state.checksum),
              bytes: state.opBytes
            },
            estimate_since_compact: {
              // There could have been a whole bunch of new operations added to the bucket while compacting,
              // which we don't currently cater for. We could potentially query for that, but that adds overhead.
              count: 0,
              bytes: 0
            }
          }
        } satisfies mongo.UpdateFilter<BucketStateDocumentBase>,
        // We generally expect this to have been created before.
        // We don't create new ones here, to avoid issues with the unique index on bucket_updates.
        upsert: false
      }
    };
  }

  protected updateBucketChecksums(state: CurrentBucketState, compactedOpId: InternalOpId) {
    this.bucketStateUpdates.push(this.collectBucketStateUpdates(state, compactedOpId));
  }

  protected async flushBucketStateUpdates() {
    if (this.bucketStateUpdates.length > 0) {
      this.logger.info(`Updating ${this.bucketStateUpdates.length} bucket states`);
      await this.writeBucketStateUpdates();
      this.bucketStateUpdates = [];
    }
  }

  protected async updateChecksumsBatch(buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]) {
    const checksums = await this.computeChecksumsForBuckets(buckets);
    const definitionIdByBucket = new Map(buckets.map((bucket) => [bucket.bucket, bucket.definitionId]));

    for (const bucketChecksum of checksums.values()) {
      if (isPartialChecksum(bucketChecksum)) {
        // Should never happen since we don't specify `start`.
        throw new ServiceAssertionError(`Full checksum expected, got ${JSON.stringify(bucketChecksum)}`);
      }

      this.bucketStateUpdates.push({
        updateOne: {
          filter: this.bucketStateFilter(
            bucketChecksum.bucket,
            definitionIdByBucket.get(bucketChecksum.bucket) ?? null
          ),
          update: {
            $set: {
              compacted_state: {
                op_id: this.maxOpId,
                count: bucketChecksum.count,
                checksum: BigInt(bucketChecksum.checksum),
                bytes: null
              },
              estimate_since_compact: {
                count: 0,
                bytes: 0
              }
            }
          } satisfies mongo.UpdateFilter<BucketStateDocumentBase>,
          // We don't create new ones here - it gets tricky to get the last_op right with the unique index on
          // bucket_updates.
          upsert: false
        }
      });
    }

    await this.flushBucketStateUpdates();
  }

  protected abstract writeBucketStateUpdates(): Promise<void>;
  protected abstract computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap>;
  protected abstract bucketStateFilter(bucket: string, definitionId: BucketDefinitionId | null): mongo.Document;
}

export interface BucketDataCollectionContext<TBucketData extends mongo.Document> {
  bucketKey: BucketKey;
  collection: mongo.Collection<TBucketData>;
}

function compactionRetryReason(error: unknown): string | null {
  if (error instanceof ConcurrentCompactionError) {
    return 'concurrent compaction';
  }
  if (isRetryableObjectStorageError(error)) {
    return 'transient object storage failure';
  }
  if (isMongoServerError(error)) {
    return 'MongoDB failure';
  }
  return null;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
