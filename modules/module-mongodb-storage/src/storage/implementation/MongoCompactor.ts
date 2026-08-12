import * as timers from 'node:timers/promises';

import { isMongoServerError, mongo } from '@powersync/lib-service-mongodb';
import { logger as defaultLogger, Logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

import { BucketKey } from './common/BucketDataDoc.js';
import type { VersionedPowerSyncMongo } from './db.js';
import type { MongoSyncBucketStorage } from './MongoSyncBucketStorage.js';
import { isRetryableObjectStorageError } from './v3/object-storage/ObjectStorage.js';

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

/**
 * A worker whose bucket lease has been replaced must stop immediately. Unlike
 * a transactional replacement conflict, retrying it with the same compactor
 * instance would still use the stale lease and could race the new owner.
 */
export class CompactionLeaseLostError extends ConcurrentCompactionError {
  constructor(message: string) {
    super(message);
    this.name = 'CompactionLeaseLostError';
  }
}

export abstract class MongoCompactor {
  protected readonly idLimitBytes: number;
  protected readonly moveBatchLimit: number;
  protected readonly moveBatchQueryLimit: number;
  protected readonly moveBatchByteLimit: number;
  protected readonly clearBatchLimit: number;
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
    this.maxOpId = options.maxOpId ?? 0n;
    this.buckets = options.compactBuckets;
    this.deleteCheckpointRequestsBefore = options.deleteCheckpointRequestsBefore;
    this.signal = options.signal;
    this.compactChunksOnly = options.compactChunksOnly ?? false;
    this.logger = options.logger ?? defaultLogger;
  }

  abstract compact(): Promise<number>;

  protected async deleteOldCheckpointRequests() {
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

  /**
   * Compaction for a single bucket, with retries on failure.
   *
   * A compaction can race another compactor after its initial scan. Restarting
   * the bucket is safe because each replacement is transactional. Object
   * storage paths are also prepared with lifecycle markers, so a retry can
   * safely overwrite or eventually clean up uploads from the failed attempt.
   */
  protected async retryCompaction(bucket: string, compact: () => Promise<void>) {
    let retryCount = 0;
    while (true) {
      this.signal?.throwIfAborted();
      try {
        await compact();
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
}

export interface BucketDataCollectionContext<TBucketData extends mongo.Document> {
  bucketKey: BucketKey;
  collection: mongo.Collection<TBucketData>;
}

function compactionRetryReason(error: unknown): string | null {
  if (error instanceof CompactionLeaseLostError) {
    return null;
  }
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
