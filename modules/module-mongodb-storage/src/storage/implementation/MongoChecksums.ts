import { mongo } from '@powersync/lib-service-mongodb';
import {
  addPartialChecksums,
  bson,
  BucketChecksum,
  BucketChecksumOptions,
  BucketChecksumRequest,
  ChecksumCache,
  ChecksumMap,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksum,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import type { VersionedPowerSyncMongo } from './db.js';

import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { StorageConfig } from './models.js';

export interface FetchPartialBucketChecksumByDefinition {
  bucket: string;
  definitionId: BucketDefinitionId;
  start?: InternalOpId;
  end: InternalOpId;
}

export interface FetchPartialBucketChecksumByBucket {
  bucket: string;
  start?: InternalOpId;
  end: InternalOpId;
}

/**
 * Checksum calculation options, primarily for tests.
 */
export interface MongoChecksumOptions {
  /**
   * How many buckets to process in a batch when calculating checksums.
   */
  bucketBatchLimit?: number;

  /**
   * Limit on the number of documents to calculate a checksum on at a time.
   */
  operationBatchLimit?: number;

  storageConfig: StorageConfig;
}

interface MongoChecksumReadContext {
  readAfterTime?: mongo.Timestamp;
  readPreference?: mongo.ReadPreference;
  readConcern?: mongo.ReadConcernLike;
}

export interface MongoChecksumSessionContext {
  session?: mongo.ClientSession;
  readPreference?: mongo.ReadPreference;
  readConcern?: mongo.ReadConcernLike;
}

const DEFAULT_BUCKET_BATCH_LIMIT = 200;
export const DEFAULT_OPERATION_BATCH_LIMIT = 50_000;

export abstract class MongoChecksums {
  private _cache: ChecksumCache<MongoChecksumReadContext | undefined> | undefined;
  protected readonly storageConfig: StorageConfig;

  constructor(
    protected readonly db: VersionedPowerSyncMongo,
    protected readonly group_id: number,
    protected readonly options: MongoChecksumOptions
  ) {
    this.storageConfig = options.storageConfig;
  }

  /**
   * Lazy-instantiated cache.
   *
   * This means the cache only allocates memory once it is used for the first time.
   */
  private get cache(): ChecksumCache<MongoChecksumReadContext | undefined> {
    this._cache ??= new ChecksumCache<MongoChecksumReadContext | undefined>({
      fetchChecksums: (batch, context) => {
        return this.computePartialChecksums(batch, context);
      }
    });
    return this._cache;
  }

  /**
   * Calculate checksums, utilizing the cache for partial checkums, and querying the remainder from
   * the database (bucket_state + bucket_data).
   */
  async getChecksums(
    checkpoint: InternalOpId,
    buckets: BucketChecksumRequest[],
    options?: {
      readAfterTime?: mongo.Timestamp;
      readConcern?: mongo.ReadConcernLike;
      readPreference?: mongo.ReadPreference;
      requestHint?: BucketChecksumOptions['requestHint'];
    }
  ): Promise<ChecksumMap> {
    if (options?.readPreference == null && options?.readConcern == null) {
      return this.cache.getChecksumMap(checkpoint, buckets);
    }

    return this.cache.getChecksumMap(checkpoint, buckets, {
      readAfterTime: options.readAfterTime,
      readConcern: options.readConcern,
      readPreference: options.readPreference
    });
  }

  clearCache() {
    this.cache.clear();
  }

  /**
   * Calculate (partial) checksums from bucket_state (pre-aggregated) and bucket_data (individual operations).
   *
   * Results are not cached here. This method is only called by {@link ChecksumCache.getChecksumMap},
   * which is responsible for caching its result.
   *
   * As long as data is compacted regularly, this should be fast. Large buckets without pre-compacted bucket_state
   * can be slow.
   */
  private async computePartialChecksums(
    batch: FetchPartialBucketChecksum[],
    context: MongoChecksumReadContext | undefined
  ): Promise<PartialChecksumMap> {
    if (batch.length == 0) {
      return new Map();
    }

    if (context?.readAfterTime != null) {
      const { readAfterTime, readConcern, readPreference } = context;
      return this.db.client.withSession({ causalConsistency: true }, async (session) => {
        session.advanceOperationTime(readAfterTime);
        return this.computePartialChecksumsWithSession(batch, { session, readConcern, readPreference });
      });
    }

    return this.computePartialChecksumsWithSession(batch, context);
  }

  private async computePartialChecksumsWithSession(
    batch: FetchPartialBucketChecksum[],
    context?: MongoChecksumSessionContext
  ): Promise<PartialChecksumMap> {
    const preStates = await this.fetchPreStates(batch, context);

    const mappedRequests = batch.map((request) => {
      let start = request.start;
      if (start == null) {
        const preState = preStates.get(request.bucket);
        if (preState != null) {
          start = preState.opId;
        }
      }
      return {
        ...request,
        start
      };
    });

    const queriedChecksums = await this.computePartialChecksumsDirect(mappedRequests, context);

    return new Map<string, PartialOrFullChecksum>(
      batch.map((request) => {
        const bucket = request.bucket;
        // Could be null if this is either (1) a partial request, or (2) no compacted checksum was available
        const preState = preStates.get(bucket);
        // Could be null if we got no data
        const partialChecksum = queriedChecksums.get(bucket);
        const merged = addPartialChecksums(bucket, preState?.checksum ?? null, partialChecksum ?? null);

        return [bucket, merged];
      })
    );
  }

  /**
   * Calculate (partial) checksums from the data collection directly, bypassing the cache and bucket_state.
   *
   * Can be used directly in cases where the cache should be bypassed, such as from a compact job.
   *
   * Internally, we do calculations in smaller batches of buckets as appropriate.
   *
   * For large buckets, this can be slow, but should not time out as the underlying queries are performed in
   * smaller batches.
   */
  public async computePartialChecksumsDirect(
    batch: FetchPartialBucketChecksum[],
    context?: MongoChecksumSessionContext
  ): Promise<PartialChecksumMap> {
    // Limit the number of buckets we query for at a time.
    const bucketBatchLimit = this.options?.bucketBatchLimit ?? DEFAULT_BUCKET_BATCH_LIMIT;

    if (batch.length <= bucketBatchLimit) {
      // Single batch - no need for splitting the batch and merging results
      return await this.computePartialChecksumsInternal(batch, context);
    }
    // Split the batch and merge results
    let results = new Map<string, PartialOrFullChecksum>();
    for (let i = 0; i < batch.length; i += bucketBatchLimit) {
      const bucketBatch = batch.slice(i, i + bucketBatchLimit);
      const batchResults = await this.computePartialChecksumsInternal(bucketBatch, context);
      for (let r of batchResults.values()) {
        results.set(r.bucket, r);
      }
    }
    return results;
  }

  /**
   * Query a batch of checksums.
   *
   * We limit the number of operations that the query aggregates in each sub-batch, to avoid potential query timeouts.
   *
   * `batch` must be limited to DEFAULT_BUCKET_BATCH_LIMIT buckets before calling this.
   */
  protected abstract computePartialChecksumsInternal(
    batch: FetchPartialBucketChecksum[],
    context?: MongoChecksumSessionContext
  ): Promise<PartialChecksumMap>;

  protected abstract fetchPreStates(
    batch: FetchPartialBucketChecksum[],
    context?: MongoChecksumSessionContext
  ): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>>;

  protected checksumReadOptions(context?: MongoChecksumSessionContext): {
    session?: mongo.ClientSession;
    readPreference?: mongo.ReadPreference;
    readConcern?: mongo.ReadConcernLike;
  } {
    if (context?.readPreference == null && context?.readConcern == null) {
      return {};
    }

    return {
      session: context.session,
      readPreference: context.readPreference,
      readConcern: context.readConcern
    };
  }
}

export function emptyChecksumForRequest(
  request: Pick<FetchPartialBucketChecksum | FetchPartialBucketChecksumByDefinition, 'bucket' | 'start'>
): PartialOrFullChecksum {
  if (request.start == null) {
    return { bucket: request.bucket, count: 0, checksum: 0 };
  }
  return { bucket: request.bucket, partialCount: 0, partialChecksum: 0 };
}

/**
 * Convert output of the $group stage into a checksum.
 */
export function checksumFromAggregate(doc: bson.Document): PartialOrFullChecksum {
  const partialChecksum = Number(BigInt(doc.checksum_total) & 0xffffffffn) & 0xffffffff;
  const bucket = doc._id;

  if (doc.has_clear_op == 1) {
    return {
      // full checksum - replaces any previous one
      bucket,
      checksum: partialChecksum,
      count: doc.count
    } satisfies BucketChecksum;
  } else {
    return {
      // partial checksum - is added to a previous one
      bucket,
      partialCount: doc.count,
      partialChecksum
    } satisfies PartialChecksum;
  }
}
