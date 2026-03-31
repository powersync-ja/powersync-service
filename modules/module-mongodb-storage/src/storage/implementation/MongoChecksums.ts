import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  addPartialChecksums,
  bson,
  BucketChecksumRequest,
  BucketChecksum,
  ChecksumCache,
  ChecksumMap,
  FetchPartialBucketChecksum,
  InternalOpId,
  isPartialChecksum,
  PartialChecksum,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import { VersionedPowerSyncMongo } from './db.js';
import { BucketDefinitionId, BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { StorageConfig } from './models.js';

export interface FetchPartialBucketChecksumV3 {
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
  mapping?: BucketDefinitionMapping;
}

const DEFAULT_BUCKET_BATCH_LIMIT = 200;
const DEFAULT_OPERATION_BATCH_LIMIT = 50_000;

/**
 * Shared checksum query plumbing.
 *
 * General implementation flow is:
 * 1. getChecksums() -> check cache for (partial) matches. If not found or partial match, query the remainder using computePartialChecksums().
 * 2. computePartialChecksums() -> query bucket_state for partial matches. Query the remainder using computePartialChecksumsDirect().
 * 3. computePartialChecksumsDirect() -> split into batches of 200 buckets at a time -> computePartialChecksumsInternal()
 * 4. computePartialChecksumsInternal() -> aggregate over 50_000 operations in bucket_data at a time
 */
abstract class AbstractMongoChecksums {
  private _cache: ChecksumCache | undefined;
  private readonly storageConfig: StorageConfig;

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
  private get cache(): ChecksumCache {
    this._cache ??= new ChecksumCache({
      fetchChecksums: (batch) => {
        return this.computePartialChecksums(batch);
      }
    });
    return this._cache;
  }

  /**
   * Calculate checksums, utilizing the cache for partial checkums, and querying the remainder from
   * the database (bucket_state + bucket_data).
   */
  async getChecksums(checkpoint: InternalOpId, buckets: BucketChecksumRequest[]): Promise<ChecksumMap> {
    return this.cache.getChecksumMap(checkpoint, buckets);
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
  private async computePartialChecksums(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    if (batch.length == 0) {
      return new Map();
    }
    const preStates = await this.fetchPreStates(batch);

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

    const queriedChecksums = await this.computePartialChecksumsDirect(mappedRequests);

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
  public async computePartialChecksumsDirect(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    // Limit the number of buckets we query for at a time.
    const bucketBatchLimit = this.options?.bucketBatchLimit ?? DEFAULT_BUCKET_BATCH_LIMIT;

    if (batch.length <= bucketBatchLimit) {
      // Single batch - no need for splitting the batch and merging results
      return await this.computePartialChecksumsInternal(batch);
    }
    // Split the batch and merge results
    let results = new Map<string, PartialOrFullChecksum>();
    for (let i = 0; i < batch.length; i += bucketBatchLimit) {
      const bucketBatch = batch.slice(i, i + bucketBatchLimit);
      const batchResults = await this.computePartialChecksumsInternal(bucketBatch);
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
  protected abstract computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap>;

  protected abstract fetchPreStates(
    batch: FetchPartialBucketChecksum[]
  ): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>>;

  protected async computePartialChecksumsForCollection<TRequest extends FetchPartialBucketChecksumByBucket>(
    batch: TRequest[],
    collection: mongo.Collection<mongo.Document>,
    createFilter: (request: TRequest) => any
  ): Promise<PartialChecksumMap> {
    const batchLimit = this.options?.operationBatchLimit ?? DEFAULT_OPERATION_BATCH_LIMIT;

    // Map requests by bucket. We adjust this as we get partial results.
    let requests = new Map<string, TRequest>();
    for (let request of batch) {
      requests.set(request.bucket, request);
    }

    const partialChecksums = new Map<string, PartialOrFullChecksum>();

    while (requests.size > 0) {
      const filters = Array.from(requests.values(), createFilter);

      // Historically, checksum may be stored as 'int' or 'double'.
      // More recently, this should be a 'long'.
      // $toLong ensures that we always sum it as a long, avoiding inaccuracies in the calculations.
      const checksumLong = this.storageConfig.longChecksums ? '$checksum' : { $toLong: '$checksum' };

      // Aggregate over a max of `batchLimit` operations at a time.
      // Let's say we have 3 buckets (A, B, C), each with 10 operations, and our batch limit is 12.
      // Then we'll do three batches:
      // 1. Query: A[1-end], B[1-end], C[1-end]
      //    Returns: A[1-10], B[1-2]
      // 2. Query: B[3-end], C[1-end]
      //    Returns: B[3-10], C[1-4]
      // 3. Query: C[5-end]
      //    Returns: C[5-10]
      const aggregate = await collection
        .aggregate(
          [
            {
              $match: {
                $or: filters
              }
            },
            // sort and limit _before_ grouping
            { $sort: { _id: 1 } },
            { $limit: batchLimit },
            {
              $group: {
                _id: '$_id.b',
                checksum_total: { $sum: checksumLong },
                count: { $sum: 1 },
                has_clear_op: {
                  $max: {
                    $cond: [{ $eq: ['$op', 'CLEAR'] }, 1, 0]
                  }
                },
                last_op: { $max: '$_id.o' }
              }
            },
            // Sort the aggregated results (100 max, so should be fast).
            // This is important to identify which buckets we have partial data for.
            { $sort: { _id: 1 } }
          ],
          { session: undefined, readConcern: 'snapshot', maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS }
        )
        .toArray()
        .catch((e) => {
          throw lib_mongo.mapQueryError(e, 'while reading checksums');
        });

      let batchCount = 0;
      let limitReached = false;
      for (let doc of aggregate) {
        const bucket = doc._id;
        const checksum = checksumFromAggregate(doc);

        const existing = partialChecksums.get(bucket);
        if (existing != null) {
          partialChecksums.set(bucket, addPartialChecksums(bucket, existing, checksum));
        } else {
          partialChecksums.set(bucket, checksum);
        }

        batchCount += doc.count;
        if (batchCount == batchLimit) {
          // Limit reached. Request more in the next batch.
          // Note that this only affects the _last_ bucket in a batch.
          limitReached = true;
          const req = requests.get(bucket);
          requests.set(bucket, {
            ...req!,
            start: doc.last_op
          });
        } else {
          // All done for this bucket
          requests.delete(bucket);
        }
      }
      if (!limitReached) {
        break;
      }
    }

    return new Map<string, PartialOrFullChecksum>(
      batch.map((request) => {
        const bucket = request.bucket;
        // Could be null if we got no data
        let partialChecksum = partialChecksums.get(bucket);
        if (partialChecksum == null) {
          partialChecksum = {
            bucket,
            partialCount: 0,
            partialChecksum: 0
          };
        }
        if (request.start == null && isPartialChecksum(partialChecksum)) {
          partialChecksum = {
            bucket,
            count: partialChecksum.partialCount,
            checksum: partialChecksum.partialChecksum
          };
        }

        return [bucket, partialChecksum];
      })
    );
  }
}

class MongoChecksumsV1Impl extends AbstractMongoChecksums {
  async computePartialChecksumsDirectByBucket(
    batch: FetchPartialBucketChecksumByBucket[]
  ): Promise<PartialChecksumMap> {
    return this.computePartialChecksumsForCollection(
      batch,
      this.db.bucket_data as unknown as mongo.Collection<mongo.Document>,
      (request) => ({
        _id: {
          $gt: {
            g: this.group_id,
            b: request.bucket,
            o: request.start ?? new bson.MinKey()
          },
          $lte: {
            g: this.group_id,
            b: request.bucket,
            o: request.end
          }
        }
      })
    );
  }

  protected async fetchPreStates(
    batch: FetchPartialBucketChecksum[]
  ): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>> {
    const preFilters = batch
      .filter((request) => request.start == null)
      .map((request) => ({
        _id: {
          g: this.group_id,
          b: request.bucket
        },
        'compacted_state.op_id': { $exists: true, $lte: request.end }
      }));

    const preStates = new Map<string, { opId: InternalOpId; checksum: BucketChecksum }>();
    if (preFilters.length == 0) {
      return preStates;
    }

    const states = await this.db.bucketStateV1
      .find({
        $or: preFilters
      })
      .toArray();

    for (const state of states) {
      const compactedState = state.compacted_state!;
      preStates.set(state._id.b, {
        opId: compactedState.op_id,
        checksum: {
          bucket: state._id.b,
          checksum: Number(compactedState.checksum),
          count: compactedState.count
        }
      });
    }

    return preStates;
  }

  protected async computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return this.computePartialChecksumsDirectByBucket(batch);
  }
}

class MongoChecksumsV3Impl extends AbstractMongoChecksums {
  constructor(
    db: VersionedPowerSyncMongo,
    group_id: number,
    options: MongoChecksumOptions,
    private readonly mapping: BucketDefinitionMapping
  ) {
    super(db, group_id, options);
  }

  private normalizeBatch(batch: FetchPartialBucketChecksum[]): FetchPartialBucketChecksumV3[] {
    return batch.map((request) => ({
      bucket: request.bucket,
      definitionId: this.mapping.bucketSourceId(request.source),
      start: request.start,
      end: request.end
    }));
  }

  async computePartialChecksumsDirectByDefinition(batch: FetchPartialBucketChecksumV3[]): Promise<PartialChecksumMap> {
    const results = new Map<string, PartialOrFullChecksum>();
    const requestsByDefinition = new Map<string, FetchPartialBucketChecksumV3[]>();

    for (const request of batch) {
      const existing = requestsByDefinition.get(request.definitionId) ?? [];
      existing.push(request);
      requestsByDefinition.set(request.definitionId, existing);
    }

    for (const [definitionId, requests] of requestsByDefinition.entries()) {
      const groupResults = await this.computePartialChecksumsForCollection(
        requests,
        this.db.bucket_data_v3(this.group_id, definitionId) as unknown as mongo.Collection<mongo.Document>,
        createV3BucketFilter
      );
      for (const checksum of groupResults.values()) {
        results.set(checksum.bucket, checksum);
      }
    }

    return new Map<string, PartialOrFullChecksum>(
      batch.map((request) => [request.bucket, results.get(request.bucket) ?? emptyChecksumForRequest(request)])
    );
  }

  protected async fetchPreStates(
    batch: FetchPartialBucketChecksum[]
  ): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>> {
    const preFilters = this.normalizeBatch(batch)
      .filter((request) => request.start == null)
      .map((request) => ({
        _id: {
          d: request.definitionId,
          b: request.bucket
        },
        'compacted_state.op_id': { $exists: true, $lte: request.end }
      }));

    const preStates = new Map<string, { opId: InternalOpId; checksum: BucketChecksum }>();
    if (preFilters.length == 0) {
      return preStates;
    }

    const states = await this.db
      .bucketStateV3(this.group_id)
      .find({
        $or: preFilters
      })
      .toArray();

    for (const state of states) {
      const compactedState = state.compacted_state!;
      preStates.set(state._id.b, {
        opId: compactedState.op_id,
        checksum: {
          bucket: state._id.b,
          checksum: Number(compactedState.checksum),
          count: compactedState.count
        }
      });
    }

    return preStates;
  }

  protected async computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return this.computePartialChecksumsDirectByDefinition(this.normalizeBatch(batch));
  }
}

/**
 * Public checksum API. Delegates to a storage-version-specific implementation.
 */
export class MongoChecksums {
  private readonly impl: AbstractMongoChecksums;
  private readonly v3Impl: MongoChecksumsV3Impl | null;
  private readonly v1Impl: MongoChecksumsV1Impl | null;

  constructor(db: VersionedPowerSyncMongo, group_id: number, options: MongoChecksumOptions) {
    this.v3Impl = options.storageConfig.incrementalReprocessing
      ? new MongoChecksumsV3Impl(
          db,
          group_id,
          options,
          options.mapping ??
            (() => {
              throw new ServiceAssertionError('BucketDefinitionMapping is required for v3 MongoDB checksum queries');
            })()
        )
      : null;
    this.v1Impl = this.v3Impl == null ? new MongoChecksumsV1Impl(db, group_id, options) : null;
    this.impl = this.v3Impl ?? this.v1Impl!;
  }

  async getChecksums(checkpoint: InternalOpId, buckets: BucketChecksumRequest[]): Promise<ChecksumMap> {
    return this.impl.getChecksums(checkpoint, buckets);
  }

  clearCache() {
    this.impl.clearCache();
  }

  async computePartialChecksumsDirect(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return this.impl.computePartialChecksumsDirect(batch);
  }

  async computePartialChecksumsDirectV1(batch: FetchPartialBucketChecksumByBucket[]): Promise<PartialChecksumMap> {
    if (this.v1Impl == null) {
      throw new ServiceAssertionError('V1 checksum routing is only available when incrementalReprocessing is disabled');
    }
    return this.v1Impl.computePartialChecksumsDirectByBucket(batch);
  }

  async computePartialChecksumsDirectV3(batch: FetchPartialBucketChecksumV3[]): Promise<PartialChecksumMap> {
    if (this.v3Impl == null) {
      throw new ServiceAssertionError('V3 checksum routing is only available when incrementalReprocessing is enabled');
    }
    return this.v3Impl.computePartialChecksumsDirectByDefinition(batch);
  }
}

function createV3BucketFilter(request: Pick<FetchPartialBucketChecksumV3, 'bucket' | 'start' | 'end'>) {
  return {
    _id: {
      $gt: {
        b: request.bucket,
        o: request.start ?? new bson.MinKey()
      },
      $lte: {
        b: request.bucket,
        o: request.end
      }
    }
  };
}

function emptyChecksumForRequest(
  request: Pick<FetchPartialBucketChecksum | FetchPartialBucketChecksumV3, 'bucket' | 'start'>
): PartialOrFullChecksum {
  return request.start == null
    ? { bucket: request.bucket, count: 0, checksum: 0 }
    : { bucket: request.bucket, partialCount: 0, partialChecksum: 0 };
}

/**
 * Convert output of the $group stage into a checksum.
 */
function checksumFromAggregate(doc: bson.Document): PartialOrFullChecksum {
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
