import {
  addPartialChecksums,
  bson,
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
import * as lib_mongo from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { PowerSyncMongo } from './db.js';

/**
 * Checksum calculation options, primarily for tests.
 */
export interface MongoChecksumOptions {
  /**
   * Force calculating checksums for one bucket at a time, in batches.
   *
   * This is slower when many buckets are involved, but is less likely to time out.
   */
  forceBatchedImplementation?: boolean;
  /**
   * When using the batched implementation, this specifies the limit on the number of documents to calculate
   * a checksum on at a time.
   */
  batchLimit?: number;
}

const DEFAULT_BATCH_LIMIT = 50_000;

/**
 * Checksum query implementation.
 */
export class MongoChecksums {
  private cache = new ChecksumCache({
    fetchChecksums: (batch) => {
      return this.getChecksumsInternal(batch);
    }
  });

  constructor(
    private db: PowerSyncMongo,
    private group_id: number,
    private options?: MongoChecksumOptions
  ) {}

  /**
   * Calculate checksums, utilizing the cache.
   */
  async getChecksums(checkpoint: InternalOpId, buckets: string[]): Promise<ChecksumMap> {
    return this.cache.getChecksumMap(checkpoint, buckets);
  }

  clearCache() {
    this.cache.clear();
  }

  /**
   * Calculate (partial) checksums from bucket_state and the data collection.
   *
   * Results are not cached.
   */
  private async getChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    if (batch.length == 0) {
      return new Map();
    }

    const preFilters: any[] = [];
    for (let request of batch) {
      if (request.start == null) {
        preFilters.push({
          _id: {
            g: this.group_id,
            b: request.bucket
          },
          'compacted_state.op_id': { $exists: true, $lte: request.end }
        });
      }
    }

    const preStates = new Map<string, { opId: InternalOpId; checksum: BucketChecksum }>();

    if (preFilters.length > 0) {
      // For un-cached bucket checksums, attempt to use the compacted state first.
      const states = await this.db.bucket_state
        .find({
          $or: preFilters
        })
        .toArray();
      for (let state of states) {
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
    }

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

    const queriedChecksums = await this.queryPartialChecksums(mappedRequests);

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
   * Calculate (partial) checksums from the data collection directly.
   */
  async queryPartialChecksums(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    if (this.options?.forceBatchedImplementation) {
      // Primarily for testing
      return await this.queryPartialChecksumsFallback(batch);
    }
    try {
      return await this.queryPartialChecksumsInternal(batch);
    } catch (e) {
      if (e.codeName == 'MaxTimeMSExpired') {
        logger.warn(`Checksum query timed out; falling back to slower version`, e);
        // Timeout - try the slower but more robust version
        return await this.queryPartialChecksumsFallback(batch);
      }
      throw lib_mongo.mapQueryError(e, 'while reading checksums');
    }
  }

  private async queryPartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    const filters: any[] = [];
    for (let request of batch) {
      filters.push({
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
      });
    }

    const aggregate = await this.db.bucket_data
      .aggregate(
        [
          {
            $match: {
              $or: filters
            }
          },
          CHECKSUM_QUERY_GROUP_STAGE
        ],
        { session: undefined, readConcern: 'snapshot', maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS }
      )
      // Don't map the error here - we want to keep timeout errors as-is
      .toArray();

    const partialChecksums = new Map<string, PartialOrFullChecksum>(
      aggregate.map((doc) => {
        const bucket = doc._id;
        return [bucket, checksumFromAggregate(doc)];
      })
    );

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

  /**
   * Checksums for large buckets can run over the query timeout.
   * To avoid this, we query in batches.
   * This version can handle larger amounts of data, but is slower, especially for many buckets.
   */
  async queryPartialChecksumsFallback(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    const partialChecksums = new Map<string, PartialOrFullChecksum>();
    for (let request of batch) {
      const checksum = await this.slowChecksum(request);
      partialChecksums.set(request.bucket, checksum);
    }

    return partialChecksums;
  }

  private async slowChecksum(request: FetchPartialBucketChecksum): Promise<PartialOrFullChecksum> {
    const batchLimit = this.options?.batchLimit ?? DEFAULT_BATCH_LIMIT;

    let lowerBound = request.start ?? 0n;
    const bucket = request.bucket;

    let runningChecksum: PartialOrFullChecksum = {
      bucket,
      partialCount: 0,
      partialChecksum: 0
    };
    if (request.start == null) {
      runningChecksum = {
        bucket,
        count: 0,
        checksum: 0
      };
    }

    while (true) {
      const filter = {
        _id: {
          $gt: {
            g: this.group_id,
            b: bucket,
            o: lowerBound
          },
          $lte: {
            g: this.group_id,
            b: bucket,
            o: request.end
          }
        }
      };
      const docs = await this.db.bucket_data
        .aggregate(
          [
            {
              $match: filter
            },
            // sort and limit _before_ grouping
            { $sort: { _id: 1 } },
            { $limit: batchLimit },
            CHECKSUM_QUERY_GROUP_STAGE
          ],
          { session: undefined, readConcern: 'snapshot', maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS }
        )
        .toArray();
      const doc = docs[0];
      if (doc == null) {
        return runningChecksum;
      }
      const partial = checksumFromAggregate(doc);
      runningChecksum = addPartialChecksums(bucket, runningChecksum, partial);
      const isFinal = doc.count != batchLimit;
      if (isFinal) {
        break;
      } else {
        lowerBound = doc.last_op;
      }
    }
    return runningChecksum;
  }
}

const CHECKSUM_QUERY_GROUP_STAGE = {
  $group: {
    _id: '$_id.b',
    // Historically, checksum may be stored as 'int' or 'double'.
    // More recently, this should be a 'long'.
    // $toLong ensures that we always sum it as a long, avoiding inaccuracies in the calculations.
    checksum_total: { $sum: { $toLong: '$checksum' } },
    count: { $sum: 1 },
    has_clear_op: {
      $max: {
        $cond: [{ $eq: ['$op', 'CLEAR'] }, 1, 0]
      }
    },
    last_op: { $max: '$_id.o' }
  }
};

/**
 * Convert output of CHECKSUM_QUERY_GROUP_STAGE into a checksum.
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
