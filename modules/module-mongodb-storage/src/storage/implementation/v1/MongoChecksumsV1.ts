import * as lib_mongo from '@powersync/lib-service-mongodb';
import {
  addPartialChecksums,
  bson,
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  isPartialChecksum,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import {
  checksumFromAggregate,
  DEFAULT_OPERATION_BATCH_LIMIT,
  FetchPartialBucketChecksumByBucket,
  MongoChecksums
} from '../MongoChecksums.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoChecksumsV1 extends MongoChecksums {
  declare protected readonly db: VersionedPowerSyncMongoV1;

  async computePartialChecksumsDirectByBucket(
    batch: FetchPartialBucketChecksumByBucket[]
  ): Promise<PartialChecksumMap> {
    const collection = this.db.bucketDataV1;
    const createFilter = (request: FetchPartialBucketChecksumByBucket) => ({
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

    const batchLimit = this.options?.operationBatchLimit ?? DEFAULT_OPERATION_BATCH_LIMIT;

    // Map requests by bucket. We adjust this as we get partial results.
    let requests = new Map<string, FetchPartialBucketChecksumByBucket>();
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
          {
            session: undefined,
            readConcern: 'snapshot',
            maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS
          }
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
        let partialChecksum = partialChecksums.get(bucket);
        // Could be null if we got no data
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
