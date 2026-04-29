import * as lib_mongo from '@powersync/lib-service-mongodb';
import {
  bson,
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  isPartialChecksum,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { BucketDataDocumentBase } from '../models.js';
import {
  checksumFromAggregate,
  emptyChecksumForRequest,
  FetchPartialBucketChecksumByBucket,
  FetchPartialBucketChecksumV3 as FetchPartialBucketChecksumV5,
  MongoChecksumOptions,
  MongoChecksums
} from '../MongoChecksums.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoChecksumsV5 extends MongoChecksums {
  declare protected readonly db: VersionedPowerSyncMongoV5;
  private readonly mapping: BucketDefinitionMapping;

  constructor(db: VersionedPowerSyncMongoV5, group_id: number, options: MongoChecksumOptions) {
    super(db, group_id, options);
    this.mapping = options.mapping!;
  }

  private normalizeBatch(batch: FetchPartialBucketChecksum[]): FetchPartialBucketChecksumV5[] {
    return batch.map((request) => ({
      bucket: request.bucket,
      definitionId: this.mapping.bucketSourceId(request.source),
      start: request.start,
      end: request.end
    }));
  }

  async computePartialChecksumsDirectByDefinition(batch: FetchPartialBucketChecksumV5[]): Promise<PartialChecksumMap> {
    const results = new Map<string, PartialOrFullChecksum>();
    const requestsByDefinition = new Map<string, FetchPartialBucketChecksumV5[]>();

    for (const request of batch) {
      const existing = requestsByDefinition.get(request.definitionId) ?? [];
      existing.push(request);
      requestsByDefinition.set(request.definitionId, existing);
    }

    for (const [definitionId, requests] of requestsByDefinition.entries()) {
      const groupResults = await this.computePartialChecksumsForCollection(
        requests,
        this.db.bucketDataV5(this.group_id, definitionId) as unknown as lib_mongo.mongo.Collection<
          import('../models.js').BucketDataDocumentBase
        >,
        createV5BucketFilter
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
      .bucketStateV5(this.group_id)
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

  protected override async computePartialChecksumsForCollection<
    TRequest extends FetchPartialBucketChecksumByBucket,
    TBucketDataDocument extends BucketDataDocumentBase
  >(
    batch: TRequest[],
    collection: lib_mongo.mongo.Collection<TBucketDataDocument>,
    createFilter: (request: TRequest) => any
  ): Promise<PartialChecksumMap> {
    const requests = new Map<string, TRequest>();
    for (let request of batch) {
      requests.set(request.bucket, request);
    }

    const filters = Array.from(requests.values(), createFilter);

    const aggregate = await collection
      .aggregate(
        [
          {
            $match: {
              $or: filters
            }
          },
          { $sort: { _id: 1 } },
          // Add per-bucket start threshold
          {
            $addFields: {
              bucket_start: {
                $switch: {
                  branches: Array.from(requests.entries()).map(([bucket, req]) => ({
                    case: { $eq: ['$_id.b', bucket] },
                    then: req.start ?? new bson.MinKey()
                  })),
                  default: new bson.MinKey()
                }
              }
            }
          },
          // Determine if document is fully included
          {
            $project: {
              _id: 1,
              min_op: 1,
              checksum: 1,
              count: 1,
              ops: 1,
              bucket_start: 1,
              is_fully_included: { $gt: ['$min_op', '$bucket_start'] }
            }
          },
          // Compute included checksum, count, and clear op detection
          {
            $project: {
              _id: 1,
              checksum_total: {
                $cond: {
                  if: '$is_fully_included',
                  then: '$checksum',
                  else: {
                    $sum: {
                      $map: {
                        input: {
                          $filter: {
                            input: '$ops',
                            cond: { $gt: ['$$this.o', '$bucket_start'] }
                          }
                        },
                        in: '$$this.checksum'
                      }
                    }
                  }
                }
              },
              count_total: {
                $cond: {
                  if: '$is_fully_included',
                  then: '$count',
                  else: {
                    $size: {
                      $filter: {
                        input: '$ops',
                        cond: { $gt: ['$$this.o', '$bucket_start'] }
                      }
                    }
                  }
                }
              },
              has_clear_op: {
                $max: {
                  $map: {
                    input: {
                      $filter: {
                        input: '$ops',
                        cond: { $gt: ['$$this.o', '$bucket_start'] }
                      }
                    },
                    in: { $cond: [{ $eq: ['$$this.op', 'CLEAR'] }, 1, 0] }
                  }
                }
              },
              last_op: { $max: '$_id.o' }
            }
          },
          // Group by bucket
          {
            $group: {
              _id: '$_id.b',
              checksum_total: { $sum: '$checksum_total' },
              count: { $sum: '$count_total' },
              has_clear_op: { $max: '$has_clear_op' },
              last_op: { $max: '$last_op' }
            }
          },
          { $sort: { _id: 1 } }
        ],
        { session: undefined, readConcern: 'snapshot', maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS }
      )
      .toArray()
      .catch((e) => {
        throw lib_mongo.mapQueryError(e, 'while reading checksums');
      });

    const partialChecksums = new Map<string, PartialOrFullChecksum>();
    for (let doc of aggregate) {
      const bucket = doc._id;
      partialChecksums.set(bucket, checksumFromAggregate(doc));
    }

    return new Map<string, PartialOrFullChecksum>(
      batch.map((request) => {
        const bucket = request.bucket;
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

function createV5BucketFilter(request: Pick<FetchPartialBucketChecksumV5, 'bucket' | 'start' | 'end'>) {
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
