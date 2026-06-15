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
import { SingleSyncConfigBucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import {
  checksumFromAggregate,
  emptyChecksumForRequest,
  FetchPartialBucketChecksumByBucket,
  FetchPartialBucketChecksumByDefinition,
  MongoChecksumOptions,
  MongoChecksums
} from '../MongoChecksums.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { BucketDataDocumentV3 } from './models.js';

/**
 * Checksum operations addressed by persisted definition id.
 *
 * Safe on any storage instance, including multi-config replication-side instances - no
 * source resolution is involved. This is the only checksum surface the compactor may use.
 */
export interface DefinitionChecksumOperations {
  computePartialChecksumsDirectByDefinition(
    batch: FetchPartialBucketChecksumByDefinition[]
  ): Promise<PartialChecksumMap>;
}

export interface MongoChecksumOptionsV3 extends MongoChecksumOptions {
  /**
   * The persisted mapping of the single sync config that reads are served from.
   *
   * A thunk rather than a plain value: checksums are constructed eagerly with the storage
   * instance, but the single-config requirement may only be asserted when a read happens.
   */
  syncConfigMapping: () => SingleSyncConfigBucketDefinitionMapping;
}

export class MongoChecksumsV3 extends MongoChecksums implements DefinitionChecksumOperations {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  private readonly syncConfigMapping: () => SingleSyncConfigBucketDefinitionMapping;

  constructor(db: VersionedPowerSyncMongoV3, group_id: number, options: MongoChecksumOptionsV3) {
    super(db, group_id, options);
    this.syncConfigMapping = options.syncConfigMapping;
  }

  async computePartialChecksumsDirectByDefinition(
    batch: FetchPartialBucketChecksumByDefinition[]
  ): Promise<PartialChecksumMap> {
    const results = new Map<string, PartialOrFullChecksum>();
    const requestsByDefinition = new Map<string, FetchPartialBucketChecksumByDefinition[]>();

    for (const request of batch) {
      const existing = requestsByDefinition.get(request.definitionId) ?? [];
      existing.push(request);
      requestsByDefinition.set(request.definitionId, existing);
    }

    for (const [definitionId, requests] of requestsByDefinition.entries()) {
      const groupResults = await this.computeChecksumsByDefinition(
        requests,
        this.db.bucketData(this.group_id, definitionId)
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
    const normalizedBatch = batch.map((request) => ({
      bucket: request.bucket,
      definitionId: this.syncConfigMapping().bucketSourceId(request.source),
      start: request.start,
      end: request.end
    }));

    const preFilters = normalizedBatch
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
      .bucketState(this.group_id)
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
    const normalized = batch.map((request) => ({
      bucket: request.bucket,
      definitionId: this.syncConfigMapping().bucketSourceId(request.source),
      start: request.start,
      end: request.end
    }));

    const results = new Map<string, PartialOrFullChecksum>();
    const requestsByDefinition = new Map<string, FetchPartialBucketChecksumByDefinition[]>();

    for (const request of normalized) {
      const existing = requestsByDefinition.get(request.definitionId) ?? [];
      existing.push(request);
      requestsByDefinition.set(request.definitionId, existing);
    }

    for (const [definitionId, requests] of requestsByDefinition.entries()) {
      const groupResults = await this.computeChecksumsByDefinition(
        requests,
        this.db.bucketData(this.group_id, definitionId)
      );
      for (const checksum of groupResults.values()) {
        results.set(checksum.bucket, checksum);
      }
    }

    return new Map<string, PartialOrFullChecksum>(
      normalized.map((request) => [request.bucket, results.get(request.bucket) ?? emptyChecksumForRequest(request)])
    );
  }

  private async computeChecksumsByDefinition(
    batch: FetchPartialBucketChecksumByBucket[],
    collection: lib_mongo.mongo.Collection<BucketDataDocumentV3>
  ): Promise<PartialChecksumMap> {
    const requests = new Map<string, FetchPartialBucketChecksumByBucket>();
    for (let request of batch) {
      requests.set(request.bucket, request);
    }

    const pipeline = this.buildPartialChecksumPipeline(requests);
    const aggregate = await collection
      .aggregate(pipeline, {
        session: undefined,
        readConcern: 'snapshot',
        maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS
      })
      .toArray()
      .catch((e) => {
        throw lib_mongo.mapQueryError(e, 'while reading checksums');
      });

    return this.normalizePartialChecksumResults(batch, aggregate);
  }

  private buildPartialChecksumPipeline(requests: Map<string, FetchPartialBucketChecksumByBucket>): bson.Document[] {
    const filters = Array.from(requests.values(), createBucketFilter);

    return [
      // $match with $or filters
      {
        $match: {
          $or: filters
        }
      },
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
          },
          bucket_end: {
            $switch: {
              branches: Array.from(requests.entries()).map(([bucket, req]) => ({
                case: { $eq: ['$_id.b', bucket] },
                then: req.end
              })),
              default: new bson.MaxKey()
            }
          }
        }
      },
      // Determine if document is fully included within [start, end] range
      {
        $project: {
          _id: 1,
          min_op: 1,
          checksum: 1,
          count: 1,
          ops: 1,
          bucket_start: 1,
          bucket_end: 1,
          is_fully_included: {
            $and: [{ $gt: ['$min_op', '$bucket_start'] }, { $lte: ['$_id.o', '$bucket_end'] }]
          }
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
                        cond: {
                          $and: [{ $gt: ['$$this.o', '$bucket_start'] }, { $lte: ['$$this.o', '$bucket_end'] }]
                        }
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
                    cond: {
                      $and: [{ $gt: ['$$this.o', '$bucket_start'] }, { $lte: ['$$this.o', '$bucket_end'] }]
                    }
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
                    cond: {
                      $and: [{ $gt: ['$$this.o', '$bucket_start'] }, { $lte: ['$$this.o', '$bucket_end'] }]
                    }
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
      // $sort results
      { $sort: { _id: 1 } }
    ];
  }

  private normalizePartialChecksumResults(
    batch: FetchPartialBucketChecksumByBucket[],
    aggregate: bson.Document[]
  ): PartialChecksumMap {
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

function createBucketFilter(request: Pick<FetchPartialBucketChecksumByBucket, 'bucket' | 'start' | 'end'>) {
  return {
    _id: {
      $gt: {
        b: request.bucket,
        o: request.start ?? new bson.MinKey()
      },
      $lte: {
        b: request.bucket,
        o: new bson.MaxKey()
      }
    },
    min_op: {
      $lte: request.end
    }
  };
}
