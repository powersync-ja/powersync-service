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
import {
  checksumFromAggregate,
  emptyChecksumForRequest,
  FetchPartialBucketChecksumByBucket,
  FetchPartialBucketChecksumByDefinition,
  MongoChecksumOptions,
  MongoChecksums
} from '../MongoChecksums.js';
import {
  computePartialChecksumsInternal,
  fetchPreStates,
  normalizeBatch
} from '../bucket-operations/checksum-aggregation.js';
import { createBucketFilter } from '../bucket-operations/query-builders.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { BucketDataDocumentBase } from '../models.js';

export class MongoChecksumsV3 extends MongoChecksums {
  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

  private readonly mapping: BucketDefinitionMapping;

  constructor(db: VersionedPowerSyncMongo, group_id: number, options: MongoChecksumOptions) {
    super(db, group_id, options);
    this.mapping = options.mapping!;
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
      const groupResults = await this.computePartialChecksumsForCollection(
        requests,
        this.db.bucketData<BucketDataDocumentBase>(this.group_id, definitionId),
        createBucketFilter
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
    return fetchPreStates(normalizeBatch(batch, this.mapping), this.db.bucketState(this.group_id));
  }

  protected async computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return computePartialChecksumsInternal(
      batch,
      this.mapping,
      (definitionId) => this.db.bucketData<BucketDataDocumentBase>(this.group_id, definitionId),
      (batch, collection, createFilter) => this.computePartialChecksumsForCollection(batch, collection, createFilter)
    );
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

    const pipeline = this.buildPartialChecksumPipeline(requests, createFilter);
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

  private buildPartialChecksumPipeline<TRequest extends FetchPartialBucketChecksumByBucket>(
    requests: Map<string, TRequest>,
    createFilter: (request: TRequest) => any
  ): bson.Document[] {
    const filters = Array.from(requests.values(), createFilter);

    return [
      // $match with $or filters
      {
        $match: {
          $or: filters
        }
      },
      // $sort by _id
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
      // $sort results
      { $sort: { _id: 1 } }
    ];
  }

  private normalizePartialChecksumResults<TRequest extends FetchPartialBucketChecksumByBucket>(
    batch: TRequest[],
    aggregate: any[]
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
