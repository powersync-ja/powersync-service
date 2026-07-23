import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  addChecksums,
  bson,
  BucketChecksum,
  CheckpointChecksumInvalidatedError,
  FetchPartialBucketChecksum,
  InternalOpId,
  isPartialChecksum,
  PartialChecksumMap,
  PartialOrFullChecksum,
  SingleSyncConfigBucketDefinitionMapping
} from '@powersync/service-core';
import {
  emptyChecksumForRequest,
  FetchPartialBucketChecksumByBucket,
  FetchPartialBucketChecksumByDefinition,
  MongoChecksumOptions,
  MongoChecksums,
  MongoChecksumSessionContext
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
        this.db.bucketData(this.group_id, definitionId),
        { readOptions: { readConcern: 'snapshot' } }
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
    batch: FetchPartialBucketChecksum[],
    context: MongoChecksumSessionContext
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
      .find(
        {
          $or: preFilters
        },
        {
          ...context.readOptions
        }
      )
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

  protected async computePartialChecksumsInternal(
    batch: FetchPartialBucketChecksum[],
    context: MongoChecksumSessionContext
  ): Promise<PartialChecksumMap> {
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
        this.db.bucketData(this.group_id, definitionId),
        context
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
    collection: lib_mongo.mongo.Collection<BucketDataDocumentV3>,
    context: MongoChecksumSessionContext
  ): Promise<PartialChecksumMap> {
    const requests = new Map<string, FetchPartialBucketChecksumByBucket>();
    for (let request of batch) {
      requests.set(request.bucket, request);
    }

    const aggregate = await this.aggregatePartialChecksums(collection, requests, context);
    const { checksums, startStraddledBuckets } = this.normalizePartialChecksumResults(batch, aggregate);

    if (startStraddledBuckets.size == 0) {
      return checksums;
    }

    // A cached base ends inside a compaction-produced document. Its checksum cannot
    // be safely combined with the document-level checksum, so recalculate this
    // bucket from the beginning. A full result replaces the cached base upstream.
    const fullRequests = batch
      .filter((request) => startStraddledBuckets.has(request.bucket))
      .map((request) => ({ ...request, start: undefined }));
    const fullRequestMap = new Map(fullRequests.map((request) => [request.bucket, request]));
    const fullAggregate = await this.aggregatePartialChecksums(collection, fullRequestMap, context);
    const fullResults = this.normalizePartialChecksumResults(fullRequests, fullAggregate);

    if (fullResults.startStraddledBuckets.size > 0) {
      throw new ServiceAssertionError(
        `Unexpected start-boundary straddle while recalculating bucket(s) ${[...fullResults.startStraddledBuckets].join(', ')}`
      );
    }
    for (const [bucket, checksum] of fullResults.checksums) {
      checksums.set(bucket, checksum);
    }

    return checksums;
  }

  private async aggregatePartialChecksums(
    collection: lib_mongo.mongo.Collection<BucketDataDocumentV3>,
    requests: Map<string, FetchPartialBucketChecksumByBucket>,
    context: MongoChecksumSessionContext
  ): Promise<bson.Document[]> {
    return collection
      .aggregate(this.buildPartialChecksumPipeline(requests), {
        ...context.readOptions,
        maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS
      })
      .toArray()
      .catch((e) => {
        throw lib_mongo.mapQueryError(e, 'while reading checksums');
      });
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
      // Only document-level metadata is used below. Bucket-data operations may
      // be offloaded, and checksum queries must never inspect the ops array.
      {
        $project: {
          _id: 1,
          min_op: 1,
          checksum: 1,
          count: 1,
          target_op: 1,
          has_clear_op: 1,
          bucket_start: 1,
          bucket_end: 1,
          is_start_straddle: {
            $and: [{ $lte: ['$min_op', '$bucket_start'] }, { $gt: ['$_id.o', '$bucket_start'] }]
          },
          is_end_straddle: {
            $gt: ['$_id.o', '$bucket_end']
          }
        }
      },
      // Aggregate document metadata per bucket. A CLEAR makes the result a full
      // checksum that replaces any cached prefix; storage guarantees that
      // documents preceding the CLEAR have already been removed.
      {
        $group: {
          _id: '$_id.b',
          checksum_total: { $sum: '$checksum' },
          count_total: { $sum: '$count' },
          has_clear_op: {
            $max: { $cond: ['$has_clear_op', 1, 0] }
          },
          has_start_straddle: {
            $max: { $cond: ['$is_start_straddle', 1, 0] }
          },
          has_end_straddle: {
            $max: { $cond: ['$is_end_straddle', 1, 0] }
          },
          invalid_end_straddle: {
            $max: {
              $cond: ['$is_end_straddle', { $cond: [{ $gt: ['$target_op', '$bucket_end'] }, 0, 1] }, 0]
            }
          },
          end_straddle_doc_op: {
            $max: { $cond: ['$is_end_straddle', '$_id.o', null] }
          }
        }
      }
    ];
  }

  private normalizePartialChecksumResults(
    batch: FetchPartialBucketChecksumByBucket[],
    aggregate: bson.Document[]
  ): { checksums: PartialChecksumMap; startStraddledBuckets: Set<string> } {
    const requests = new Map(batch.map((request) => [request.bucket, request]));
    const startStraddledBuckets = new Set<string>();
    const partialChecksums = new Map<string, PartialOrFullChecksum>();

    for (const doc of aggregate) {
      const bucket = doc._id as string;
      const request = requests.get(bucket)!;

      if (doc.invalid_end_straddle) {
        throw new ServiceAssertionError(
          `V3 bucket-data document ${bucket}/${doc.end_straddle_doc_op} straddles checkpoint ${request.end} without target_op > checkpoint`
        );
      }
      if (doc.has_end_straddle) {
        throw new CheckpointChecksumInvalidatedError(request.end, bucket);
      }
      if (doc.has_start_straddle) {
        startStraddledBuckets.add(bucket);
        continue;
      }

      const checksum = normalizeDocumentChecksum(doc.checksum_total);
      const current: PartialOrFullChecksum = doc.has_clear_op
        ? {
            bucket,
            checksum,
            count: Number(doc.count_total)
          }
        : {
            bucket,
            partialChecksum: checksum,
            partialCount: Number(doc.count_total)
          };

      partialChecksums.set(bucket, current);
    }

    const checksums = new Map<string, PartialOrFullChecksum>(
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
    return { checksums, startStraddledBuckets };
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

function normalizeDocumentChecksum(value: bigint): number {
  return addChecksums(0, Number(value));
}
