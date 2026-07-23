import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
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
  checksumFromAggregate,
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
      // Sort on the complete _id so the _id index can satisfy it, and so the
      // grouped boundary metadata comes from each bucket's first and last documents.
      { $sort: { _id: 1 } },
      // Only document-level metadata is used below. Bucket-data operations may
      // be offloaded, and checksum queries must never inspect the ops array.
      {
        $project: {
          _id: 1,
          min_op: 1,
          checksum: 1,
          count: 1,
          target_op: 1,
          has_clear_op: 1
        }
      },
      // Aggregate document metadata per bucket. A CLEAR makes the result a full
      // checksum that replaces any cached prefix; storage guarantees that
      // documents preceding the CLEAR have already been removed.
      {
        $group: {
          _id: '$_id.b',
          checksum_total: { $sum: '$checksum' },
          count: { $sum: '$count' },
          has_clear_op: {
            $max: { $cond: ['$has_clear_op', 1, 0] }
          },
          first_min_op: { $first: '$min_op' },
          last_op: { $last: '$_id.o' },
          last_target_op: { $last: '$target_op' }
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

      // By the ordered, disjoint range invariants on BucketDataDocumentV3,
      // only the last matched document can straddle the end boundary. Original
      // append-only documents cannot contain a valid checkpoint boundary, so a
      // straddle must carry a compaction target beyond the invalid checkpoint.
      if (doc.last_op > request.end) {
        if (doc.last_target_op == null || doc.last_target_op <= request.end) {
          throw new ServiceAssertionError(
            `V3 bucket-data document ${bucket}/${doc.last_op} straddles checkpoint ${request.end} without target_op > checkpoint`
          );
        }
        throw new CheckpointChecksumInvalidatedError(request.end, bucket);
      }
      // The _id filter excludes documents ending at or before start. By the
      // same range invariants, only the first matched document can contain it.
      if (request.start != null && doc.first_min_op <= request.start) {
        startStraddledBuckets.add(bucket);
        continue;
      }

      partialChecksums.set(bucket, checksumFromAggregate(doc));
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
