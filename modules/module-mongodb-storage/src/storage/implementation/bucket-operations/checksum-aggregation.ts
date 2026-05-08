import { mongo } from '@powersync/lib-service-mongodb';
import {
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import { BucketDefinitionId, BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { emptyChecksumForRequest, FetchPartialBucketChecksumByDefinition } from '../MongoChecksums.js';
import { createBucketFilter } from './query-builders.js';

/**
 * Normalizes a batch of checksum requests by resolving source names to definition IDs.
 */
export function normalizeBatch(
  batch: FetchPartialBucketChecksum[],
  mapping: BucketDefinitionMapping
): FetchPartialBucketChecksumByDefinition[] {
  return batch.map((request) => ({
    bucket: request.bucket,
    definitionId: mapping.bucketSourceId(request.source),
    start: request.start,
    end: request.end
  }));
}

/**
 * Fetches pre-compacted checksum states from the bucket_state collection for requests
 * that don't have a start op_id.
 */
export async function fetchPreStates(
  normalizedBatch: FetchPartialBucketChecksumByDefinition[],
  bucketStateCollection: mongo.Collection<any>
): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>> {
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

  const states = await bucketStateCollection
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

/**
 * Computes partial checksums by grouping requests by definition ID and querying
 * the corresponding bucket_data collections.
 */
export async function computePartialChecksumsDirectByDefinition(
  batch: FetchPartialBucketChecksumByDefinition[],
  getBucketData: (definitionId: BucketDefinitionId) => mongo.Collection<any>,
  computePartialChecksumsForCollection: (
    batch: FetchPartialBucketChecksumByDefinition[],
    collection: mongo.Collection<any>,
    createFilter: (request: FetchPartialBucketChecksumByDefinition) => any
  ) => Promise<PartialChecksumMap>
): Promise<PartialChecksumMap> {
  const results = new Map<string, PartialOrFullChecksum>();
  const requestsByDefinition = new Map<string, FetchPartialBucketChecksumByDefinition[]>();

  for (const request of batch) {
    const existing = requestsByDefinition.get(request.definitionId) ?? [];
    existing.push(request);
    requestsByDefinition.set(request.definitionId, existing);
  }

  for (const [definitionId, requests] of requestsByDefinition.entries()) {
    const groupResults = await computePartialChecksumsForCollection(
      requests,
      getBucketData(definitionId),
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

/**
 * Computes partial checksums for a batch by normalizing and delegating to
 * computePartialChecksumsDirectByDefinition.
 */
export async function computePartialChecksumsInternal(
  batch: FetchPartialBucketChecksum[],
  mapping: BucketDefinitionMapping,
  getBucketData: (definitionId: BucketDefinitionId) => mongo.Collection<any>,
  computePartialChecksumsForCollection: (
    batch: FetchPartialBucketChecksumByDefinition[],
    collection: mongo.Collection<any>,
    createFilter: (request: FetchPartialBucketChecksumByDefinition) => any
  ) => Promise<PartialChecksumMap>
): Promise<PartialChecksumMap> {
  const normalized = normalizeBatch(batch, mapping);
  return computePartialChecksumsDirectByDefinition(normalized, getBucketData, computePartialChecksumsForCollection);
}
