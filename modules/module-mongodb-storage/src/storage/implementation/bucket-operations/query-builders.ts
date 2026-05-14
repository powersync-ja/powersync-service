import { mongo } from '@powersync/lib-service-mongodb';
import * as bson from 'bson';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketStateDocumentBase } from '../models.js';
import { FetchPartialBucketChecksumByBucket } from '../MongoChecksums.js';

/**
 * Creates a MongoDB filter for bucket data queries based on bucket name and op id range.
 */
export function createBucketFilter<
  TRequest extends Pick<FetchPartialBucketChecksumByBucket, 'bucket' | 'start' | 'end'>
>(request: TRequest) {
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

/**
 * Creates a filter for bucket_state collection queries.
 */
export function bucketStateFilter(
  bucket: string,
  definitionId: BucketDefinitionId
): mongo.Filter<BucketStateDocumentBase> {
  return {
    _id: {
      d: definitionId,
      b: bucket
    }
  };
}

/**
 * Parameters for building bucket data context
 */
export interface BucketDataContextParams {
  bucket: string;
  definitionId: BucketDefinitionId | null;
  allDefinitionIds: BucketDefinitionId[];
  groupId: number;
}

/**
 * Result from looking up bucket state
 */
export interface BucketStateLookup {
  definitionId: BucketDefinitionId;
}

/**
 * Builds the context parameters for bucket data operations.
 *
 * @returns The resolved definitionId, or null if not found
 */
export async function resolveBucketDefinitionId(
  params: BucketDataContextParams,
  lookupBucketState: (potentialIds: Array<{ d: BucketDefinitionId; b: string }>) => Promise<BucketStateLookup | null>
): Promise<BucketDefinitionId | null> {
  const { bucket, definitionId, allDefinitionIds } = params;

  if (definitionId != null) {
    return definitionId;
  }

  // Not the _most_ efficient approach, but this is not used often
  if (allDefinitionIds.length == 0) {
    return null;
  }

  const potentialIds = allDefinitionIds.map((id) => ({ d: id, b: bucket }));
  const bucketState = await lookupBucketState(potentialIds);

  if (bucketState == null) {
    return null;
  }

  return bucketState.definitionId;
}
