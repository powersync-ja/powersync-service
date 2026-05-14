import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';

export async function* dirtyBucketBatches<TBucketState extends BucketStateDocumentBase>(
  compactor: MongoCompactor,
  collection: mongo.Collection<TBucketState>,
  options: {
    minBucketChanges: number;
    minChangeRatio: number;
  },
  getDefinitionId: (state: TBucketState) => BucketDefinitionId | null
): AsyncGenerator<DirtyBucket[]> {
  if (options.minBucketChanges <= 0) {
    throw new ReplicationAssertionError('minBucketChanges must be >= 1');
  }
  yield* compactor.dirtyBucketBatchesForCollection(
    collection,
    // MongoDB MinKey/MaxKey sentinel values don't match the typed _id shape,
    // so we cast through unknown for the scan boundaries.
    { d: new mongo.MinKey(), b: new mongo.MinKey() } as unknown as TBucketState['_id'],
    { d: new mongo.MaxKey(), b: new mongo.MaxKey() } as unknown as TBucketState['_id'],
    options,
    getDefinitionId
  );
}

export async function dirtyBucketBatchForChecksums<TBucketState extends BucketStateDocumentBase>(
  compactor: MongoCompactor,
  collection: mongo.Collection<TBucketState>,
  options: { minBucketChanges: number },
  getDefinitionId: (state: mongo.WithId<TBucketState>) => BucketDefinitionId | null
): Promise<DirtyBucket[]> {
  if (options.minBucketChanges <= 0) {
    throw new ReplicationAssertionError('minBucketChanges must be >= 1');
  }
  return compactor.dirtyBucketBatchForChecksumsForCollection(
    collection,
    // MongoDB Filter<T> doesn't accept dotted field paths like 'estimate_since_compact.count'
    // in its type, so we need an explicit cast.
    {
      'estimate_since_compact.count': { $gte: options.minBucketChanges }
    } as unknown as mongo.Filter<TBucketState>,
    getDefinitionId
  );
}

export async function computeChecksumsForBuckets(
  computeChecksums: (
    batch: { bucket: string; definitionId: BucketDefinitionId; end: bigint }[]
  ) => Promise<storage.PartialChecksumMap>,
  maxOpId: bigint,
  buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
): Promise<storage.PartialChecksumMap> {
  return computeChecksums(
    buckets.map(({ bucket, definitionId }) => {
      if (definitionId == null) {
        throw new ServiceAssertionError(`Missing definitionId for bucket checksum update on bucket ${bucket}`);
      }
      return {
        bucket,
        definitionId,
        end: maxOpId
      };
    })
  );
}
