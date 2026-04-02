import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import {
  BucketDataDocumentBase,
  BucketStateDocumentBase,
  LEGACY_BUCKET_DATA_DEFINITION_ID,
  TaggedBucketDataDocument
} from '../models.js';
import { BucketDataCollectionContext, DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { BucketDataKeyV1, BucketStateDocumentV1, taggedBucketDataDocumentToV1 } from './models.js';
import type { MongoSyncBucketStorageV1 } from './MongoSyncBucketStorageV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoCompactorV1 extends MongoCompactor {
  // Override types to the more specific ones
  declare protected readonly db: VersionedPowerSyncMongoV1;
  declare protected readonly storage: MongoSyncBucketStorageV1;

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Previously, we used an index on {_id.g: 1, estimate_since_compact.count: 1} to only scan buckets with changes.
    // That works well if there are only a small number of dirty buckets, but it causes repeated rescans while data is
    // still changing. We now iterate through all V1 bucket_state rows for the group and filter after projecting.
    yield* this.dirtyBucketBatchesForCollection(
      this.db.bucketStateV1,
      { g: this.group_id, b: new mongo.MinKey() as any },
      { g: this.group_id, b: new mongo.MaxKey() as any },
      options,
      () => null
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Unlike dirtyBucketBatches, this path is resumable after restart because populateChecksums resets
    // estimate_since_compact as it progresses.
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketStateV1,
      {
        '_id.g': this.group_id,
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      },
      () => null
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db.bucketStateV1.bulkWrite(
      this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV1>[],
      { ordered: false }
    );
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectByBucket(
      buckets.map(({ bucket }) => ({
        bucket,
        end: this.maxOpId
      }))
    );
  }

  protected bucketStateFilter(
    bucket: string,
    _definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    return {
      _id: {
        g: this.group_id,
        b: bucket
      }
    };
  }

  protected bucketDataKey(bucket: string, opId: InternalOpId | mongo.MinKey | mongo.MaxKey): BucketDataKeyV1 {
    return {
      g: this.group_id,
      b: bucket,
      o: opId as any
    };
  }

  protected async getBucketDataContext(
    _bucket: string,
    _definitionId: BucketDefinitionId | null
  ): Promise<BucketDataCollectionContext | null> {
    return {
      collection: this.db.v1_bucket_data as unknown as mongo.Collection<BucketDataDocumentBase>,
      definitionId: LEGACY_BUCKET_DATA_DEFINITION_ID
    };
  }

  protected collectionBucketDataDocument(document: TaggedBucketDataDocument): BucketDataDocumentBase {
    return taggedBucketDataDocumentToV1(this.group_id, document);
  }
}
