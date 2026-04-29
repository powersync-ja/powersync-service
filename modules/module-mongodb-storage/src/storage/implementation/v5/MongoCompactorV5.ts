import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { SingleBucketStore } from '../common/SingleBucketStore.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { BucketStateDocumentV5 } from './models.js';
import type { MongoSyncBucketStorageV5 } from './MongoSyncBucketStorageV5.js';
import { SingleBucketStoreV5 } from './SingleBucketStoreV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoCompactorV5 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV5;
  declare protected readonly storage: MongoSyncBucketStorageV5;

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Same scan strategy as V1, but with the V5 bucket_state key shape.
    yield* this.dirtyBucketBatchesForCollection(
      this.db.bucketStateV5(this.group_id),
      { d: new mongo.MinKey() as any, b: new mongo.MinKey() as any },
      { d: new mongo.MaxKey() as any, b: new mongo.MaxKey() as any },
      options,
      (bucketState) => (bucketState as BucketStateDocumentV5)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketStateV5(this.group_id),
      {
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      },
      (bucketState) => (bucketState as BucketStateDocumentV5)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketStateV5(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV5>[], { ordered: false });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectByDefinition(
      buckets.map(({ bucket, definitionId }) => {
        if (definitionId == null) {
          throw new ServiceAssertionError(`Missing definitionId for V5 bucket checksum update on bucket ${bucket}`);
        }
        return {
          bucket,
          definitionId,
          end: this.maxOpId
        };
      })
    );
  }

  protected bucketStateFilter(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    if (definitionId == null) {
      throw new ServiceAssertionError(`Missing definitionId for V5 bucket state filter on bucket ${bucket}`);
    }
    return {
      _id: {
        d: definitionId,
        b: bucket
      }
    };
  }

  protected async getBucketDataContext(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<SingleBucketStore | null> {
    if (definitionId == null) {
      // Not the _most_ efficient approach, but this is not used often
      const allDefinitionIds = this.storage.mapping.allBucketDefinitionIds();
      if (allDefinitionIds.length == 0) {
        return null;
      }
      const potentialIds = allDefinitionIds.map((definitionId) => ({ d: definitionId, b: bucket }));
      const bucketState = await this.db.bucketStateV5(this.group_id).findOne({
        _id: { $in: potentialIds }
      });
      if (bucketState == null) {
        return null;
      }
      definitionId = bucketState._id.d;
    }

    return new SingleBucketStoreV5(this.db, { bucket, definitionId, replicationStreamId: this.group_id });
  }
}
