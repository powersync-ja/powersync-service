import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import {
  computeChecksumsForBuckets,
  dirtyBucketBatchForChecksums,
  dirtyBucketBatches
} from '../bucket-operations/compaction-scaffolding.js';
import { bucketStateFilter, resolveBucketDefinitionId } from '../bucket-operations/query-builders.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { SingleBucketStore } from '../common/SingleBucketStore.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import type { MongoSyncBucketStorage } from '../MongoSyncBucketStorageBase.js';
import { BucketStateDocumentV3 } from './models.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { SingleBucketStoreV3 } from './SingleBucketStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoCompactorV3 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  declare protected readonly storage: MongoSyncBucketStorage;

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    yield* dirtyBucketBatches(
      this,
      this.db.bucketStateV3(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    return dirtyBucketBatchForChecksums(
      this,
      this.db.bucketStateV3(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketStateV3(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV3>[], {
        ordered: false
      });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return computeChecksumsForBuckets(
      (batch) => (this.storage.checksums as MongoChecksumsV3).computePartialChecksumsDirectByDefinition(batch),
      this.maxOpId,
      buckets
    );
  }

  protected bucketStateFilter(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    if (definitionId == null) {
      throw new ServiceAssertionError(`Missing definitionId for V3 bucket state filter on bucket ${bucket}`);
    }
    return bucketStateFilter(bucket, definitionId);
  }

  protected async getBucketDataContext(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<SingleBucketStore | null> {
    const resolvedDefinitionId = await resolveBucketDefinitionId(
      {
        bucket,
        definitionId,
        allDefinitionIds: this.storage.mapping.allBucketDefinitionIds(),
        groupId: this.group_id
      },
      async (potentialIds) => {
        const bucketState = await this.db.bucketStateV3(this.group_id).findOne({
          _id: { $in: potentialIds }
        });
        return bucketState ? { definitionId: bucketState._id.d } : null;
      }
    );

    if (resolvedDefinitionId == null) {
      return null;
    }

    return new SingleBucketStoreV3(this.db, {
      bucket,
      definitionId: resolvedDefinitionId,
      replicationStreamId: this.group_id
    });
  }
}
