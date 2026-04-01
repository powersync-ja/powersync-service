import { MONGO_OPERATION_TIMEOUT_MS, mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { BucketDataDocumentBase, BucketStateDocumentBase, TaggedBucketDataDocument } from '../models.js';
import { BucketDataKeyV3, BucketStateDocumentV3, taggedBucketDataDocumentToV3 } from './models.js';
import type { MongoSyncBucketStorageV3 } from './MongoSyncBucketStorageV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoCompactorV3 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  declare protected readonly storage: MongoSyncBucketStorageV3;

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Same scan strategy as V1, but with the V3 bucket_state key shape.
    yield* this.dirtyBucketBatchesForCollection(
      this.db.bucketStateV3(this.group_id),
      { d: new mongo.MinKey() as any, b: new mongo.MinKey() as any },
      { d: new mongo.MaxKey() as any, b: new mongo.MaxKey() as any },
      options,
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketStateV3(this.group_id),
      {
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      },
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  protected async flushBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketStateV3(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV3>[], { ordered: false });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectByDefinition(
      buckets.map(({ bucket, definitionId }) => {
        if (definitionId == null) {
          throw new ServiceAssertionError(`Missing definitionId for V3 bucket checksum update on bucket ${bucket}`);
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
      throw new ServiceAssertionError(`Missing definitionId for V3 bucket state filter on bucket ${bucket}`);
    }
    return {
      _id: {
        d: definitionId,
        b: bucket
      }
    };
  }

  protected bucketDataKey(bucket: string, opId: InternalOpId | mongo.MinKey | mongo.MaxKey): BucketDataKeyV3 {
    return { b: bucket, o: opId as any };
  }

  protected async getBucketDataCollection(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<{ collection: mongo.Collection<BucketDataDocumentBase>; definitionId: BucketDefinitionId } | null> {
    if (definitionId != null) {
      return {
        collection: this.db.bucketDataV3(
          this.group_id,
          definitionId
        ) as unknown as mongo.Collection<BucketDataDocumentBase>,
        definitionId
      };
    }

    // FIXME: This is slow. It is only used when compacting a single bucket without a known definition id.
    for (const collection of await this.db.listBucketDataCollectionsV3(this.group_id)) {
      const existing = await collection.findOne(
        { '_id.b': bucket },
        { projection: { _id: 1 }, maxTimeMS: MONGO_OPERATION_TIMEOUT_MS }
      );
      if (existing != null) {
        const resolvedDefinitionId = collection.collectionName.replace(`bucket_data_${this.group_id}_`, '');
        return {
          collection: collection as unknown as mongo.Collection<BucketDataDocumentBase>,
          definitionId: resolvedDefinitionId
        };
      }
    }

    return null;
  }

  protected collectionBucketDataDocument(document: TaggedBucketDataDocument): BucketDataDocumentBase {
    return taggedBucketDataDocumentToV3(document);
  }
}
