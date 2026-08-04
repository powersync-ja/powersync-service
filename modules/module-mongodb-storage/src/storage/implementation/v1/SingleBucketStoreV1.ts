import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataProperties } from '../models.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';
import { BucketDataDocumentV1, BucketDataKeyV1, serializeBucketDataV1 } from './models.js';

export class SingleBucketStoreV1 {
  public readonly collection: mongo.Collection<BucketDataDocumentV1>;

  constructor(
    db: VersionedPowerSyncMongoV1,
    public readonly key: BucketKey
  ) {
    this.collection = db.bucketDataV1;
  }

  docId(o: InternalOpId): BucketDataKeyV1 {
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o
    };
  }

  get minId(): BucketDataKeyV1 {
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o: new mongo.MinKey() as any
    };
  }

  get maxId(): BucketDataKeyV1 {
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o: new mongo.MaxKey() as any
    };
  }

  toPersistedDocument(source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentV1 {
    return serializeBucketDataV1({ bucketKey: this.key, ...source });
  }

  fromPersistedDocument(doc: BucketDataDocumentV1): BucketDataDoc {
    const { _id, ...rest } = doc;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    };
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentV1, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    const { _id, ...rest } = doc;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }
}
