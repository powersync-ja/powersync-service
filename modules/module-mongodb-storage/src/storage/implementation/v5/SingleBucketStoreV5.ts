import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentGeneric,
  BucketDataDocumentGenericId,
  SingleBucketStore
} from '../common/SingleBucketStore.js';
import { BucketDataProperties } from '../models.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';
import { BucketDataDocumentV5, BucketDataKeyV5, loadBucketDataDocumentV5, serializeBucketDataV5 } from './models.js';

export class SingleBucketStoreV5 implements SingleBucketStore {
  public readonly collection: mongo.Collection<BucketDataDocumentGeneric>;

  constructor(
    private db: VersionedPowerSyncMongoV5,
    public readonly key: BucketKey
  ) {
    this.collection = db.bucketDataV5(
      key.replicationStreamId,
      key.definitionId
    ) as unknown as mongo.Collection<BucketDataDocumentGeneric>;
  }

  docId(o: InternalOpId): BucketDataDocumentGenericId {
    // `satisfies BucketDataKeyV5` checks that we use the correct type for V5 storage
    // `as BucketDataDocumentGenericId` does a cast to get the interface virtual type
    return {
      b: this.key.bucket,
      o
    } satisfies BucketDataKeyV5 as BucketDataDocumentGenericId;
  }

  get minId(): BucketDataDocumentGenericId {
    return {
      b: this.key.bucket,
      o: new mongo.MinKey()
    } as any; // No way to properly type this
  }

  get maxId(): BucketDataDocumentGenericId {
    return {
      b: this.key.bucket,
      o: new mongo.MaxKey()
    } as any; // No way to properly type this
  }

  toPersistedDocument(source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric {
    return serializeBucketDataV5({ bucketKey: this.key, ...source }) as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    return loadBucketDataDocumentV5(this.key, doc as BucketDataDocumentV5);
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    const document = doc as Pick<BucketDataDocumentV5, '_id' | T>;
    const { _id, ...rest } = document;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }
}
