import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentGeneric,
  BucketDataDocumentGenericId,
  SingleBucketStore
} from '../common/SingleBucketStore.js';
import { BucketDataProperties } from '../models.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { BucketDataDocumentV3, BucketDataKeyV3 } from './models.js';

export class SingleBucketStoreV3 implements SingleBucketStore {
  public readonly collection: mongo.Collection<BucketDataDocumentGeneric>;

  constructor(
    private db: VersionedPowerSyncMongoV3,
    public readonly key: BucketKey
  ) {
    this.collection = db.bucketDataV3(
      key.replicationStreamId,
      key.definitionId
    ) as unknown as mongo.Collection<BucketDataDocumentGeneric>;
  }

  docId(o: InternalOpId): BucketDataDocumentGenericId {
    // `satisfies BucketDataKeyV3` checks that we use the correct type for V3 storage
    // `as BucketDataDocumentGenericId` does a cast to get the interface virtual type
    return {
      b: this.key.bucket,
      o
    } satisfies BucketDataKeyV3 as BucketDataDocumentGenericId;
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
    const { o, ...rest } = source;
    const doc: BucketDataDocumentV3 = {
      _id: {
        b: this.key.bucket,
        o: o
      },
      ...rest
    };
    return doc as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    const document = doc as BucketDataDocumentV3;
    const { _id, ...rest } = document;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    };
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    const document = doc as Pick<BucketDataDocumentV3, '_id' | T>;
    const { _id, ...rest } = document;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }
}
