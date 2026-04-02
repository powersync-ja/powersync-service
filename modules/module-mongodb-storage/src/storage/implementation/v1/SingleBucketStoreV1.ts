import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentGeneric,
  BucketDataDocumentGenericId,
  SingleBucketStore
} from '../common/SingleBucketStore.js';
import { BucketDataProperties } from '../models.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';
import { BucketDataDocumentV1, BucketDataKeyV1 } from './models.js';

export class SingleBucketStoreV1 implements SingleBucketStore {
  public readonly collection: mongo.Collection<BucketDataDocumentGeneric>;

  constructor(
    private db: VersionedPowerSyncMongoV1,
    public readonly key: BucketKey
  ) {
    this.collection = db.bucketDataV1 as unknown as mongo.Collection<BucketDataDocumentGeneric>;
  }

  docId(o: InternalOpId): BucketDataDocumentGenericId {
    // `satisfies BucketDataKeyV1` checks that we use the correct type for V1 storage
    // `as anyt` is to allow casting to the interface virtual type
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o
    } satisfies BucketDataKeyV1 as any;
  }

  get minId(): BucketDataDocumentGenericId {
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o: new mongo.MinKey()
    } as any;
  }

  get maxId(): BucketDataDocumentGenericId {
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o: new mongo.MaxKey()
    } as any;
  }

  toPersistedDocument(source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric {
    const { o, ...rest } = source;
    const doc: BucketDataDocumentV1 = {
      _id: {
        g: this.key.replicationStreamId,
        b: this.key.bucket,
        o: o
      },
      ...rest
    };
    return doc as unknown as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    const document = doc as unknown as BucketDataDocumentV1;
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
    const document = doc as Pick<BucketDataDocumentV1, '_id' | T>;
    const { _id, ...rest } = document;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }
}
