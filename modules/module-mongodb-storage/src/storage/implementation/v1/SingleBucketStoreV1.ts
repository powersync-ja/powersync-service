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
import { BucketDataDocumentV1, BucketDataKeyV1, serializeBucketDataV1 } from './models.js';

export class SingleBucketStoreV1 implements SingleBucketStore {
  public readonly collection: mongo.Collection<BucketDataDocumentGeneric>;

  constructor(
    private db: VersionedPowerSyncMongoV1,
    public readonly key: BucketKey
  ) {
    // Cast from the V1-specific collection type to the generic interface used
    // across storage versions.
    this.collection = db.bucketDataV1 as unknown as mongo.Collection<BucketDataDocumentGeneric>;
  }

  docId(o: InternalOpId): BucketDataDocumentGenericId {
    // `satisfies BucketDataKeyV1` checks that we use the correct type for V1 storage.
    // `as any` casts to the interface virtual type.
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o
    } satisfies BucketDataKeyV1 as any;
  }

  get minId(): BucketDataDocumentGenericId {
    // MongoDB MinKey doesn't match the bigint type for _id.o, so we need an explicit cast.
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o: new mongo.MinKey()
    } as any;
  }

  get maxId(): BucketDataDocumentGenericId {
    // MongoDB MaxKey doesn't match the bigint type for _id.o, so we need an explicit cast.
    return {
      g: this.key.replicationStreamId,
      b: this.key.bucket,
      o: new mongo.MaxKey()
    } as any;
  }

  toPersistedDocument(source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric {
    // BucketDataDocumentGeneric is a virtual type — the actual shape is V1-specific.
    return serializeBucketDataV1({ bucketKey: this.key, ...source }) as unknown as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    // We know the concrete type is BucketDataDocumentV1 in V1 storage logic.
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
    // We know the concrete type is BucketDataDocumentV1, but Pick prevents a direct cast.
    const document = doc as Pick<BucketDataDocumentV1, '_id' | T>;
    const { _id, ...rest } = document;
    return {
      bucketKey: this.key,
      o: _id.o,
      ...rest
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }
}
