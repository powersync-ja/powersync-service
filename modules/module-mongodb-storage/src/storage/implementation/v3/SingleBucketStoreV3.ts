import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentGeneric,
  BucketDataDocumentGenericId,
  SingleBucketStore
} from '../common/SingleBucketStore.js';
import { BucketDataKey, BucketDataProperties } from '../models.js';
import { BucketDataDocument, BucketDocumentFormatAdapter } from './document-formats/bucket-document-format.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

// MongoDB's MinKey/MaxKey are special sentinel values that don't match the bigint type
// for _id.o in BucketDataDocumentGenericId, so we need an explicit cast.
function minKeyForBucket(bucket: string): BucketDataDocumentGenericId {
  return {
    b: bucket,
    o: new mongo.MinKey()
  } as unknown as BucketDataDocumentGenericId;
}

function maxKeyForBucket(bucket: string): BucketDataDocumentGenericId {
  return {
    b: bucket,
    o: new mongo.MaxKey()
  } as unknown as BucketDataDocumentGenericId;
}

export class SingleBucketStoreV3 implements SingleBucketStore {
  public readonly collection: mongo.Collection<BucketDataDocumentGeneric>;
  private format = new BucketDocumentFormatAdapter();

  constructor(
    private db: VersionedPowerSyncMongoV3,
    public readonly key: BucketKey
  ) {
    // Cast from the version-specific collection type to the generic interface
    // used across storage versions.
    this.collection = db.bucketData(
      key.replicationStreamId,
      key.definitionId
    ) as unknown as mongo.Collection<BucketDataDocumentGeneric>;
  }

  docId(o: InternalOpId): BucketDataDocumentGenericId {
    // `satisfies BucketDataKey` checks that we use the correct type for V3 storage
    // `as BucketDataDocumentGenericId` does a cast to get the interface virtual type
    return {
      b: this.key.bucket,
      o
    } satisfies BucketDataKey as BucketDataDocumentGenericId;
  }

  get minId(): BucketDataDocumentGenericId {
    return minKeyForBucket(this.key.bucket);
  }

  get maxId(): BucketDataDocumentGenericId {
    return maxKeyForBucket(this.key.bucket);
  }

  toPersistedDocument(source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric {
    return this.format.toPersistedDocument(this.key, source) as unknown as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    return this.format.fromPersistedDocument(this.key, doc as unknown as BucketDataDocument);
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    return this.format.fromPartialPersistedDocument(
      this.key,
      doc as unknown as Pick<BucketDataDocument & BucketDataProperties, '_id' | T>
    );
  }
}
