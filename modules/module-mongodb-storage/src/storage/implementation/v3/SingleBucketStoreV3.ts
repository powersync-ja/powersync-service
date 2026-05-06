import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentGeneric,
  BucketDataDocumentGenericId,
  SingleBucketStore
} from '../common/SingleBucketStore.js';
import { BucketDataProperties } from '../models.js';
import { BucketDataKeyV3, V3FormatAdapter } from '../document-formats/v3-format.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class SingleBucketStoreV3 implements SingleBucketStore {
  public readonly collection: mongo.Collection<BucketDataDocumentGeneric>;
  private format = new V3FormatAdapter();

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
    return this.format.toPersistedDocument(this.key, source);
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    return this.format.fromPersistedDocument(this.key, doc);
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    return this.format.fromPartialPersistedDocument(this.key, doc);
  }
}
