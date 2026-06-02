import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentGeneric,
  BucketDataDocumentGenericId,
  SingleBucketStore
} from '../common/SingleBucketStore.js';
import { BucketDataKey, BucketDataProperties } from '../models.js';
import { loadBucketDataDocument, serializeBucketData } from './bucket-format.js';
import { BucketDataDocumentV3 } from './models.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

function extractPartialDocumentFields(doc: unknown): { _id: { o: bigint }; [key: string]: unknown } {
  if (typeof doc !== 'object' || doc === null) {
    throw new Error('Invalid partial document: expected object');
  }
  const d = doc as Record<string, unknown>;
  const id = d._id;
  if (typeof id !== 'object' || id === null || !('o' in id)) {
    throw new Error('Invalid partial document: missing _id.o');
  }
  return d as { _id: { o: bigint }; [key: string]: unknown };
}

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
    return serializeBucketData(this.key.bucket, [
      { bucketKey: this.key, ...source }
    ]) as unknown as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc {
    const generator = loadBucketDataDocument(this.key, doc as unknown as BucketDataDocumentV3);
    const first = generator.next();
    if (first.done) {
      throw new Error('Empty ops array in BucketDataDocumentV3');
    }
    return first.value;
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    const document = doc as unknown as Pick<BucketDataDocumentV3, '_id' | 'ops'>;
    const op = document.ops?.[0];
    if (op == null) {
      const fields = extractPartialDocumentFields(doc);
      const { _id, ...rest } = fields;
      return {
        bucketKey: this.key,
        o: _id.o,
        ...rest
      } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
    }
    return {
      bucketKey: this.key,
      ...op
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }
}
