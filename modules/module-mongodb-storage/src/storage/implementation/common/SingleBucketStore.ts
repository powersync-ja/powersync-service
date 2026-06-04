import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataProperties } from '../models.js';
import { BucketDataDoc, BucketKey } from './BucketDataDoc.js';

const GENERIC_ID = Symbol('BucketDataDocumentGenericId');
export type BucketDataDocumentGenericId = {
  b: string;
  o: InternalOpId;
  // Hack to ensure this can't be constructed directly
  [GENERIC_ID]: true;
};

/**
 * This document is never actually constructed - we use it as a "virtual" type.
 *
 * The actual implementations are BucketDataDocumentV1 or BucketDataDocumentV3.
 * They don't fully satisfy this interface, but this works to share common implementations.
 *
 * The idea is that we can have a common implementation between V1 & V3, using this type,
 * and operate on MongoDB collections.
 *
 * This interface serves two primary purposes:
 * 1. Captures properties that exist on both V1 and V3 storage models.
 * 2. Gives a common reference when querying or modifying collections.
 *
 * Generics would've been ideal, but they don't play well with MongoDB collections.
 */
export interface BucketDataDocumentGeneric extends BucketDataProperties {
  _id: BucketDataDocumentGenericId;
}

/**
 * Represent read/write access for a single bucket.
 *
 * This does not implement the actual collection operations, but supports the required conversions
 * between in-memory BucketDataDoc and the specific storage formats.
 */
export interface SingleBucketStore {
  readonly key: BucketKey;

  readonly collection: mongo.Collection<BucketDataDocumentGeneric>;
  docId(o: InternalOpId): BucketDataDocumentGenericId;
  readonly minId: BucketDataDocumentGenericId;
  readonly maxId: BucketDataDocumentGenericId;

  /**
   * Convert in-memory document -> persisted document.
   */
  toPersistedDocument(source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric;

  /**
   * Convert persisted document -> in-memory document.
   */
  fromPersistedDocument(doc: BucketDataDocumentGeneric): BucketDataDoc;

  /**
   * Convert partial persisted document -> partial in-memory document.
   */
  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
}
