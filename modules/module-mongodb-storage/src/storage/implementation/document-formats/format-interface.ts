import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataDocumentGeneric } from '../common/SingleBucketStore.js';
import { BucketDataProperties } from '../models.js';

export interface BucketDataFormatAdapter {
  /**
   * Serialize in-memory BucketDataDoc[] into MongoDB bulk-write operations.
   * V3: one insert per doc.
   * V5: group by bucket, chunk, one insert per chunk.
   */
  serializeForBulkWrite(
    bucket: string,
    docs: BucketDataDoc[]
  ): mongo.AnyBulkWriteOperation<BucketDataDocumentGeneric>[];

  /**
   * Deserialize a raw MongoDB document into a generator of BucketDataDoc.
   * V3: yields exactly one.
   * V5: yields each op in ops[].
   */
  loadDocument(
    context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
    rawDoc: unknown
  ): Generator<BucketDataDoc>;

  /**
   * Convert a single in-memory op into a persisted document for SingleBucketStore.
   * V3: returns the document directly.
   * V5: wraps in ops[] array.
   */
  toPersistedDocument(bucketKey: BucketKey, source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric;

  /**
   * Convert a persisted document back to a single in-memory op.
   * Used by SingleBucketStore when we know the document represents exactly one op.
   */
  fromPersistedDocument(bucketKey: BucketKey, doc: BucketDataDocumentGeneric): BucketDataDoc;

  /**
   * Convert a partial persisted document (e.g., from aggregation pipeline) to partial in-memory.
   */
  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    bucketKey: BucketKey,
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;

  /**
   * Build the MongoDB query for fetching bucket data in a range.
   * V3: uses limit on cursor.
   * V5: no limit, reads all matching documents.
   */
  buildBucketDataQuery(options: { startOpId?: InternalOpId; endOpId: InternalOpId; remainingLimit: number }): {
    filter: mongo.Filter<BucketDataDocumentGeneric>;
    cursorOptions: { limit?: number; batchSize?: number };
  };
}
