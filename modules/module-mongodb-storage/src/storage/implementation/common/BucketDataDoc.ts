import { InternalOpId } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketDataProperties } from '../models.js';

/**
 * Full context identifying a bucket.
 */
export interface BucketKey {
  /**
   * Also referred to as g / group_id.
   */
  replicationStreamId: number;
  /**
   * Bucket definition id, '0' for storage V1.
   */
  definitionId: BucketDefinitionId;
  /**
   * Bucket name.
   */
  bucket: string;
}

/**
 * In-memory bucket data document.
 *
 * This is converted to/from BucketDataDocumentV1 / BucketDataDocumentV3 for storage.
 */
export interface BucketDataDoc extends BucketDataProperties {
  /**
   * Identifies the bucket for this document.
   */
  bucketKey: BucketKey;
  /**
   * op_id
   */
  o: InternalOpId;
}
