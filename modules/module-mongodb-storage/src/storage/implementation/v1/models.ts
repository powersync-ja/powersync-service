import * as bson from 'bson';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentBase,
  BucketParameterDocumentBase,
  BucketStateDocumentBase,
  CurrentBucket,
  LEGACY_BUCKET_DATA_DEFINITION_ID,
  SourceKey,
  SourceTableDocument,
  TaggedBucketParameterDocument
} from '../models.js';

export interface BucketDataKeyV1 {
  /** group_id */
  g: number;
  /** bucket name */
  b: string;
  /** op_id */
  o: bigint;
}

export interface CurrentDataDocument {
  _id: SourceKey;
  data: bson.Binary;
  buckets: CurrentBucket[];
  lookups: bson.Binary[];
}

export interface BucketParameterDocument extends BucketParameterDocumentBase<SourceKey> {}

export interface BucketDataDocumentV1 extends BucketDataDocumentBase {
  _id: BucketDataKeyV1;
}

export function serializeBucketDataV1(document: BucketDataDoc): BucketDataDocumentV1 {
  const { bucketKey, o } = document;
  return {
    _id: {
      g: bucketKey.replicationStreamId,
      b: bucketKey.bucket,
      o: o
    },
    // List fields directly, so that we don't accidentally persist any unknown fields
    op: document.op,
    source_table: document.source_table,
    source_key: document.source_key,
    table: document.table,
    row_id: document.row_id,
    checksum: document.checksum,
    data: document.data,
    target_op: document.target_op
  };
}

export function loadBucketDataDocumentV1(doc: BucketDataDocumentV1): BucketDataDoc {
  const { _id, ...rest } = doc;
  return {
    bucketKey: {
      replicationStreamId: _id.g,
      definitionId: LEGACY_BUCKET_DATA_DEFINITION_ID,
      bucket: _id.b
    },
    o: _id.o,
    ...rest
  };
}

export function taggedBucketParameterDocumentToV1(document: TaggedBucketParameterDocument): BucketParameterDocument {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocument;
}

export interface SourceTableDocumentV1 extends SourceTableDocument {
  group_id: number;
}

export interface BucketStateDocumentV1 extends BucketStateDocumentBase {
  _id: BucketStateDocumentBase['_id'] & {
    g: number;
  };
}

export type BucketStateDocument = BucketStateDocumentV1;
