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

export function taggedBucketDataDocumentToV1(document: BucketDataDoc): BucketDataDocumentV1 {
  const { bucketKey, o, ...rest } = document;
  return {
    _id: {
      g: bucketKey.replicationStreamId,
      b: bucketKey.bucket,
      o: o
    },
    ...rest
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
