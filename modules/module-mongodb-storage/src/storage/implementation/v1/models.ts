import {
  BucketParameterDocumentBase,
  BucketDataDocumentBase,
  BucketStateDocumentBase,
  SourceKey,
  SourceTableDocument,
  TaggedBucketDataDocument,
  TaggedBucketParameterDocument
} from '../models.js';
import * as bson from 'bson';

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
  buckets: import('../models.js').CurrentBucket[];
  lookups: bson.Binary[];
}

export interface BucketParameterDocument extends BucketParameterDocumentBase<SourceKey> {}

export interface BucketDataDocumentV1 extends BucketDataDocumentBase {
  _id: BucketDataKeyV1;
}

export function taggedBucketDataDocumentToV1(
  groupId: number,
  document: TaggedBucketDataDocument
): BucketDataDocumentV1 {
  const { def: _definitionId, _id: _id, ...rest } = document;
  return {
    _id: {
      g: groupId,
      b: _id.b,
      o: _id.o
    },
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
