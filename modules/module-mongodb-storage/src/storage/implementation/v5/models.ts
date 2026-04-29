import { InternalOpId } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentBase,
  BucketDataKey,
  BucketParameterDocumentBase,
  BucketStateDocumentBase,
  CurrentBucket,
  ReplicaId,
  SourceTableDocument,
  SourceTableKey,
  TaggedBucketParameterDocument
} from '../models.js';

export interface CurrentBucketV5 extends CurrentBucket {
  def: BucketDefinitionId;
}

export interface RecordedLookupV5 {
  i: ParameterIndexId;
  l: bson.Binary;
}

export interface CurrentDataDocumentV5 {
  _id: ReplicaId;
  data: bson.Binary | null;
  buckets: CurrentBucketV5[];
  lookups: RecordedLookupV5[];
  /**
   * If set, this can be deleted, once there is a consistent checkpoint >= pending_delete.
   *
   * This must only be set if buckets = [], lookups = [].
   */
  pending_delete?: bigint;
}

export interface BucketParameterDocumentV5 extends BucketParameterDocumentBase<SourceTableKey> {}

export type BucketDataKeyV5 = BucketDataKey;

export interface BucketDataDocumentV5 extends BucketDataDocumentBase {
  _id: BucketDataKeyV5;
}

export function serializeBucketDataV5(document: BucketDataDoc): BucketDataDocumentV5 {
  const { bucketKey, o } = document;
  return {
    _id: {
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

export function loadBucketDataDocumentV5(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV5
): BucketDataDoc {
  const { _id, ...rest } = doc;
  return {
    bucketKey: {
      ...context,
      bucket: _id.b
    },
    o: _id.o,
    ...rest
  };
}

export function taggedBucketParameterDocumentToV5(document: TaggedBucketParameterDocument): BucketParameterDocumentV5 {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocumentV5;
}

export interface SourceTableDocumentV5 extends SourceTableDocument {
  bucket_data_source_ids: BucketDefinitionId[];
  parameter_lookup_source_ids: ParameterIndexId[];
  latest_pending_delete?: InternalOpId | undefined;
}

export interface BucketStateDocumentV5 extends BucketStateDocumentBase {
  _id: BucketStateDocumentBase['_id'] & {
    d: BucketDefinitionId;
  };
}
