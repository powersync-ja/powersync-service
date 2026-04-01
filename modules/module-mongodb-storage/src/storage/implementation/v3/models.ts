import { InternalOpId } from '@powersync/service-core';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import {
  BucketDataDocumentBase,
  BucketDataKey,
  BucketParameterDocumentBase,
  CurrentBucket,
  ReplicaId,
  SourceTableDocument,
  SourceTableKey,
  TaggedBucketDataDocument,
  TaggedBucketParameterDocument,
  BucketStateDocumentBase
} from '../common/models.js';
import * as bson from 'bson';

export interface CurrentBucketV3 extends CurrentBucket {
  def: BucketDefinitionId;
}

export interface RecordedLookupV3 {
  i: ParameterIndexId;
  l: bson.Binary;
}

export interface CurrentDataDocumentV3 {
  _id: ReplicaId;
  data: bson.Binary | null;
  buckets: CurrentBucketV3[];
  lookups: RecordedLookupV3[];
  /**
   * If set, this can be deleted, once there is a consistent checkpoint >= pending_delete.
   *
   * This must only be set if buckets = [], lookups = [].
   */
  pending_delete?: bigint;
}

export interface BucketParameterDocumentV3 extends BucketParameterDocumentBase<SourceTableKey> {}

export type BucketDataKeyV3 = BucketDataKey;

export interface BucketDataDocumentV3 extends BucketDataDocumentBase {
  _id: BucketDataKeyV3;
}

export function taggedBucketDataDocumentToV3(document: TaggedBucketDataDocument): BucketDataDocumentV3 {
  const { def: _definitionId, ...rest } = document;
  return rest;
}

export function taggedBucketParameterDocumentToV3(document: TaggedBucketParameterDocument): BucketParameterDocumentV3 {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocumentV3;
}

export interface SourceTableDocumentV3 extends SourceTableDocument {
  bucket_data_source_ids: BucketDefinitionId[];
  parameter_lookup_source_ids: ParameterIndexId[];
  latest_pending_delete?: InternalOpId | undefined;
}

export interface BucketStateDocumentV3 extends BucketStateDocumentBase {
  _id: BucketStateDocumentBase['_id'] & {
    d: BucketDefinitionId;
  };
}
