import { InternalOpId } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import {
  BucketParameterDocumentBase,
  BucketStateDocumentBase,
  CurrentBucket,
  ReplicaId,
  SourceTableDocument,
  SourceTableKey,
  TaggedBucketParameterDocument
} from '../models.js';
export {
  BucketDataDocumentV5,
  BucketDataKeyV5,
  BucketOperationV5,
  loadBucketDataDocumentV5,
  serializeBucketDataV5
} from '../document-formats/v5-format.js';

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
