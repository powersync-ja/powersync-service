import { InternalOpId } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import {
  CurrentBucket as BaseCurrentBucket,
  SourceTableDocument as BaseSourceTableDocument,
  BucketParameterDocumentBase,
  ReplicaId,
  SourceTableKey,
  TaggedBucketParameterDocument
} from '../models.js';

export interface CurrentBucket extends BaseCurrentBucket {
  def: BucketDefinitionId;
}

export interface RecordedLookup {
  i: ParameterIndexId;
  l: bson.Binary;
}

export interface CurrentDataDocument {
  _id: ReplicaId;
  data: bson.Binary | null;
  buckets: CurrentBucket[];
  lookups: RecordedLookup[];
  /**
   * If set, this can be deleted, once there is a consistent checkpoint >= pending_delete.
   *
   * This must only be set if buckets = [], lookups = [].
   */
  pending_delete?: bigint;
}

export interface BucketParameterDocument extends BucketParameterDocumentBase<SourceTableKey> {}

export function taggedBucketParameterDocumentToTagged(
  document: TaggedBucketParameterDocument
): BucketParameterDocument {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocument;
}

export interface SourceTableDocument extends BaseSourceTableDocument {
  bucket_data_source_ids: BucketDefinitionId[];
  parameter_lookup_source_ids: ParameterIndexId[];
  latest_pending_delete?: InternalOpId | undefined;
}
