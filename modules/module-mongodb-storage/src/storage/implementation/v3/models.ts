import { InternalOpId, SerializedSyncPlan, SyncRuleState } from '@powersync/service-core';
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
  SyncRuleCheckpointFields,
  SyncRuleDocumentBase,
  TaggedBucketParameterDocument
} from '../models.js';

export interface SyncRuleConfigStateV3 extends SyncRuleCheckpointFields<bigint | null> {
  _id: bson.ObjectId;

  /**
   * If false, we cannot create any checkpoints.
   */
  snapshot_done: boolean;

  state: SyncRuleState;
}

export interface SyncRuleDocumentV3 extends SyncRuleDocumentBase {
  storage_version: number;

  /**
   * These contain the checkpoint/state per sync config.
   *
   * In common cases we'd have one active config or one active + one processing config,
   * but the model allows multiple configs in any state.
   */
  sync_configs: SyncRuleConfigStateV3[];
}

/**
 * Static sync config definition.
 *
 * This should be treated as immutable - we don't update this after initial creation.
 */
export interface SyncConfigDefinition {
  _id: bson.ObjectId;
  created_at: Date;
  storage_version: number;
  /**
   * The related SyncRuleDocumentV3.
   *
   * Note that a specific sync config definition never moves between replication streams. Instead, we can create a new copy for the new replication stream.
   *
   * When terminating a specific sync config definition, we remove the reference from replication stream -> sync config, but this reference here remains as a historical record.
   */
  replication_stream_id: number;

  content: string;
  serialized_plan?: SerializedSyncPlan | null;

  rule_mapping: {
    /**
     * Map of uniqueName -> id, unique per replication stream.
     */
    definitions: Record<string, string>;
    /**
     * Map of (lookupName, queryId) -> id, unique per replication stream.
     */
    parameter_indexes: Record<string, string>;
  };
}

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

export function serializeBucketDataV3(document: BucketDataDoc): BucketDataDocumentV3 {
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

export function loadBucketDataDocumentV3(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV3
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
