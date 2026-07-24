import {
  InternalOpId,
  PersistedDefinitionMapping,
  SerializedSyncPlan,
  SyncRuleState,
  deserializeParameterLookup as deserializeParameterLookupCore
} from '@powersync/service-core';
import {
  BucketDefinitionId,
  ParameterIndexId,
  ScopedParameterLookup,
  SqliteJsonValue
} from '@powersync/service-sync-rules';
import * as bson from 'bson';
import {
  BucketDataKey,
  BucketParameterDocumentBase,
  BucketStateDocumentBase,
  CurrentBucket,
  OpType,
  ReplicaId,
  SourceTableDocumentSnapshotStatus,
  SourceTableKey,
  SyncRuleCheckpointFields,
  SyncRuleDocumentBase,
  TaggedBucketParameterDocument
} from '../models.js';

/**
 * Embedded in sync_rules.sync_configs.
 */
export interface SyncRuleConfigStateV3 extends SyncRuleCheckpointFields {
  _id: bson.ObjectId;

  /**
   * If false, we cannot create any checkpoints.
   */
  snapshot_done: boolean;

  state: SyncRuleState;
}

/**
 * Represents the state of a replication stream, in the sync_rules collection.
 *
 * Differences from V1:
 * 1. The static config is moved into a separate sync_configs collection.
 * 2. The same replication stream may be shared by multiple sync config instances.
 */
export interface ReplicationStreamDocumentV3 extends SyncRuleDocumentBase {
  storage_version: number;

  /**
   * These contain the checkpoint/state per sync config.
   *
   * In common cases we'd have one active config or one active + one processing config,
   * but the model allows multiple configs in any state.
   */
  sync_configs: SyncRuleConfigStateV3[];

  /**
   * The monotonic head of the stream's op sequence: the highest op id persisted to bucket data,
   * whether or not yet covered by a checkpoint.
   *
   * This is shared across all sync configs of the stream (they share the global op sequence).
   * It is never cleared, only `$max`-advanced. A newly-appended config that replicates nothing
   * adopts this value as its checkpoint rather than starting at 0.
   *
   * Stored as a mongo Long, nullable.
   */
  last_persisted_op?: bigint | null;

  /**
   * The stream's replication position: all source changes up to this LSN have been processed,
   * and the resulting ops persisted. Replication resumes from here.
   *
   * Like {@link last_persisted_op}, this is shared across all sync configs of the stream -
   * per-config last_checkpoint_lsn values are consistency markers, not replication positions.
   *
   * Set via setResumeLsn() (snapshot start, and per-batch progress during streaming), and
   * advanced on every commit/keepalive - including checkpoint-blocked ones, since commit
   * flushes first and blocking only delays consistency markers, not data persistence.
   */
  resume_lsn?: string | null;
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

  rule_mapping: PersistedDefinitionMapping;
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

export function taggedBucketParameterDocumentToV3(document: TaggedBucketParameterDocument): BucketParameterDocumentV3 {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocumentV3;
}

export interface ReplicaIdColumn {
  name: string;
  type_oid?: number;
  type?: string;
}

export interface SourceTableDocumentV3 {
  _id: bson.ObjectId;
  connection_id: number;
  relation_id: number | string | undefined;
  schema_name: string;
  table_name: string;
  replica_id_columns: ReplicaIdColumn[];
  snapshot_done: boolean;
  snapshot_status: SourceTableDocumentSnapshotStatus | undefined;
  bucket_data_source_ids: BucketDefinitionId[];
  parameter_lookup_source_ids: ParameterIndexId[];
  latest_pending_delete?: InternalOpId | undefined;
}

export interface BucketStateDocumentV3 extends BucketStateDocumentBase {
  _id: BucketStateDocumentBase['_id'] & {
    d: BucketDefinitionId;
  };
}

export interface BucketOperation {
  o: bigint;
  op: OpType;
  source_table?: bson.ObjectId;
  source_key?: ReplicaId;
  table?: string;
  row_id?: string;
  checksum: bigint;
  data: string | null;
}

export interface StorageRef {
  path: string;
  compressed_size: number;
}

/** An S3 object that may be deleted once its grace period has elapsed. */
export interface ObjectStorageDeletionMarker {
  _id: bson.ObjectId;
  path: string;
  delete_after: Date;
}

/**
 * A non-empty chunk of a bucket's ordered operation stream.
 *
 * All writers maintain these invariants:
 *
 * - `ops` is non-empty and strictly ordered by `o`.
 * - `min_op` is `ops[0].o` and `_id.o` is `ops[ops.length - 1].o`.
 * - Documents for the same bucket have ordered, disjoint operation ranges,
 *   although gaps between ranges are allowed.
 * - `checksum` is the sum of every operation checksum and `count` is
 *   `ops.length`.
 *
 * Metadata-only checksum and data queries rely on these invariants to identify
 * boundary-straddling documents without reading `ops`.
 */
export interface BucketDataDocumentV3 {
  _id: BucketDataKey;
  min_op: bigint;
  checksum: bigint;
  count: number;
  size: number;
  /**
   * The greatest operation boundary that influenced this document's contents.
   * Bucket-data reads propagate it so serving an earlier checkpoint can be
   * invalidated when compaction changed the operations being returned.
   */
  target_op?: bigint | null;
  ops?: BucketOperation[];
  storage_ref?: StorageRef;
  /**
   * Present (and always true) when this document contains a CLEAR operation.
   * In that case, this is the first document in the bucket: all preceding
   * documents have been removed.
   */
  has_clear_op?: true;
}

export function serializeParameterLookup(lookup: ScopedParameterLookup): bson.Binary {
  return new bson.Binary(bson.serialize({ l: lookup.values.slice(2) }));
}

export function deserializeParameterLookup(lookup: bson.Binary, indexId: ParameterIndexId): SqliteJsonValue[] {
  return [indexId, '', ...deserializeParameterLookupCore(lookup)];
}

export function taggedBucketParameterDocumentToTagged(
  document: TaggedBucketParameterDocument
): BucketParameterDocumentV3 {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocumentV3;
}
