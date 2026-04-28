import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { InternalOpId, SerializedSyncPlan, storage } from '@powersync/service-core';
import { SqliteJsonValue } from '@powersync/service-sync-rules';
import { event_types } from '@powersync/service-types';
import * as bson from 'bson';
import { ParameterIndexId } from './BucketDefinitionMapping.js';
import type { CurrentDataDocument, SourceTableDocumentV1 } from './v1/models.js';
import type { CurrentBucketV3, CurrentDataDocumentV3, RecordedLookupV3, SourceTableDocumentV3 } from './v3/models.js';

/**
 * Replica id uniquely identifying a row on the source database.
 *
 * Can be any value serializable to BSON.
 *
 * If the value is an entire document, the data serialized to a v5 UUID may be a good choice here.
 */
export type ReplicaId = bson.UUID | bson.Document | any;

export interface SourceKey {
  /** group_id */
  g: number;
  /** source table id */
  t: bson.ObjectId;
  /** source key */
  k: ReplicaId;
}

export interface SourceTableKey {
  /** source table id */
  t: bson.ObjectId;
  /** source key */
  k: ReplicaId;
}

export interface BucketDataKey {
  /** bucket name */
  b: string;
  /** op_id */
  o: bigint;
}

export interface CurrentBucket {
  bucket: string;
  table: string;
  id: string;
}

export interface BucketParameterDocumentBase<TKey> {
  _id: bigint;
  key: TKey;
  lookup: bson.Binary;
  bucket_parameters: Record<string, SqliteJsonValue>[];
}

export interface TaggedBucketParameterDocument extends BucketParameterDocumentBase<SourceKey | SourceTableKey> {
  index: ParameterIndexId;
}

export function bucketParameterDocumentToTagged<TKey extends SourceKey | SourceTableKey>(
  document: BucketParameterDocumentBase<TKey>,
  index: ParameterIndexId
): TaggedBucketParameterDocument {
  return {
    ...document,
    index
  };
}

export type OpType = 'PUT' | 'REMOVE' | 'MOVE' | 'CLEAR';

/**
 * Common properties for storage V1, storage V3 and in-memory BucketDataDoc.
 */
export interface BucketDataProperties {
  op: OpType;
  source_table?: bson.ObjectId;
  source_key?: ReplicaId;
  table?: string;
  row_id?: string;
  checksum: bigint;
  data: string | null;
  target_op?: bigint | null;
}

export interface BucketDataDocumentBase extends BucketDataProperties {
  _id: { b: string };
}

/**
 * Internal-only tag used for v1 bucket_data rows before they are converted to the v1 on-disk shape.
 */
export const LEGACY_BUCKET_DATA_DEFINITION_ID = '0';

/**
 * Internal-only tag used for v1 bucket_parameters rows before they are converted to the v1 on-disk shape.
 */
export const LEGACY_BUCKET_PARAMETER_INDEX_ID = '0';

export interface SourceTableDocument {
  _id: bson.ObjectId;
  connection_id: number;
  relation_id: number | string | undefined;
  schema_name: string;
  table_name: string;
  replica_id_columns: string[] | null;
  replica_id_columns2: { name: string; type_oid?: number; type?: string }[] | undefined;
  snapshot_done: boolean | undefined;
  snapshot_status: SourceTableDocumentSnapshotStatus | undefined;
}

export interface SourceTableDocumentSnapshotStatus {
  total_estimated_count: number;
  replicated_count: number;
  last_key: bson.Binary | null;
}

/**
 * Record the state of each bucket.
 *
 * The primary use case is to track when buckets are updated, for efficient incremental sync.
 *
 * The secondary use case is to track operation counts to determine whether or not a bucket should be compacted.
 *
 * Note: For storage V1, there is no migration to populate this collection from existing data - it is only
 * populated by new updates.
 *
 * For storage V3, these will always be present.
 */
export interface BucketStateDocumentBase {
  _id: {
    b: string;
  };
  /**
   * Important: There is an unique index on last_op per logical stream.
   * That means the last_op must match an actual op in the bucket, and not the commit checkpoint.
   */
  last_op: bigint;
  /**
   * If set, this can be treated as "cache" of a checksum at a specific point.
   * Can be updated periodically, for example by the compact job.
   */
  compacted_state?: {
    op_id: InternalOpId;
    count: number;
    checksum: bigint;
    bytes: number | bigint | null;
  };

  estimate_since_compact?: {
    count: number;
    bytes: number | bigint;
  };
}

export interface IdSequenceDocument {
  _id: string;
  op_id: bigint;
}

export interface SyncRuleDocument {
  _id: number;

  state: storage.SyncRuleState;

  /**
   * True if initial snapshot has been replicated.
   *
   * Can only be false if state == PROCESSING.
   */
  snapshot_done: boolean;

  /**
   * This is now used for "resumeLsn".
   *
   * If snapshot_done = false, this may be the lsn at which we started the snapshot.
   *
   * This can be used for resuming the snapshot after a restart.
   *
   * If snapshot_done is true, this is treated as the point to restart replication from.
   *
   * More specifically, we resume replication from max(snapshot_lsn, last_checkpoint_lsn).
   */
  snapshot_lsn: string | undefined;

  /**
   * The last consistent checkpoint.
   *
   * There may be higher OpIds used in the database if we're in the middle of replicating a large transaction.
   */
  last_checkpoint: bigint | null;

  /**
   * The LSN associated with the last consistent checkpoint.
   */
  last_checkpoint_lsn: string | null;

  /**
   * If set, no new checkpoints may be created < this value.
   */
  no_checkpoint_before: string | null;

  /**
   * Goes together with no_checkpoint_before.
   *
   * If a keepalive is triggered that creates the checkpoint > no_checkpoint_before,
   * then the checkpoint must be equal to this keepalive_op.
   */
  keepalive_op: string | null;

  slot_name: string | null;

  /**
   * Last time we persisted a checkpoint.
   *
   * This may be old if no data is incoming.
   */
  last_checkpoint_ts: Date | null;

  /**
   * Last time we persisted a checkpoint or keepalive.
   *
   * This should stay fairly current while replicating.
   */
  last_keepalive_ts: Date | null;

  /**
   * If an error is stopping replication, it will be stored here.
   */
  last_fatal_error: string | null;

  last_fatal_error_ts: Date | null;

  content: string;
  serialized_plan?: SerializedSyncPlan | null;

  lock?: {
    id: string;
    expires_at: Date;
  } | null;

  storage_version?: number;
}

export interface StorageConfig extends storage.StorageVersionConfig {
  /**
   * When true, bucket_data.checksum is guaranteed to be persisted as a Long.
   *
   * When false, it could also have been persisted as an Int32 or Double, in which case it must be converted to
   * a Long before summing.
   */
  longChecksums: boolean;
  /**
   * Enables v3 MongoDB storage behavior used for incremental reprocessing.
   */
  incrementalReprocessing: boolean;
}

const LONG_CHECKSUMS_STORAGE_VERSION = 2;
const INCREMENTAL_REPROCESSING_STORAGE_VERSION = storage.STORAGE_VERSION_3;

export function getMongoStorageConfig(storageVersion: number): StorageConfig {
  const baseConfig = storage.STORAGE_VERSION_CONFIG[storageVersion];
  if (baseConfig == null) {
    throw new ServiceError(ErrorCode.PSYNC_S1005, `Unsupported storage version ${storageVersion}`);
  }

  return {
    ...baseConfig,
    longChecksums: storageVersion >= LONG_CHECKSUMS_STORAGE_VERSION,
    incrementalReprocessing: storageVersion >= INCREMENTAL_REPROCESSING_STORAGE_VERSION
  };
}

export interface CheckpointEventDocument {
  _id: bson.ObjectId;
}

export type SyncRuleCheckpointState = Pick<
  SyncRuleDocument,
  'last_checkpoint' | 'last_checkpoint_lsn' | '_id' | 'state'
>;

export interface CustomWriteCheckpointDocument {
  _id: bson.ObjectId;
  user_id: string;
  checkpoint: bigint;
  sync_rules_id: number;
  /**
   * Unlike managed write checkpoints, custom write checkpoints are flushed together with
   * normal ops. This means we can assign an op_id for ordering / correlating with read checkpoints.
   *
   * This is not unique - multiple write checkpoints can have the same op_id.
   */
  op_id?: InternalOpId;
}

export interface WriteCheckpointDocument {
  _id: bson.ObjectId;
  user_id: string;
  lsns: Record<string, string>;
  client_id: bigint;
  /**
   * This is set to the checkpoint lsn when the checkpoint lsn >= this lsn.
   * This is used to make it easier to determine what write checkpoints have been processed
   * between two checkpoints.
   */
  processed_at_lsn: string | null;
}

export interface InstanceDocument {
  // The instance UUID
  _id: string;
}

export interface ClientConnectionDocument extends event_types.ClientConnection {}

export type CurrentDataDocumentId = CurrentDataDocument['_id'] | CurrentDataDocumentV3['_id'];
export type CommonCurrentBucket = CurrentBucket | CurrentBucketV3;
export type CommonCurrentLookup = bson.Binary | RecordedLookupV3;
export type CommonSourceTableDocument = SourceTableDocumentV1 | SourceTableDocumentV3;
