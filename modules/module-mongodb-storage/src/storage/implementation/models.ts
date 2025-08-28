import { InternalOpId, storage } from '@powersync/service-core';
import { SqliteJsonValue } from '@powersync/service-sync-rules';
import * as bson from 'bson';

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

export interface BucketDataKey {
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

export interface CurrentBucket {
  bucket: string;
  table: string;
  id: string;
}

export interface BucketParameterDocument {
  _id: bigint;
  key: SourceKey;
  lookup: bson.Binary;
  bucket_parameters: Record<string, SqliteJsonValue>[];
}

export interface BucketDataDocument {
  _id: BucketDataKey;
  op: OpType;
  source_table?: bson.ObjectId;
  source_key?: ReplicaId;
  table?: string;
  row_id?: string;
  checksum: bigint;
  data: string | null;
  target_op?: bigint | null;
}

export type OpType = 'PUT' | 'REMOVE' | 'MOVE' | 'CLEAR';

export interface SourceTableDocument {
  _id: bson.ObjectId;
  group_id: number;
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
 * Right now, this is just used to track when buckets are updated, for efficient incremental sync.
 * In the future, this could be used to track operation counts, both for diagnostic purposes, and for
 * determining when a compact and/or defragment could be beneficial.
 *
 * Note: There is currently no migration to populate this collection from existing data - it is only
 * populated by new updates.
 */
export interface BucketStateDocument {
  _id: {
    g: number;
    b: string;
  };
  last_op: bigint;
  /**
   * If set, this can be treated as "cache" of a checksum at a specific point.
   * Can be updated periodically, for example by the compact job.
   */
  compacted_state?: {
    op_id: InternalOpId;
    count: number;
    checksum: bigint;
    bytes: number;
  };

  estimate_since_compact?: {
    count: number;
    bytes: number;
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

  content: string;
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
