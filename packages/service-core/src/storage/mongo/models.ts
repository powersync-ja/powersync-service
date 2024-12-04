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
  checksum: number;
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
}

export interface IdSequenceDocument {
  _id: string;
  op_id: bigint;
}

export enum SyncRuleState {
  /**
   * New sync rules - needs to be processed (initial replication).
   *
   * While multiple sets of sync rules _can_ be in PROCESSING,
   * it's generally pointless, so we only keep one in that state.
   */
  PROCESSING = 'PROCESSING',

  /**
   * Sync rule processing is done, and can be used for sync.
   *
   * Only one set of sync rules should be in ACTIVE state.
   */
  ACTIVE = 'ACTIVE',
  /**
   * This state is used when the sync rules has been replaced,
   * and replication is or should be stopped.
   */
  STOP = 'STOP',
  /**
   * After sync rules have been stopped, the data needs to be
   * deleted. Once deleted, the state is TERMINATED.
   */
  TERMINATED = 'TERMINATED'
}

export interface SyncRuleDocument {
  _id: number;

  state: SyncRuleState;

  /**
   * True if initial snapshot has been replicated.
   *
   * Can only be false if state == PROCESSING.
   */
  snapshot_done: boolean;

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

export interface CustomWriteCheckpointDocument {
  _id: bson.ObjectId;
  user_id: string;
  checkpoint: bigint;
  sync_rules_id: number;
}

export interface WriteCheckpointDocument {
  _id: bson.ObjectId;
  user_id: string;
  lsns: Record<string, string>;
  client_id: bigint;
}

export interface InstanceDocument {
  // The instance UUID
  _id: string;
}
