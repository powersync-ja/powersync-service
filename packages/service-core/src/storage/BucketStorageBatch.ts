import { ObserverClient } from '@powersync/lib-services-framework';
import { EvaluatedParameters, EvaluatedRow, SqliteRow, ToastableSqliteRow } from '@powersync/service-sync-rules';
import { BSON } from 'bson';
import { ReplicationEventPayload } from './ReplicationEventPayload.js';
import { SourceTable, TableSnapshotStatus } from './SourceTable.js';
import { BatchedCustomWriteCheckpointOptions } from './storage-index.js';
import { InternalOpId } from '../util/utils.js';

export const DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS: ResolvedBucketBatchCommitOptions = {
  createEmptyCheckpoints: true,
  oldestUncommittedChange: null
};

export interface BucketDataWriter {
  /**
   * Save an op, and potentially flush.
   *
   * This can be an insert, update or delete op.
   */
  save(record: SaveOptions): Promise<FlushedResult | null>;

  /**
   * Replicate a truncate op - deletes all data in the specified tables.
   */
  truncate(sourceTables: SourceTable[]): Promise<FlushedResult | null>;

  /**
   * Drop one or more tables.
   *
   * This is the same as truncate, but additionally removes the SourceTable record.
   */
  drop(sourceTables: SourceTable[]): Promise<FlushedResult | null>;

  /**
   * Explicitly flush all pending changes in the batch.
   *
   * This does not create a new checkpoint until `commit()` is called. This means it's
   * safe to flush multiple times in the middle of a large transaction.
   *
   * @returns null if there are no changes to flush.
   */
  flush(options?: BatchBucketFlushOptions): Promise<FlushedResult | null>;
}

export interface BucketStorageBatch
  extends ObserverClient<BucketBatchStorageListener>,
    AsyncDisposable,
    BucketDataWriter {
  /**
   * Alias for [Symbol.asyncDispose]
   */
  dispose(): Promise<void>;

  /**
   * Flush and commit any saved ops. This creates a new checkpoint by default.
   *
   * Only call this after a transaction.
   *
   * Returns true if either (1) a new checkpoint was created, or (2) there are no changes to commit.
   */
  commit(lsn: string, options?: BucketBatchCommitOptions): Promise<boolean>;

  /**
   * Advance the checkpoint LSN position, without any associated op.
   *
   * This must only be called when not inside a transaction.
   *
   * @returns true if the checkpoint was advanced, false if this was a no-op
   */
  keepalive(lsn: string): Promise<boolean>;

  /**
   * Set the LSN that replication should resume from.
   *
   * This can be used for:
   * 1. Setting the LSN for a snapshot, before starting replication.
   * 2. Setting the LSN to resume from after a replication restart, without advancing the checkpoint LSN via a commit.
   *
   * Not required if the source database keeps track of this, for example with
   * PostgreSQL logical replication slots.
   */
  setResumeLsn(lsn: string): Promise<void>;

  /**
   * Get the last checkpoint LSN, from either commit or keepalive.
   */
  lastCheckpointLsn: string | null;

  /**
   * LSN to resume from.
   *
   * Not relevant for streams where the source keeps track of replication progress, such as Postgres.
   */
  resumeFromLsn: string | null;

  markTableSnapshotDone(tables: SourceTable[], no_checkpoint_before_lsn?: string): Promise<SourceTable[]>;
  markTableSnapshotRequired(table: SourceTable): Promise<void>;
  markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void>;

  updateTableProgress(table: SourceTable, progress: Partial<TableSnapshotStatus>): Promise<SourceTable>;

  /**
   * Queues the creation of a custom Write Checkpoint. This will be persisted after operations are flushed.
   */
  addCustomWriteCheckpoint(checkpoint: BatchedCustomWriteCheckpointOptions): void;
}

/**
 * Replica id uniquely identifying a row on the source database.
 *
 * Can be any value serializable to BSON.
 *
 * If the value is an entire document, the data serialized to a v5 UUID may be a good choice here.
 */
export type ReplicaId = BSON.UUID | BSON.Document | any;

export interface SaveParameterData {
  sourceTable: SourceTable;
  /** UUID */
  sourceKey: string;
  evaluated: EvaluatedParameters[];
}

export interface SaveBucketData {
  sourceTable: SourceTable;
  /** UUID */
  sourceKey: string;

  evaluated: EvaluatedRow[];
}

export type SaveOp = 'insert' | 'update' | 'delete';

export type SaveOptions = SaveInsert | SaveUpdate | SaveDelete;

export enum SaveOperationTag {
  INSERT = 'insert',
  UPDATE = 'update',
  DELETE = 'delete'
}

export interface SaveInsert {
  tag: SaveOperationTag.INSERT;
  sourceTable: SourceTable;
  before?: undefined;
  beforeReplicaId?: undefined;
  after: SqliteRow;
  afterReplicaId: ReplicaId;
}

export interface SaveUpdate {
  tag: SaveOperationTag.UPDATE;
  sourceTable: SourceTable;

  /**
   * This is only present when the id has changed, and will only contain replica identity columns.
   */
  before?: SqliteRow;
  beforeReplicaId?: ReplicaId;

  /**
   * A null value means null column.
   *
   * An undefined value means it's a TOAST value - must be copied from another record.
   */
  after: ToastableSqliteRow;
  afterReplicaId: ReplicaId;
}

export interface SaveDelete {
  tag: SaveOperationTag.DELETE;
  sourceTable: SourceTable;
  before?: SqliteRow;
  beforeReplicaId: ReplicaId;
  after?: undefined;
  afterReplicaId?: undefined;
}

export interface BucketBatchStorageListener {
  replicationEvent: (payload: ReplicationEventPayload) => void;
}

export interface FlushedResult {
  flushed_op: InternalOpId;
}

export interface BatchBucketFlushOptions {
  /**
   * The timestamp of the first change in this batch, according to the source database.
   *
   * Used to estimate replication lag.
   */
  oldestUncommittedChange?: Date | null;
}

export interface BucketBatchCommitOptions extends BatchBucketFlushOptions {
  /**
   * Creates a new checkpoint even if there were no persisted operations.
   * Defaults to true.
   */
  createEmptyCheckpoints?: boolean;
}

export type ResolvedBucketBatchCommitOptions = Required<BucketBatchCommitOptions>;
