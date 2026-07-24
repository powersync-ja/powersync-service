import { ObserverClient } from '@powersync/lib-services-framework';
import { EvaluatedParameters, EvaluatedRow, SqliteRow, ToastableSqliteRow } from '@powersync/service-sync-rules';
import { BSON } from 'bson';
import { InternalOpId } from '../util/utils.js';
import { ReplicationEventPayload } from './ReplicationEventPayload.js';
import { SourceTable, TableSnapshotStatus } from './SourceTable.js';
import { BatchedCustomWriteCheckpointOptions } from './storage-index.js';
import { ResolveTablesOptions, ResolveTablesResult } from './SyncRulesBucketStorage.js';

export const DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS: ResolvedBucketBatchCommitOptions = {
  createEmptyCheckpoints: true,
  oldestUncommittedChange: null
};

/**
 * Storage writer for a source-specific unit of replication work.
 *
 * A batch evaluates source row changes against the hydrated sync config,
 * persists bucket operations and parameter lookup rows, tracks source table and
 * snapshot state, records the committed source position for visible
 * checkpoints, and stores restart cursors for sources with PowerSync-managed
 * resume state.
 */
export interface BucketStorageBatch extends ObserverClient<BucketBatchStorageListener>, AsyncDisposable {
  /**
   * Alias for [Symbol.asyncDispose]
   */
  dispose(): Promise<void>;

  /**
   * Last written op, if any. This may not reflect a consistent checkpoint.
   */
  last_flushed_op: InternalOpId | null;

  /**
   * True for snapshot batches that should skip rows already present in current_data.
   */
  readonly skipExistingRows: boolean;

  /**
   * Queue one normalized source row operation and potentially flush.
   *
   * When flushed, the operation is evaluated against data and parameter
   * queries. That may produce bucket operations, parameter lookup rows, and
   * current-data updates. `save()` may trigger that flush automatically when
   * the batch reaches its size limits, but a flush does not create a checkpoint
   * by itself.
   */
  save(record: SaveOptions): Promise<FlushedResult | null>;

  /**
   * Replicate a truncate operation by removing all currently replicated rows
   * for the specified source tables.
   */
  truncate(sourceTables: SourceTable[]): Promise<FlushedResult | null>;

  /**
   * Drop one or more source table mappings.
   *
   * This performs the same logical row removal as `truncate()`, then removes
   * the stored `SourceTable` record. It is used for table drops, renames,
   * replica identity changes, and superseded mappings.
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

  /**
   * Flush saved ops and advance the committed source checkpoint position.
   *
   * Only call this at a source transaction, page, snapshot, or other boundary
   * where it is valid for clients to observe all flushed changes together. A
   * single commit may include multiple complete source transactions, but must
   * not split one source transaction across checkpoint boundaries.
   */
  commit(lsn: string, options?: BucketBatchCommitOptions): Promise<CheckpointResult>;

  /**
   * Advance the committed source checkpoint position without associated row
   * operations.
   *
   * Use this for source heartbeats, marker-only events, and idle-source write
   * checkpoint progress. This must only be called outside a source transaction.
   */
  keepalive(lsn: string): Promise<CheckpointResult>;

  /**
   * Set the source position that replication should resume from after restart.
   *
   * This can be used for:
   * 1. Setting the LSN for a snapshot, before starting replication.
   * 2. Setting the LSN to resume from after a replication restart, without advancing the checkpoint LSN via a commit.
   *
   * Not required if the source database keeps track of this, for example with
   * PostgreSQL logical replication slots.
   *
   * This is for restart safety only. If the same source position should become
   * visible to clients, also call `commit()` or `keepalive()` at a valid source
   * boundary.
   */
  setResumeLsn(lsn: string): Promise<void>;

  /**
   * LSN to resume from.
   *
   * Not relevant for streams where the source keeps track of replication progress, such as Postgres.
   */
  resumeFromLsn: string | null;

  /**
   * Mark table snapshots complete and record the source position that
   * checkpoints must not precede for those tables.
   */
  markTableSnapshotDone(tables: SourceTable[], no_checkpoint_before_lsn?: string): Promise<SourceTable[]>;

  /**
   * Mark a source table as requiring snapshot work again.
   *
   * Use this when a table that was previously considered snapshot-complete
   * must be copied again before it can safely contribute to checkpoints. Common
   * cases include detected schema or replica identity changes, interrupted
   * re-snapshot work, or other source metadata changes that make the existing
   * replicated rows no longer trustworthy.
   */
  markTableSnapshotRequired(table: SourceTable): Promise<void>;

  /**
   * Mark the full replication snapshot as done without validating individual source table snapshot state.
   *
   * This is primarily intended for storage tests and setup helpers that manually construct storage state.
   * Replicators should use `markSnapshotDone()` instead, because that validates that all known source tables
   * have completed snapshotting before allowing checkpoints to be unblocked.
   */
  markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void>;

  /**
   * Mark the full replication snapshot as done after validating that all known source tables have completed snapshotting.
   *
   * Replicators should use this method when completing an initial snapshot. The validation prevents races where
   * new source tables are marked as requiring a snapshot while global snapshot finalization is running.
   *
   * If `throwOnConflict` is false, this will instead return early without throwing if there are source tables that still require snapshotting.
   * Use that only in cases where concurrency is expected, and can automatically retry/continue.
   */
  markSnapshotDone(no_checkpoint_before_lsn: string, options?: { throwOnConflict?: boolean }): Promise<void>;

  /**
   * Persist resumable table snapshot progress such as row counts or source
   * cursors.
   */
  updateTableProgress(table: SourceTable, progress: Partial<TableSnapshotStatus>): Promise<SourceTable>;

  /**
   * Get the current status for an existing source table without creating or resolving a replacement table.
   */
  getSourceTableStatus(table: SourceTable): Promise<SourceTable | null>;

  /**
   * Resolve discovered source metadata against the active sync config and
   * persisted source table state.
   *
   * @returns Current mappings that should receive replicated rows, plus
   *          outdated mappings that the source connector should drop.
   */
  resolveTables(options: ResolveTablesOptions): Promise<ResolveTablesResult>;

  /**
   * Queue a custom write checkpoint to be persisted after operations are flushed.
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

export interface CheckpointResult {
  /**
   * True if any of these are true:
   * 1. A snapshot is in progress.
   * 2. The last checkpoint is older than "no_checkpoint_before" (if provided).
   * 3. Replication was restarted with a lower LSN, and has not caught up yet.
   */
  checkpointBlocked: boolean;

  /**
   * True if a checkpoint was actually created by this operation. This can be false even if checkpointBlocked is false,
   * if the checkpoint was empty.
   */
  checkpointCreated: boolean;
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
