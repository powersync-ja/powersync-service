import {
  EvaluatedParameters,
  EvaluatedRow,
  SqlSyncRules,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow,
  ToastableSqliteRow
} from '@powersync/service-sync-rules';
import * as util from '../util/util-index.js';
import { SourceTable } from './SourceTable.js';
import { SourceEntityDescriptor } from './SourceEntity.js';
import * as bson from 'bson';

export interface BucketStorageFactory {
  /**
   * Update sync rules from configuration, if changed.
   */
  configureSyncRules(
    sync_rules: string,
    options?: { lock?: boolean }
  ): Promise<{ updated: boolean; persisted_sync_rules?: PersistedSyncRulesContent; lock?: ReplicationLock }>;

  /**
   * Get a storage instance to query sync data for specific sync rules.
   */
  getInstance(options: PersistedSyncRulesContent): SyncRulesBucketStorage;

  /**
   * Deploy new sync rules.
   */
  updateSyncRules(options: UpdateSyncRulesOptions): Promise<PersistedSyncRulesContent>;

  /**
   * Indicate that a slot was removed, and we should re-sync by creating
   * a new sync rules instance.
   *
   * This is roughly the same as deploying a new version of the current sync
   * rules, but also accounts for cases where the current sync rules are not
   * the latest ones.
   *
   * Replication should be restarted after this.
   *
   * @param slot_name The removed slot
   */
  slotRemoved(slot_name: string): Promise<void>;

  /**
   * Get the sync rules used for querying.
   */
  getActiveSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null>;

  /**
   * Get the sync rules used for querying.
   */
  getActiveSyncRulesContent(): Promise<PersistedSyncRulesContent | null>;

  /**
   * Get the sync rules that will be active next once done with initial replicatino.
   */
  getNextSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null>;

  /**
   * Get the sync rules that will be active next once done with initial replicatino.
   */
  getNextSyncRulesContent(): Promise<PersistedSyncRulesContent | null>;

  /**
   * Get all sync rules currently replicating. Typically this is the "active" and "next" sync rules.
   */
  getReplicatingSyncRules(): Promise<PersistedSyncRulesContent[]>;

  /**
   * Get all sync rules stopped but not terminated yet.
   */
  getStoppedSyncRules(): Promise<PersistedSyncRulesContent[]>;

  /**
   * Same as:
   * getInstance(await getActiveSyncRules()).getCheckpoint().
   */
  getActiveCheckpoint(): Promise<ActiveCheckpoint>;

  createWriteCheckpoint(user_id: string, lsns: Record<string, string>): Promise<bigint>;

  lastWriteCheckpoint(user_id: string, lsn: string): Promise<bigint | null>;

  watchWriteCheckpoint(user_id: string, signal: AbortSignal): AsyncIterable<WriteCheckpoint>;

  /**
   * Get storage size of active sync rules.
   */
  getStorageMetrics(): Promise<StorageMetrics>;

  /**
   * Get the unique identifier for this instance of Powersync
   */
  getPowerSyncInstanceId(): Promise<string>;
}

export interface WriteCheckpoint {
  base: ActiveCheckpoint;
  writeCheckpoint: bigint | null;
}

export interface ActiveCheckpoint {
  readonly checkpoint: util.OpId;
  readonly lsn: string | null;

  hasSyncRules(): boolean;

  getBucketStorage(): Promise<SyncRulesBucketStorage | null>;
}

export interface StorageMetrics {
  /**
   * Size of operations (bucket_data)
   */
  operations_size_bytes: number;

  /**
   * Size of parameter storage.
   *
   * Replication storage -> raw data as received from Postgres.
   */
  parameters_size_bytes: number;

  /**
   * Size of current_data.
   */
  replication_size_bytes: number;
}

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export interface PersistedSyncRulesContent {
  readonly id: number;
  readonly sync_rules_content: string;
  readonly slot_name: string;

  readonly last_fatal_error?: string | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules;

  lock(): Promise<ReplicationLock>;
}

export interface ReplicationLock {
  sync_rules_id: number;

  release(): Promise<void>;
}

export interface PersistedSyncRules {
  readonly id: number;
  readonly sync_rules: SqlSyncRules;
  readonly slot_name: string;
}

export interface UpdateSyncRulesOptions {
  content: string;
  lock?: boolean;
}

export interface SyncRulesBucketStorageOptions {
  sync_rules: SqlSyncRules;
  group_id: number;
}

export const DEFAULT_DOCUMENT_BATCH_LIMIT = 1000;
export const DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES = 1 * 1024 * 1024;

export interface BucketDataBatchOptions {
  /** Limit number of documents returned. Defaults to 1000. */
  limit?: number;

  /**
   * Limit size of chunks returned. Defaults to 1MB.
   *
   * This is a lower bound, not an upper bound. As soon as the chunk size goes over this limit,
   * it is returned.
   *
   * Note that an individual data row can be close to 16MB in size, so this does not help in
   * extreme cases.
   */
  chunkLimitBytes?: number;
}

export interface StartBatchOptions extends ParseSyncRulesOptions {
  zeroLSN: string;
}

export interface SyncRulesBucketStorage {
  readonly group_id: number;
  readonly slot_name: string;

  readonly factory: BucketStorageFactory;

  resolveTable(options: ResolveTableOptions): Promise<ResolveTableResult>;

  startBatch(
    options: StartBatchOptions,
    callback: (batch: BucketStorageBatch) => Promise<void>
  ): Promise<FlushedResult | null>;

  getCheckpoint(): Promise<{ checkpoint: util.OpId }>;

  getParsedSyncRules(options: ParseSyncRulesOptions): SqlSyncRules;

  getParameterSets(checkpoint: util.OpId, lookups: SqliteJsonValue[][]): Promise<SqliteJsonRow[]>;

  /**
   * Get a "batch" of data for a checkpoint.
   *
   * The results will be split into separate SyncBucketData chunks to:
   * 1. Separate buckets.
   * 2. Limit the size of each individual chunk according to options.batchSizeLimitBytes.
   *
   * @param checkpoint the checkpoint
   * @param dataBuckets current bucket states
   * @param options batch size options
   */
  getBucketDataBatch(
    checkpoint: util.OpId,
    dataBuckets: Map<string, string>,
    options?: BucketDataBatchOptions
  ): AsyncIterable<SyncBucketDataBatch>;

  /**
   * Compute checksums for a given list of buckets.
   *
   * Returns zero checksums for any buckets not found.
   */
  getChecksums(checkpoint: util.OpId, buckets: string[]): Promise<util.ChecksumMap>;

  /**
   * Terminate the sync rules.
   *
   * This clears the storage, and sets state to TERMINATED.
   *
   * Must only be called on stopped sync rules.
   */
  terminate(options?: TerminateOptions): Promise<void>;

  getStatus(): Promise<SyncRuleStatus>;

  /**
   * Clear the storage, without changing state.
   */
  clear(): Promise<void>;

  setSnapshotDone(lsn: string): Promise<void>;

  autoActivate(): Promise<void>;

  /**
   * Record a replication error.
   *
   * This could be a recoverable error (e.g. temporary network failure),
   * or a permanent error (e.g. missing toast data).
   *
   * Errors are cleared on commit.
   */
  reportError(e: any): Promise<void>;

  compact(options?: CompactOptions): Promise<void>;
}

export interface SyncRuleStatus {
  checkpoint_lsn: string | null;
  active: boolean;
  snapshot_done: boolean;
}
export interface ResolveTableOptions {
  group_id: number;
  connection_id: number;
  connection_tag: string;
  entity_descriptor: SourceEntityDescriptor;

  sync_rules: SqlSyncRules;
}

export interface ResolveTableResult {
  table: SourceTable;
  dropTables: SourceTable[];
}

export interface FlushedResult {
  flushed_op: string;
}

export interface BucketStorageBatch {
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
  flush(): Promise<FlushedResult | null>;

  /**
   * Flush and commit any saved ops. This creates a new checkpoint.
   *
   * Only call this after a transaction.
   */
  commit(lsn: string): Promise<boolean>;

  /**
   * Advance the checkpoint LSN position, without any associated op.
   *
   * This must only be called when not inside a transaction.
   *
   * @returns true if the checkpoint was advanced, false if this was a no-op
   */
  keepalive(lsn: string): Promise<boolean>;

  /**
   * Get the last checkpoint LSN, from either commit or keepalive.
   */
  lastCheckpointLsn: string | null;

  markSnapshotDone(tables: SourceTable[], no_checkpoint_before_lsn: string): Promise<SourceTable[]>;
}

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

export type SaveOptions = SaveInsert | SaveUpdate | SaveDelete;

export type ReplicaId = string | bson.UUID | bson.Document | any;

export interface SaveInsert {
  tag: 'insert';
  sourceTable: SourceTable;
  before?: undefined;
  beforeReplicaId?: undefined;
  after: SqliteRow;
  afterReplicaId: ReplicaId;
}

export interface SaveUpdate {
  tag: 'update';
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
  tag: 'delete';
  sourceTable: SourceTable;
  before?: SqliteRow;
  beforeReplicaId: ReplicaId;
  after?: undefined;
  afterReplicaId?: undefined;
}

export interface SyncBucketDataBatch {
  batch: util.SyncBucketData;
  targetOp: bigint | null;
}

export function mergeToast(record: ToastableSqliteRow, persisted: ToastableSqliteRow): ToastableSqliteRow {
  const newRecord: ToastableSqliteRow = {};
  for (let key in record) {
    if (typeof record[key] == 'undefined') {
      newRecord[key] = persisted[key];
    } else {
      newRecord[key] = record[key];
    }
  }
  return newRecord;
}

export interface CompactOptions {
  /**
   * Heap memory limit for the compact process.
   *
   * Add around 64MB to this to determine the "--max-old-space-size" argument.
   * Add another 80MB to get RSS usage / memory limits.
   */
  memoryLimitMB?: number;

  /**
   * If specified, ignore any operations newer than this when compacting.
   *
   * This is primarily for tests, where we want to test compacting at a specific
   * point.
   *
   * This can also be used to create a "safe buffer" of recent operations that should
   * not be compacted, to avoid invalidating checkpoints in use.
   */
  maxOpId?: bigint;

  /**
   * If specified, compact only the specific buckets.
   *
   * If not specified, compacts all buckets.
   */
  compactBuckets?: string[];
}

export interface TerminateOptions {
  /**
   * If true, also clear the storage before terminating.
   */
  clearStorage: boolean;
}
