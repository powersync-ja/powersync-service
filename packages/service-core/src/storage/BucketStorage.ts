import {
  AsyncDisposableObserverClient,
  DisposableListener,
  DisposableObserverClient
} from '@powersync/lib-services-framework';
import {
  EvaluatedParameters,
  EvaluatedRow,
  SqlSyncRules,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow,
  ToastableSqliteRow
} from '@powersync/service-sync-rules';
import { BSON } from 'bson';
import * as util from '../util/util-index.js';
import { ReplicationEventPayload } from './ReplicationEventPayload.js';
import { SourceEntityDescriptor } from './SourceEntity.js';
import { SourceTable } from './SourceTable.js';
import { BatchedCustomWriteCheckpointOptions } from './storage-index.js';
import { SyncStorageWriteCheckpointAPI } from './WriteCheckpointAPI.js';

/**
 * Replica id uniquely identifying a row on the source database.
 *
 * Can be any value serializable to BSON.
 *
 * If the value is an entire document, the data serialized to a v5 UUID may be a good choice here.
 */
export type ReplicaId = BSON.UUID | BSON.Document | any;

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
export interface BucketStorageFactoryListener extends DisposableListener {
  syncStorageCreated: (storage: SyncRulesBucketStorage) => void;
  replicationEvent: (event: ReplicationEventPayload) => void;
}

export interface BucketStorageSystemIdentifier {
  /**
   * A unique identifier for the system used for storage.
   * For Postgres this can be the cluster `system_identifier` and database name.
   * For MongoDB this can be the replica set name.
   */
  id: string;
  /**
   * A unique type for the storage implementation.
   * e.g. `mongodb`, `postgresql`.
   */
  type: string;
}

export interface BucketStorageFactory extends AsyncDisposableObserverClient<BucketStorageFactoryListener> {
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
   * Get the active storage instance.
   */
  getActiveStorage(): Promise<SyncRulesBucketStorage | null>;

  /**
   * Get storage size of active sync rules.
   */
  getStorageMetrics(): Promise<StorageMetrics>;

  /**
   * Get the unique identifier for this instance of Powersync
   */
  getPowerSyncInstanceId(): Promise<string>;

  /**
   * Get a unique identifier for the system used for storage.
   */
  getSystemIdentifier(): Promise<BucketStorageSystemIdentifier>;
}

export interface ReplicationCheckpoint {
  readonly checkpoint: util.OpId;
  readonly lsn: string | null;
}

export interface WatchWriteCheckpointOptions {
  /** user_id and client_id combined. */
  user_id: string;

  signal: AbortSignal;

  filter?: (event: WatchFilterEvent) => boolean;
}

export interface WatchFilterEvent {
  bucket?: string;
  invalidate?: boolean;
}

export interface WriteCheckpoint {
  base: ReplicationCheckpoint;
  writeCheckpoint: bigint | null;
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
  /**
   * Whether or not to store a copy of the current data.
   *
   * This is needed if we need to apply partial updates, for example
   * when we get TOAST values from Postgres.
   *
   * This is not needed when we get the full document from the source
   * database, for example from MongoDB.
   */
  storeCurrentData: boolean;

  /**
   * Set to true for initial replication.
   *
   * This will avoid creating new operations for rows previously replicated.
   */
  skipExistingRows?: boolean;
}

export interface SyncRulesBucketStorageListener extends DisposableListener {
  batchStarted: (batch: BucketStorageBatch) => void;
}

export interface SyncRulesBucketStorage
  extends DisposableObserverClient<SyncRulesBucketStorageListener>,
    SyncStorageWriteCheckpointAPI {
  readonly group_id: number;
  readonly slot_name: string;

  readonly factory: BucketStorageFactory;

  resolveTable(options: ResolveTableOptions): Promise<ResolveTableResult>;

  startBatch(
    options: StartBatchOptions,
    callback: (batch: BucketStorageBatch) => Promise<void>
  ): Promise<FlushedResult | null>;

  getCheckpoint(): Promise<ReplicationCheckpoint>;

  getParsedSyncRules(options: ParseSyncRulesOptions): SqlSyncRules;

  getParameterSets(checkpoint: util.OpId, lookups: SqliteJsonValue[][]): Promise<SqliteJsonRow[]>;

  /**
   * Yields the latest user write checkpoint whenever the sync checkpoint updates.
   *
   * The stream stops or errors if this is not the active sync rules (anymore).
   */
  watchWriteCheckpoint(options: WatchWriteCheckpointOptions): AsyncIterable<WriteCheckpoint>;

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

export interface BucketBatchStorageListener extends DisposableListener {
  replicationEvent: (payload: ReplicationEventPayload) => void;
}

export interface BucketBatchCommitOptions {
  /**
   * Creates a new checkpoint even if there were no persisted operations.
   * Defaults to true.
   */
  createEmptyCheckpoints?: boolean;
}

export type ResolvedBucketBatchCommitOptions = Required<BucketBatchCommitOptions>;

export const DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS: ResolvedBucketBatchCommitOptions = {
  createEmptyCheckpoints: true
};

export interface BucketStorageBatch extends DisposableObserverClient<BucketBatchStorageListener> {
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
   * Flush and commit any saved ops. This creates a new checkpoint by default.
   *
   * Only call this after a transaction.
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
   * Get the last checkpoint LSN, from either commit or keepalive.
   */
  lastCheckpointLsn: string | null;

  markSnapshotDone(tables: SourceTable[], no_checkpoint_before_lsn: string): Promise<SourceTable[]>;

  /**
   * Queues the creation of a custom Write Checkpoint. This will be persisted after operations are flushed.
   */
  addCustomWriteCheckpoint(checkpoint: BatchedCustomWriteCheckpointOptions): void;
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
   *
   * These can be individual bucket names, or bucket definition names.
   */
  compactBuckets?: string[];
}

export interface TerminateOptions {
  /**
   * If true, also clear the storage before terminating.
   */
  clearStorage: boolean;
}

/**
 * Helper for tests.
 * This is not in the `service-core-tests` package in order for storage modules
 * to provide relevant factories without requiring `service-core-tests` as a direct dependency.
 */
export interface TestStorageOptions {
  /**
   * By default, collections are only cleared/
   * Setting this to true will drop the collections completely.
   */
  dropAll?: boolean;

  doNotClear?: boolean;
}
export type TestStorageFactory = (options?: TestStorageOptions) => Promise<BucketStorageFactory>;
