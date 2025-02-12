import { DisposableListener, DisposableObserverClient } from '@powersync/lib-services-framework';
import { SqlSyncRules, SqliteJsonRow, SqliteJsonValue } from '@powersync/service-sync-rules';
import * as util from '../util/util-index.js';
import { BucketStorageBatch, FlushedResult } from './BucketStorageBatch.js';
import { BucketStorageFactory } from './BucketStorageFactory.js';
import { ParseSyncRulesOptions } from './PersistedSyncRulesContent.js';
import { SourceEntityDescriptor } from './SourceEntity.js';
import { SourceTable } from './SourceTable.js';
import { SyncStorageWriteCheckpointAPI } from './WriteCheckpointAPI.js';

/**
 * Storage for a specific copy of sync rules.
 */
export interface SyncRulesBucketStorage
  extends DisposableObserverClient<SyncRulesBucketStorageListener>,
    SyncStorageWriteCheckpointAPI {
  readonly group_id: number;
  readonly slot_name: string;

  readonly factory: BucketStorageFactory;

  /**
   * Resolve a table, keeping track of it internally.
   */
  resolveTable(options: ResolveTableOptions): Promise<ResolveTableResult>;

  /**
   * Use this to get access to update storage data.
   */
  startBatch(
    options: StartBatchOptions,
    callback: (batch: BucketStorageBatch) => Promise<void>
  ): Promise<FlushedResult | null>;

  getParsedSyncRules(options: ParseSyncRulesOptions): SqlSyncRules;

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

  // ## Read operations

  getCheckpoint(): Promise<ReplicationCheckpoint>;

  /**
   * Used to resolve "dynamic" parameter queries.
   */
  getParameterSets(checkpoint: util.OpId, lookups: SqliteJsonValue[][]): Promise<SqliteJsonRow[]>;

  getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<CheckpointChanges>;

  /**
   * Yields the latest user write checkpoint whenever the sync checkpoint updates.
   *
   * The stream stops or errors if this is not the active sync rules (anymore).
   */
  watchWriteCheckpoint(options: WatchWriteCheckpointOptions): AsyncIterable<StorageCheckpointUpdate>;

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
}

export interface SyncRulesBucketStorageListener extends DisposableListener {
  batchStarted: (batch: BucketStorageBatch) => void;
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

export interface SyncBucketDataBatch {
  batch: util.SyncBucketData;
  targetOp: bigint | null;
}

export interface ReplicationCheckpoint {
  readonly checkpoint: util.OpId;
  readonly lsn: string | null;
}

export interface WatchWriteCheckpointOptions {
  /** user_id and client_id combined. */
  user_id: string;

  signal: AbortSignal;
}

export interface WatchFilterEvent {
  changedDataBucket?: string;
  changedParameterBucketDefinition?: string;
  invalidate?: boolean;
}

export interface WriteCheckpoint {
  base: ReplicationCheckpoint;
  writeCheckpoint: bigint | null;
}

export interface StorageCheckpointUpdate extends WriteCheckpoint {
  update: CheckpointChanges;
}

export interface GetCheckpointChangesOptions {
  lastCheckpoint: util.OpId;
  nextCheckpoint: util.OpId;
}

export interface CheckpointChanges {
  updatedDataBuckets: string[];
  invalidateDataBuckets: boolean;
  updatedParameterBucketDefinitions: string[];
  invalidateParameterBuckets: boolean;
}
