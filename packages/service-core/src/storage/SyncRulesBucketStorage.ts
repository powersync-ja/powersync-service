import { Logger, ObserverClient } from '@powersync/lib-services-framework';
import { ParameterLookup, SqlSyncRules, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as util from '../util/util-index.js';
import { BucketStorageBatch, FlushedResult, SaveUpdate } from './BucketStorageBatch.js';
import { BucketStorageFactory } from './BucketStorageFactory.js';
import { ParseSyncRulesOptions } from './PersistedSyncRulesContent.js';
import { SourceEntityDescriptor } from './SourceEntity.js';
import { SourceTable } from './SourceTable.js';
import { SyncStorageWriteCheckpointAPI } from './WriteCheckpointAPI.js';

/**
 * Storage for a specific copy of sync rules.
 */
export interface SyncRulesBucketStorage
  extends ObserverClient<SyncRulesBucketStorageListener>,
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
  clear(options?: ClearStorageOptions): Promise<void>;

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

  /**
   * Lightweight "compact" process to populate the checksum cache, if any.
   */
  populatePersistentChecksumCache(options?: Pick<CompactOptions, 'signal' | 'maxOpId'>): Promise<void>;

  // ## Read operations

  getCheckpoint(): Promise<ReplicationCheckpoint>;

  /**
   * Given two checkpoints, return the changes in bucket data and parameters that may have occurred
   * in that period.
   *
   * This is a best-effort optimization:
   * 1. This may include more changes than what actually occurred.
   * 2. This may return invalidateDataBuckets or invalidateParameterBuckets instead of of returning
   *    specific changes.
   * @param options
   */
  getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<CheckpointChanges>;

  /**
   * Yields the latest user write checkpoint whenever the sync checkpoint updates.
   *
   * The stream stops or errors if this is not the active sync rules (anymore).
   */
  watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<StorageCheckpointUpdate>;

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
    checkpoint: util.InternalOpId,
    dataBuckets: Map<string, util.InternalOpId>,
    options?: BucketDataBatchOptions
  ): AsyncIterable<SyncBucketDataChunk>;

  /**
   * Compute checksums for a given list of buckets.
   *
   * Returns zero checksums for any buckets not found.
   *
   * This may be slow, depending on the size of the buckets.
   * The checksums are cached internally to compensate for this, but does not cover all cases.
   */
  getChecksums(checkpoint: util.InternalOpId, buckets: string[]): Promise<util.ChecksumMap>;

  /**
   * Clear checksum cache. Primarily intended for tests.
   */
  clearChecksumCache(): void;
}

export interface SyncRulesBucketStorageListener {
  batchStarted: (batch: BucketStorageBatch) => void;
}

export interface SyncRuleStatus {
  checkpoint_lsn: string | null;
  active: boolean;
  snapshot_done: boolean;
  snapshot_lsn: string | null;
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

  /**
   * Callback called if we streamed an update to a record that we don't have yet.
   *
   * This is expected to happen in some initial replication edge cases, only if storeCurrentData = true.
   */
  markRecordUnavailable?: BucketStorageMarkRecordUnavailable;

  logger?: Logger;
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
  maxOpId?: util.InternalOpId;

  /**
   * If specified, compact only the specific buckets.
   *
   * If not specified, compacts all buckets.
   *
   * These can be individual bucket names, or bucket definition names.
   */
  compactBuckets?: string[];

  compactParameterData?: boolean;

  /** Minimum of 2 */
  clearBatchLimit?: number;

  /** Minimum of 1 */
  moveBatchLimit?: number;

  /** Minimum of 1 */
  moveBatchQueryLimit?: number;

  /**
   * Internal/testing use: Cache size for compacting parameters.
   */
  compactParameterCacheLimit?: number;

  signal?: AbortSignal;
}

export interface ClearStorageOptions {
  signal?: AbortSignal;
}

export interface TerminateOptions extends ClearStorageOptions {
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

export interface SyncBucketDataChunk {
  chunkData: util.SyncBucketData;
  targetOp: util.InternalOpId | null;
}

export interface ReplicationCheckpoint {
  readonly checkpoint: util.InternalOpId;
  readonly lsn: string | null;

  /**
   * Used to resolve "dynamic" parameter queries.
   *
   * This gets parameter sets specific to this checkpoint.
   */
  getParameterSets(lookups: ParameterLookup[]): Promise<SqliteJsonRow[]>;
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
  writeCheckpoint: util.InternalOpId | null;
}

export interface StorageCheckpointUpdate extends WriteCheckpoint {
  update: CheckpointChanges;
}

export interface GetCheckpointChangesOptions {
  lastCheckpoint: ReplicationCheckpoint;
  nextCheckpoint: ReplicationCheckpoint;
}

export interface CheckpointChanges {
  updatedDataBuckets: Set<string>;
  invalidateDataBuckets: boolean;
  /** Serialized using JSONBig */
  updatedParameterLookups: Set<string>;
  invalidateParameterBuckets: boolean;
}

export const CHECKPOINT_INVALIDATE_ALL: CheckpointChanges = {
  updatedDataBuckets: new Set<string>(),
  invalidateDataBuckets: true,
  updatedParameterLookups: new Set<string>(),
  invalidateParameterBuckets: true
};

export type BucketStorageMarkRecordUnavailable = (record: SaveUpdate) => void;
