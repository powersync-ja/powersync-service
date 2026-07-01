import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  BaseObserver,
  DO_NOT_LOG,
  Logger,
  ReplicationAbortedError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
import {
  BroadcastIterable,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  GetCheckpointChangesOptions,
  InternalOpId,
  mergeAsyncIterables,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  ReplicationCheckpoint,
  ReplicationStreamStorageIds,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import {
  BucketDefinitionId,
  HydratedSyncConfig,
  ParameterLookupRows,
  ScopedParameterLookup
} from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { LRUCache } from 'lru-cache';
import * as timers from 'timers/promises';
import { retryOnMongoMaxTimeMSExpired } from '../../utils/util.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { BucketStateDocumentBase, StorageConfig } from './models.js';
import { MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { MongoChecksumOptions, MongoChecksums } from './MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from './MongoCompactor.js';
import { MongoParameterCompactor } from './MongoParameterCompactor.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';
import { MongoPersistedReplicationStream } from './MongoPersistedReplicationStream.js';
import { MongoWriteCheckpointAPI } from './MongoWriteCheckpointAPI.js';

export interface MongoSyncBucketStorageOptions {
  checksumOptions?: Omit<MongoChecksumOptions, 'storageConfig'>;
  readPreference?: mongo.ReadPreference;
  checksumCacheTtlMs?: number;
  storageConfig: StorageConfig;
}

interface InternalCheckpointChanges extends CheckpointChanges {
  updatedWriteCheckpoints: Map<string, bigint>;
  invalidateWriteCheckpoints: boolean;
}

/**
 * Only keep checkpoints around for a minute, before fetching a fresh one.
 *
 * The reason is that we keep a MongoDB snapshot reference (clusterTime) with the checkpoint,
 * and they expire after 5 minutes by default. This is an issue if the checkpoint stream is idle,
 * but new clients connect and use an outdated checkpoint snapshot for parameter queries.
 *
 * These will be filtered out for existing clients, so should not create significant overhead.
 */
const CHECKPOINT_TIMEOUT_MS = 60_000;

/**
 * Above this many buckets, the report ranks a bounded `$sample` of bucket_state rather than every bucket, so
 * the request cannot exhaust memory or run unbounded. Below it, the ranking is exact.
 */
const BUCKET_SELECTION_SAMPLE_THRESHOLD = 50_000;

/** Number of buckets to sample when over {@link BUCKET_SELECTION_SAMPLE_THRESHOLD}. */
const BUCKET_SELECTION_SAMPLE_SIZE = 10_000;

/**
 * Fewest operations sampled per bucket when estimating its row count. Buckets with fewer operations than
 * this are read in full (exact).
 */
const BUCKET_ROW_SAMPLE_MIN = 1_000;

/**
 * Most operations sampled per bucket, capping the per-bucket cost on very large buckets at the price of a
 * weaker estimate for buckets that are both extremely wide and barely fragmented (see {@link bucketRowSampleTarget}).
 */
const BUCKET_ROW_SAMPLE_MAX = 25_000;

/** Maximum number of per-bucket row-estimate queries to run concurrently while building a report. */
const BUCKET_ROW_SAMPLE_CONCURRENCY = 10;

/** A worst-offender bucket selected from bucket_state, with the version-specific context needed to sample it. */
export interface TopBucketCandidate {
  bucket: string;
  operations: number;
  operationBytes: number;
  /** v3 only: the bucket definition id, used to locate its per-definition bucket_data collection. */
  defId?: BucketDefinitionId;
}

export interface TopBucketSelection {
  buckets: TopBucketCandidate[];
  totals: storage.BucketReportTotals;
}

export interface BucketRowEstimate {
  rows: number;
  /** True if `rows` is a sampled estimate rather than an exact count. */
  estimated: boolean;
}

export abstract class MongoSyncBucketStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  readonly db: VersionedPowerSyncMongo;

  [DO_NOT_LOG] = true;

  readonly checksums: MongoChecksums;

  /**
   * Canonical parsed sync config sets, keyed by defaultSchema.
   *
   * Entries are never evicted: each parse options value maps to exactly one parsed set for
   * the lifetime of this storage instance, so parsed source objects and mappings always
   * stay associated.
   */
  private readonly parsedSyncConfigSets = new Map<string, MongoParsedSyncConfigSet>();
  private writeCheckpointAPI: MongoWriteCheckpointAPI;
  public readonly logger: Logger;
  public readonly storageConfig: StorageConfig;
  public readonly readPreference: mongo.ReadPreference | undefined;
  #storageInitialized = false;

  constructor(
    public readonly factory: MongoBucketStorage,
    public readonly replicationStreamId: number,
    public readonly replicationStream: MongoPersistedReplicationStream,
    public readonly replicationStreamName: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super();
    this.storageConfig = options.storageConfig;
    this.readPreference = options.readPreference;
    this.db = factory.db.versioned(this.storageConfig);
    this.checksums = this.createMongoChecksums(options);
    this.writeCheckpointAPI = new MongoWriteCheckpointAPI({
      db: this.db,
      mode: writeCheckpointMode ?? storage.WriteCheckpointMode.MANAGED,
      sync_rules_id: replicationStreamId
    });
    this.logger = replicationStream.logger;
  }

  /**
   * Not for external use - public here for tests only.
   *
   * @internal
   */
  abstract createMongoCompactor(options: MongoCompactOptions): MongoCompactor;

  protected abstract createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums;
  protected abstract createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor;

  get writeCheckpointMode() {
    return this.writeCheckpointAPI.writeCheckpointMode;
  }

  /**
   * Persisted storage ids of all sync configs in this replication stream. Parse-free.
   */
  get storageIds(): ReplicationStreamStorageIds {
    return this.replicationStream.storageIds;
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.writeCheckpointAPI.setWriteCheckpointMode(mode);
  }

  createManagedWriteCheckpoints(checkpoints: storage.ManagedWriteCheckpointOptions[]): Promise<Map<string, bigint>> {
    return this.writeCheckpointAPI.createManagedWriteCheckpoints(checkpoints);
  }

  lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    return this.writeCheckpointAPI.lastWriteCheckpoint({
      ...filters,
      sync_rules_id: this.replicationStreamId
    });
  }

  getParsedSyncConfigSet(options: storage.ParseSyncConfigOptions): MongoParsedSyncConfigSet {
    let parsed = this.parsedSyncConfigSets.get(options.defaultSchema);
    if (parsed == null) {
      parsed = this.replicationStream.parsed(options);
      this.parsedSyncConfigSets.set(options.defaultSchema, parsed);
    }
    return parsed;
  }

  getParsedSyncRules(options: storage.ParseSyncConfigOptions): HydratedSyncConfig {
    return this.getParsedSyncConfigSet(options).hydratedSyncConfig;
  }

  async getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    return (await this.getCheckpointInternal()) ?? new EmptyReplicationCheckpoint();
  }

  protected abstract fetchCheckpointState(
    session: mongo.ClientSession
  ): Promise<{ checkpoint: bigint; lsn: string | null } | null>;

  async getCheckpointInternal(): Promise<storage.ReplicationCheckpoint | null> {
    return await this.db.client.withSession({ snapshot: true }, async (session) => {
      const state = await this.fetchCheckpointState(session);
      if (state == null) {
        return null;
      }

      const snapshotTime = (session as any).snapshotTime as bson.Timestamp | undefined;
      if (snapshotTime == null) {
        throw new ServiceAssertionError('Missing snapshotTime in getCheckpoint()');
      }
      return new MongoReplicationCheckpoint(this, state.checkpoint, state.lsn, snapshotTime);
    });
  }

  protected abstract initializeVersionStorage(): Promise<void>;

  private async initializeStorage() {
    if (this.#storageInitialized) {
      return;
    }

    await this.db.initializeStreamStorage(this.replicationStreamId);
    await this.initializeVersionStorage();
    this.#storageInitialized = true;
  }

  /**
   * Create the version-specific writer. Implementations fetch their own resume state
   * (e.g. resume LSN, v1 keepalive op) and construct the batch from
   * {@link writerBatchOptions} plus the version-specific fields.
   */
  protected abstract createWriterImpl(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch>;

  /**
   * The version-independent part of the batch options.
   */
  protected writerBatchOptions(options: storage.CreateWriterOptions): Omit<MongoBucketBatchOptions, 'resumeFromLsn'> {
    return {
      logger: options.logger ?? this.logger,
      db: this.db,
      parsedSyncConfig: this.getParsedSyncConfigSet(options),
      replicationStreamId: this.replicationStreamId,
      replicationStreamName: this.replicationStreamName,
      storeCurrentData: options.storeCurrentData,
      skipExistingRows: options.skipExistingRows ?? false,
      markRecordUnavailable: options.markRecordUnavailable,
      hooks: options.hooks,
      tracer: options.tracer
    };
  }

  async createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    await this.initializeStorage();

    const writer = await this.createWriterImpl(options);
    this.iterateListeners((cb) => cb.batchStarted?.(writer));
    return writer;
  }

  async startBatch(
    options: storage.CreateWriterOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    await using writer = await this.createWriter(options);
    await callback(writer);
    await writer.flush();
    return writer.last_flushed_op != null ? { flushed_op: writer.last_flushed_op } : null;
  }

  protected abstract getParameterSetsImpl(
    checkpoint: MongoReplicationCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<ParameterLookupRows[]>;

  async getParameterSets(
    checkpoint: MongoReplicationCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<ParameterLookupRows[]> {
    return this.getParameterSetsImpl(checkpoint, lookups, limit);
  }

  protected abstract getBucketDataBatchImpl(
    checkpoint: MongoReplicationCheckpoint,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk>;

  async *getBucketDataBatch(
    checkpoint: storage.ReplicationCheckpoint,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    yield* this.getBucketDataBatchImpl(checkpoint as MongoReplicationCheckpoint, dataBuckets, options);
  }

  async getChecksums(
    checkpoint: storage.ReplicationCheckpoint,
    buckets: storage.BucketChecksumRequest[],
    options?: storage.BucketChecksumOptions
  ): Promise<utils.ChecksumMap> {
    const mongoCheckpoint = checkpoint as MongoReplicationCheckpoint;
    const snapshotTime = mongoCheckpoint.snapshotTime; // May be undefined in tests
    return this.checksums.getChecksums(checkpoint.checkpoint, buckets, {
      snapshotTime,
      readPreference: options?.requestHint == 'bulk' ? this.readPreference : undefined
    });
  }

  clearChecksumCache() {
    this.checksums.clearCache();
  }

  protected abstract terminateSyncRuleState(): Promise<void>;

  async terminate(options?: storage.TerminateOptions) {
    if (!options || options?.clearStorage) {
      await this.clear(options);
    }
    await this.terminateSyncRuleState();
    await this.db.notifyCheckpoint();
  }

  protected abstract getStatusImpl(): Promise<storage.ReplicationStreamStatus>;

  async getStatus(): Promise<storage.ReplicationStreamStatus> {
    return this.getStatusImpl();
  }

  protected abstract clearBucketData(signal?: AbortSignal): Promise<void>;

  protected abstract clearParameterIndexes(signal?: AbortSignal): Promise<void>;

  protected abstract clearSourceRecords(signal?: AbortSignal): Promise<void>;

  protected abstract clearBucketState(signal?: AbortSignal): Promise<void>;

  protected abstract clearSourceTables(signal?: AbortSignal): Promise<void>;
  protected abstract clearSyncRuleState(): Promise<void>;

  async clear(options?: storage.ClearStorageOptions): Promise<void> {
    const signal = options?.signal;

    if (signal?.aborted) {
      throw new ReplicationAbortedError('Aborted clearing data', signal.reason);
    }

    await this.clearSyncRuleState();

    await this.clearBucketData(signal);
    await this.clearParameterIndexes(signal);
    await this.clearSourceRecords(signal);
    await this.clearBucketState(signal);
    await this.clearSourceTables(signal);

    this.#storageInitialized = false;
  }

  protected async clearDeleteMany(
    label: string,
    operation: () => Promise<mongo.DeleteResult>,
    signal?: AbortSignal
  ): Promise<void> {
    await retryOnMongoMaxTimeMSExpired(operation, {
      signal,
      abortMessage: 'Aborted clearing data',
      retryDelayMs: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS / 5,
      onRetry: () => {
        this.logger.info(
          `Cleared batch of ${label} in ${lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS}ms, continuing...`
        );
      }
    });
  }

  async reportError(e: any): Promise<void> {
    const message = String(e.message ?? 'Replication failure');
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          last_fatal_error: message,
          last_fatal_error_ts: new Date()
        }
      }
    );
  }

  async compact(options?: storage.CompactOptions) {
    let maxOpId = options?.maxOpId;
    if (maxOpId == null) {
      const checkpoint = await this.getCheckpointInternal();
      maxOpId = checkpoint?.checkpoint ?? undefined;
    }
    await this.createMongoCompactor({ ...options, maxOpId, logger: this.logger }).compact();

    if (maxOpId != null && options?.compactParameterData) {
      await this.createMongoParameterCompactor(maxOpId, options).compact();
    }
  }

  async getBucketReport(options?: storage.GetBucketReportOptions): Promise<storage.BucketReport> {
    const limit = storage.resolveBucketReportLimit(options?.limit);
    try {
      // Rank the worst-offender buckets and total operations from the pre-aggregated bucket state (bounded,
      // in the database), then estimate each returned bucket's row count by sampling its operation history.
      const { buckets, totals } = await this.collectTopBuckets(limit);
      // Each bucket's row estimate is an independent query; run a bounded number concurrently so the report
      // cost scales with the limit without firing one query per bucket serially.
      const ranked: storage.RankedBucketInput[] = new Array(buckets.length);
      let cursor = 0;
      const runWorker = async () => {
        while (true) {
          const index = cursor++;
          if (index >= buckets.length) {
            return;
          }
          const candidate = buckets[index];
          const estimate = await this.estimateBucketRows(candidate);
          ranked[index] = {
            bucket: candidate.bucket,
            operations: candidate.operations,
            operationBytes: candidate.operationBytes,
            rows: estimate.rows,
            rowsEstimated: estimate.estimated
          };
        }
      };
      const workers = Math.min(BUCKET_ROW_SAMPLE_CONCURRENCY, buckets.length);
      await Promise.all(Array.from({ length: workers }, () => runWorker()));
      return storage.assembleBucketReport(ranked, totals);
    } catch (e) {
      // Translate a storage query timeout (maxTimeMS) into a specific, retryable error code rather than a
      // generic internal error.
      throw lib_mongo.mapQueryError(e, 'while building the bucket report');
    }
  }

  /**
   * Select the worst-offender buckets (by operation count) plus instance-wide operation totals from the
   * pre-aggregated bucket state. Ranking and limiting happen in the database, so memory stays bounded.
   * Implementations supply their version-specific bucket state collection and active-config filter.
   */
  protected abstract collectTopBuckets(limit: number): Promise<TopBucketSelection>;

  /**
   * Estimate a single bucket's live row count by sampling its operation history. Implementations differ
   * because v1/v2 store one document per operation while v3 batches operations per document.
   */
  protected abstract estimateBucketRows(candidate: TopBucketCandidate): Promise<BucketRowEstimate>;

  /**
   * Rank buckets by operation count in the database and compute instance-wide operation totals, reading the
   * pre-aggregated bucket state (compacted_state + estimate_since_compact). One document per bucket, no scan
   * of bucket data.
   *
   * For very large bucket sets the candidates are drawn from a bounded `$sample` rather than the whole
   * collection (so the request cannot run unbounded or exhaust memory), and the totals are scaled from the
   * sample and flagged estimated. `allowDiskUse: false` makes an over-threshold exact attempt fail fast
   * rather than spill to disk and degrade the live instance.
   *
   * Note: for v1/v2 storage, bucket_state is not backfilled (see models.ts: "only populated by new updates"),
   * so buckets that predate bucket_state tracking and have not been updated or compacted since are missing
   * here and under-counted. v3 always has bucket_state.
   */
  protected async aggregateTopBuckets<T extends BucketStateDocumentBase>(
    collection: mongo.Collection<T>,
    match: mongo.Filter<T>,
    limit: number
  ): Promise<{
    buckets: { id: T['_id']; operations: number; operationBytes: number }[];
    totals: storage.BucketReportTotals;
  }> {
    const operations = {
      $add: [{ $ifNull: ['$compacted_state.count', 0] }, { $ifNull: ['$estimate_since_compact.count', 0] }]
    };
    const operationBytes = {
      $add: [
        { $toDouble: { $ifNull: ['$compacted_state.bytes', 0] } },
        { $toDouble: { $ifNull: ['$estimate_since_compact.bytes', 0] } }
      ]
    };

    // estimatedDocumentCount is O(1) but ignores the match filter, so this is an upper bound on the active
    // bucket count. That is fine for the sampling decision: over-estimating only switches to sampling sooner.
    // It must NOT be used to scale the sampled totals though - the collection can hold buckets outside the
    // match (other replication groups for v1/v2, inactive definitions for v3), which would over-scale.
    const estimatedTotalBuckets = await collection.estimatedDocumentCount();
    const sampled = estimatedTotalBuckets > BUCKET_SELECTION_SAMPLE_THRESHOLD;

    const pipeline: mongo.Document[] = [{ $match: match }];
    if (sampled) {
      pipeline.push({ $sample: { size: BUCKET_SELECTION_SAMPLE_SIZE } });
    }
    pipeline.push({
      $facet: {
        totals: [
          {
            $group: {
              _id: null,
              operations: { $sum: operations },
              operationBytes: { $sum: operationBytes },
              bucketCount: { $sum: 1 }
            }
          }
        ],
        top: [{ $project: { _id: 1, operations, operationBytes } }, { $sort: { operations: -1 } }, { $limit: limit }]
      }
    });

    type FacetResult = {
      totals: { operations: number; operationBytes: number; bucketCount: number }[];
      top: { _id: T['_id']; operations: number; operationBytes: number }[];
    };
    const [result] = await collection
      .aggregate<FacetResult>(pipeline, { allowDiskUse: false, maxTimeMS: storage.BUCKET_REPORT_TIMEOUT_MS })
      .toArray();

    const rawTotals = result?.totals[0] ?? { operations: 0, operationBytes: 0, bucketCount: 0 };
    const buckets = (result?.top ?? []).map((doc) => ({
      id: doc._id,
      operations: doc.operations,
      operationBytes: doc.operationBytes
    }));

    if (!sampled) {
      return {
        buckets,
        totals: {
          bucketCount: rawTotals.bucketCount,
          operations: rawTotals.operations,
          operationBytes: rawTotals.operationBytes,
          estimated: false
        }
      };
    }

    // Scale the sampled totals up to the full *matched* set. countDocuments respects the match filter (so it
    // excludes other groups / inactive definitions) and uses the _id index; it only runs on the already-large
    // sampled path, and is bounded by maxTimeMS like the rest of the report. When the matched set fits within
    // the sample, rawTotals is already exact and the scale collapses to 1.
    const matchedBuckets = await collection.countDocuments(match, { maxTimeMS: storage.BUCKET_REPORT_TIMEOUT_MS });
    const scale = matchedBuckets / Math.max(rawTotals.bucketCount, 1);
    return {
      buckets,
      totals: {
        bucketCount: matchedBuckets,
        operations: Math.round(rawTotals.operations * scale),
        operationBytes: Math.round(rawTotals.operationBytes * scale),
        estimated: true
      }
    };
  }

  /**
   * Estimate a bucket's live rows from a sample of its operations.
   *
   * `pipelinePrefix` must select the bucket's operations (and, when `sampled`, randomly down-sample them) and
   * yield documents with top-level `op`, `table` and `row_id` fields. Returns the distinct row count (exact
   * when the whole bucket was read, otherwise estimated via {@link storage.estimateDistinctRows}); fragmentation is
   * then `operations / rows`.
   */
  protected async estimateRowsFromOperationSample(
    collection: mongo.Collection<any>,
    pipelinePrefix: mongo.Document[],
    operations: number,
    sampled: boolean
  ): Promise<BucketRowEstimate> {
    const pipeline: mongo.Document[] = [
      ...pipelinePrefix,
      {
        $facet: {
          sampledOps: [{ $count: 'count' }],
          distinctRows: [
            { $match: { op: { $in: ['PUT', 'REMOVE'] } } },
            { $group: { _id: { table: '$table', row_id: '$row_id' } } },
            { $count: 'count' }
          ]
        }
      }
    ];

    type FacetResult = { sampledOps: { count: number }[]; distinctRows: { count: number }[] };
    const [result] = await collection
      .aggregate<FacetResult>(pipeline, { allowDiskUse: false, maxTimeMS: storage.BUCKET_REPORT_TIMEOUT_MS })
      .toArray();

    const sampledOps = result?.sampledOps[0]?.count ?? 0;
    const distinctRows = result?.distinctRows[0]?.count ?? 0;
    if (sampledOps == 0 || distinctRows == 0) {
      // Nothing row-bearing was sampled (e.g. a bucket of only MOVE/CLEAR ops): treat as fully fragmented.
      return { rows: 0, estimated: sampled };
    }
    if (!sampled) {
      // Read in full: the distinct row count is exact.
      return { rows: distinctRows, estimated: false };
    }
    return { rows: storage.estimateDistinctRows(operations, sampledOps, distinctRows), estimated: true };
  }

  /**
   * How many operations to sample when estimating a bucket's row count.
   *
   * {@link storage.estimateDistinctRows} recovers the true row count from how often the sample lands on the
   * same row twice ("collisions"). A bucket with `R` rows produces collisions only once the sample size
   * approaches `sqrt(R)`, and needs roughly `sqrt(100 * R)` before they carry a usable signal. `R` is unknown
   * up front but is bounded by the operation count, so sampling `sqrt(200 * operations)` operations yields on
   * the order of 100 expected collisions even in the worst case of one row per operation - enough to keep the
   * estimate stable rather than swinging with sampling noise. Clamped to [MIN, MAX] to bound per-bucket cost;
   * above the MAX-implied width the estimate degrades gracefully (only for buckets both very wide and barely
   * fragmented, which are not the fragmented offenders the report exists to surface).
   */
  protected bucketRowSampleTarget(operations: number): number {
    const target = Math.ceil(Math.sqrt(200 * operations));
    return Math.min(BUCKET_ROW_SAMPLE_MAX, Math.max(BUCKET_ROW_SAMPLE_MIN, target));
  }

  /** Whether a bucket with this many operations should be sampled rather than read in full. */
  protected shouldSampleBucketRows(operations: number): boolean {
    return operations > this.bucketRowSampleTarget(operations);
  }

  /** `$sampleRate` for sampling roughly {@link bucketRowSampleTarget} operations from a bucket. */
  protected bucketRowSampleRate(operations: number): number {
    return this.bucketRowSampleTarget(operations) / operations;
  }

  /**
   * The highest op id persisted for this stream, whether or not covered by a checkpoint.
   *
   * Used as the default `maxOpId` for {@link populatePersistentChecksumCache}, which runs after
   * initial replication but before the first checkpoint exists.
   */
  protected abstract fetchPersistedOpHead(): Promise<InternalOpId | null>;

  async populatePersistentChecksumCache(options: PopulateChecksumCacheOptions): Promise<PopulateChecksumCacheResults> {
    this.logger.info(`Populating persistent checksum cache...`);
    const start = Date.now();
    const maxOpId = options.maxOpId ?? (await this.fetchPersistedOpHead()) ?? undefined;
    const compactor = this.createMongoCompactor({
      ...options,
      maxOpId,
      memoryLimitMB: 0,
      logger: this.logger
    });

    const result = await compactor.populateChecksums({
      minBucketChanges: options.minBucketChanges ?? 10
    });
    const duration = Date.now() - start;
    this.logger.info(`Populated persistent checksum cache in ${(duration / 1000).toFixed(1)}s`);
    return result;
  }

  private async *watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<ReplicationCheckpoint> {
    if (signal.aborted) {
      return;
    }

    const stream = mergeAsyncIterables(
      [this.checkpointChangesStream(signal), this.checkpointTimeoutStream(signal)],
      signal
    );

    for await (const _ of stream) {
      if (signal.aborted) {
        break;
      }

      const op = await this.getCheckpointInternal();
      if (op == null) {
        break;
      }

      yield op;
    }
  }

  private readonly sharedIter = new BroadcastIterable((signal) => {
    return this.watchActiveCheckpoint(signal);
  });

  async *watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<storage.StorageCheckpointUpdate> {
    let lastCheckpoint: ReplicationCheckpoint | null = null;

    const iter = this.sharedIter[Symbol.asyncIterator](options.signal);

    let writeCheckpoint: bigint | null = null;
    let queriedInitialWriteCheckpoint = false;

    for await (const nextCheckpoint of iter) {
      if (nextCheckpoint.lsn != null && !queriedInitialWriteCheckpoint) {
        writeCheckpoint = await this.writeCheckpointAPI.lastWriteCheckpoint({
          sync_rules_id: this.replicationStreamId,
          user_id: options.user_id,
          heads: {
            '1': nextCheckpoint.lsn
          }
        });
        queriedInitialWriteCheckpoint = true;
      }

      if (
        lastCheckpoint != null &&
        lastCheckpoint.checkpoint == nextCheckpoint.checkpoint &&
        lastCheckpoint.lsn == nextCheckpoint.lsn
      ) {
        await timers.setTimeout(20 + 10 * Math.random());
        continue;
      }

      if (lastCheckpoint == null) {
        yield {
          base: nextCheckpoint,
          writeCheckpoint,
          update: CHECKPOINT_INVALIDATE_ALL
        };
      } else {
        const updates = await this.getCheckpointChanges({
          lastCheckpoint,
          nextCheckpoint
        });

        let updatedWriteCheckpoint = updates.updatedWriteCheckpoints.get(options.user_id) ?? null;
        if (updates.invalidateWriteCheckpoints) {
          updatedWriteCheckpoint = await this.writeCheckpointAPI.lastWriteCheckpoint({
            sync_rules_id: this.replicationStreamId,
            user_id: options.user_id,
            heads: {
              '1': nextCheckpoint.lsn!
            }
          });
        }
        if (updatedWriteCheckpoint != null && (writeCheckpoint == null || updatedWriteCheckpoint > writeCheckpoint)) {
          writeCheckpoint = updatedWriteCheckpoint;
          queriedInitialWriteCheckpoint = true;
        }

        yield {
          base: nextCheckpoint,
          writeCheckpoint,
          update: {
            updatedDataBuckets: updates.updatedDataBuckets,
            invalidateDataBuckets: updates.invalidateDataBuckets,
            updatedParameterLookups: updates.updatedParameterLookups,
            invalidateParameterBuckets: updates.invalidateParameterBuckets
          }
        };
      }

      lastCheckpoint = nextCheckpoint;
    }
  }

  private async *checkpointChangesStream(signal: AbortSignal): AsyncGenerator<void> {
    if (signal.aborted) {
      return;
    }

    const query = () => {
      return this.db.checkpoint_events.find(
        {},
        { tailable: true, awaitData: true, maxAwaitTimeMS: 10_000, batchSize: 1000 }
      );
    };

    let cursor = query();

    signal.addEventListener('abort', () => {
      cursor.close().catch(() => {});
    });

    yield;

    try {
      while (!signal.aborted) {
        const doc = await cursor.tryNext().catch((e) => {
          if (lib_mongo.isMongoServerError(e) && e.codeName === 'CappedPositionLost') {
            cursor = query();
            return {};
          } else {
            return Promise.reject(e);
          }
        });
        if (cursor.closed) {
          return;
        }
        cursor.readBufferedDocuments();
        if (doc != null) {
          yield;
        }
      }
    } catch (e) {
      if (signal.aborted) {
        return;
      }
      throw e;
    } finally {
      await cursor.close();
    }
  }

  private async *checkpointTimeoutStream(signal: AbortSignal): AsyncGenerator<void> {
    while (!signal.aborted) {
      try {
        await timers.setTimeout(CHECKPOINT_TIMEOUT_MS, undefined, { signal });
      } catch (e) {
        if (e.name == 'AbortError') {
          return;
        }
        throw e;
      }

      if (!signal.aborted) {
        yield;
      }
    }
  }

  protected abstract getDataBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>>;

  private async getDataBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    return this.getDataBucketChangesImpl(options);
  }

  protected abstract getParameterBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>>;

  private async getParameterBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    return this.getParameterBucketChangesImpl(options);
  }

  private checkpointChangesCache = new LRUCache<
    string,
    InternalCheckpointChanges,
    { options: GetCheckpointChangesOptions }
  >({
    max: 50,
    maxSize: 12 * 1024 * 1024,
    sizeCalculation: (value: InternalCheckpointChanges) => {
      const paramSize = [...value.updatedParameterLookups].reduce<number>((a, b) => a + b.length, 0);
      const bucketSize = [...value.updatedDataBuckets].reduce<number>((a, b) => a + b.length, 0);
      const writeCheckpointSize = value.updatedWriteCheckpoints.size * 30;
      return 100 + paramSize + bucketSize + writeCheckpointSize;
    },
    fetchMethod: async (_key, _staleValue, options) => {
      return this.getCheckpointChangesInternal(options.context.options);
    }
  });

  async getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<InternalCheckpointChanges> {
    const key = `${options.lastCheckpoint.checkpoint}_${options.lastCheckpoint.lsn}__${options.nextCheckpoint.checkpoint}_${options.nextCheckpoint.lsn}`;
    const result = await this.checkpointChangesCache.fetch(key, { context: { options } });
    return result!;
  }

  private async getCheckpointChangesInternal(options: GetCheckpointChangesOptions): Promise<InternalCheckpointChanges> {
    const dataUpdates = await this.getDataBucketChanges(options);
    const parameterUpdates = await this.getParameterBucketChanges(options);
    const writeCheckpointUpdates = await this.writeCheckpointAPI.getWriteCheckpointChanges(options);

    return {
      ...dataUpdates,
      ...parameterUpdates,
      ...writeCheckpointUpdates
    };
  }
}

class MongoReplicationCheckpoint implements ReplicationCheckpoint {
  #storage: MongoSyncBucketStorage;

  constructor(
    storage: MongoSyncBucketStorage,
    public readonly checkpoint: InternalOpId,
    public readonly lsn: string | null,
    public snapshotTime: mongo.Timestamp
  ) {
    this.#storage = storage;
  }

  async getParameterSets(lookups: ScopedParameterLookup[], limit: number): Promise<ParameterLookupRows[]> {
    return this.#storage.getParameterSets(this, lookups, limit);
  }
}

class EmptyReplicationCheckpoint implements ReplicationCheckpoint {
  readonly checkpoint: InternalOpId = 0n;
  readonly lsn: string | null = null;

  async getParameterSets(_lookups: ScopedParameterLookup[], _limit: number): Promise<ParameterLookupRows[]> {
    return [];
  }
}
