import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  BaseObserver,
  DO_NOT_LOG,
  ErrorCode,
  Logger,
  ReplicationAbortedError,
  ServiceAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  BroadcastIterable,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  CompactInitialReplicationOptions,
  CompactInitialReplicationResults,
  GetCheckpointChangesOptions,
  InternalOpId,
  mergeAsyncIterables,
  ReplicationCheckpoint,
  ReplicationStreamStorageIds,
  storage,
  SyncRuleState,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { HydratedSyncConfig, ParameterLookupRows, ScopedParameterLookup } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { LRUCache } from 'lru-cache';
import * as timers from 'timers/promises';
import { DEFAULT_CLEAR_BATCH_THROTTLE_RATE } from '../../types/types.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import {
  MongoGetCheckpointChangesOptions,
  MongoSyncBucketStorageCheckpoint
} from './common/MongoSyncBucketStorageCheckpoint.js';
import { DEFAULT_INLINE_THRESHOLD_BYTES } from './common/PersistedBatch.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { StorageConfig } from './models.js';
import { MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { MongoChecksumOptions, MongoChecksums } from './MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from './MongoCompactor.js';
import { MongoParameterCompactor } from './MongoParameterCompactor.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';
import { MongoPersistedReplicationStream } from './MongoPersistedReplicationStream.js';
import { MongoWriteCheckpointAPI } from './MongoWriteCheckpointAPI.js';
import { ObjectStorage } from './v3/object-storage/ObjectStorage.js';

export interface MongoSyncBucketStorageOptions {
  checksumOptions?: Omit<MongoChecksumOptions, 'storageConfig'>;
  readPreference?: mongo.ReadPreference;
  checksumCacheTtlMs?: number;
  clearBatchThrottleRate?: number;
  storageConfig: StorageConfig;
  objectStorage?: ObjectStorage;
  inlineThresholdBytes?: number;
}

/**
 * The stream state read for a checkpoint. All fields must come from the same snapshot.
 */
export interface MongoCheckpointState {
  checkpoint: InternalOpId;
  lsn: string | null;
  /** See {@link MongoSyncBucketStorageCheckpoint.parameterChangesInvalidBefore}. */
  parameterChangesInvalidBefore: InternalOpId;
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
 * Above this many buckets (a collection-wide estimate), the report ranks a bounded sample of bucket_state
 * rather than every bucket, so the request cannot exhaust memory or run unbounded. Below it, the ranking is
 * exact.
 */
const BUCKET_SELECTION_SAMPLE_THRESHOLD = 50_000;

/**
 * Approximate number of buckets sampled when over {@link BUCKET_SELECTION_SAMPLE_THRESHOLD}. The sample is
 * drawn with `$sampleRate`, so the achieved count varies slightly around this.
 */
const BUCKET_SELECTION_SAMPLE_SIZE = 10_000;

/**
 * Most bucket_state index entries one report query may scan. Even when sampling fetches few documents, the
 * covered index scan and the matched-bucket count still touch every matched index entry once, so past this
 * the report fails fast instead of scaling without bound.
 */
const BUCKET_SELECTION_SCAN_MAX = 1_000_000;

export interface TopBucketSelection {
  buckets: storage.RankedBucketInput[];
  definitions: storage.RankedDefinitionInput[];
  /** True if more definitions exist than `definitions` holds ({@link storage.BUCKET_REPORT_DEFINITION_LIMIT}). */
  definitionsTruncated: boolean;
  totals: storage.BucketReportTotals;
}

/**
 * Version-specific aggregation expressions over a bucket_state document, feeding
 * {@link MongoSyncBucketStorage.aggregateTopBuckets}.
 */
export interface BucketStateReportExpressions {
  /** The bucket's current total operation count. */
  operations: mongo.Document;
  /** The bucket's current operation-history bytes, as a numeric expression. */
  operationBytes: mongo.Document;
  /**
   * Statistics captured by the bucket's last full compact. Omitted for storage versions that do not record
   * them (v1/v2), which limits the report to operation counts.
   */
  fullCompact?: {
    /** Operation count of the compacted prefix, e.g. `'$last_full_compact.count'`. */
    operations: unknown;
    /** PUT count of the compacted prefix (the row count as of the compact). */
    puts: unknown;
    /** When the full compact ran. */
    at: unknown;
    /** When the scheduled compactor next considers the bucket. */
    nextCompactAt: unknown;
  };
}

export abstract class MongoSyncBucketStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  readonly db: VersionedPowerSyncMongo;

  [DO_NOT_LOG] = true;

  readonly checksums: MongoChecksums;

  readonly objectStorage?: ObjectStorage;
  readonly inlineThresholdBytes: number;

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
  public readonly clearBatchThrottleRate: number;
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
    this.objectStorage = options.objectStorage;
    // Keep small chunks inline in MongoDB rather than offloading them to S3.
    // Configurable via object_storage.inline_threshold_bytes.
    this.inlineThresholdBytes = options.inlineThresholdBytes ?? DEFAULT_INLINE_THRESHOLD_BYTES;
    this.readPreference = options.readPreference;
    this.clearBatchThrottleRate = options.clearBatchThrottleRate ?? DEFAULT_CLEAR_BATCH_THROTTLE_RATE;
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

  /** MongoDB parameter compaction uses a persisted operation-id cursor. */
  public supportsIncrementalParameterCompaction(): boolean {
    return true;
  }

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

  createManagedWriteCheckpoints(
    checkpoints: storage.ManagedWriteCheckpointOptions[]
  ): Promise<storage.CreateManagedWriteCheckpointsResult> {
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

  protected abstract fetchCheckpointState(session: mongo.ClientSession): Promise<MongoCheckpointState | null>;

  async getCheckpointInternal(): Promise<storage.ReplicationCheckpoint | null> {
    return await this.db.client.withSession({ snapshot: true }, async (session) => {
      const state = await this.fetchCheckpointState(session);
      if (state == null) {
        return null;
      }

      const snapshotTime = (session as any).snapshotTime as bson.Timestamp | undefined;
      const clusterTime = session.clusterTime;
      if (snapshotTime == null) {
        throw new ServiceAssertionError('Missing snapshotTime in getCheckpoint()');
      }
      if (clusterTime == null) {
        throw new ServiceAssertionError('Missing clusterTime in getCheckpoint()');
      }
      return new MongoReplicationCheckpoint(
        this,
        state.checkpoint,
        state.lsn,
        snapshotTime,
        clusterTime,
        state.parameterChangesInvalidBefore
      );
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
      tracer: options.tracer,
      signal: options.signal,
      objectStorage: this.objectStorage,
      inlineThresholdBytes: this.inlineThresholdBytes
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
  ): AsyncIterable<storage.SyncBucketDataChunk | storage.SyncBucketDataBatchEnd>;

  async *getBucketDataBatch(
    checkpoint: storage.ReplicationCheckpoint,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk | storage.SyncBucketDataBatchEnd> {
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
      clusterTime: mongoCheckpoint.clusterTime,
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
      // For PROCESSING streams, this will be undefined.
      const checkpoint = await this.getCheckpointInternal();
      maxOpId = checkpoint?.checkpoint ?? undefined;
    }
    await this.createMongoCompactor({ ...options, maxOpId, logger: this.logger }).compact();

    if (maxOpId != null && options?.compactParameterData && this.replicationStream.state == SyncRuleState.ACTIVE) {
      // Use the stream-scoped logger, matching bucket compaction above.
      await this.createMongoParameterCompactor(maxOpId, { ...options, logger: this.logger }).compact();
    }
  }

  abstract compactInitialReplication(
    options: CompactInitialReplicationOptions
  ): Promise<CompactInitialReplicationResults>;

  async getBucketReport(options?: storage.GetBucketReportOptions): Promise<storage.BucketReport> {
    const limit = storage.resolveBucketReportLimit(options?.limit);
    try {
      // Everything comes from the pre-aggregated bucket state (one document per bucket, ranked and limited
      // in the database): exact operation counts plus the last full compact's statistics, from which the
      // row-level fields are derived. The operation history itself is never read.
      const { buckets, definitions, definitionsTruncated, totals } = await this.collectTopBuckets(limit);
      return storage.assembleBucketReport(buckets, definitions, totals, definitionsTruncated);
    } catch (e) {
      // Translate a storage query timeout (maxTimeMS) into a specific, retryable error code rather than a
      // generic internal error.
      throw lib_mongo.mapQueryError(e, 'while building the bucket report');
    }
  }

  /**
   * Select the worst-offender buckets (by operation count), the per-definition rollup, and instance-wide
   * operation totals from the pre-aggregated bucket state. Ranking and limiting happen in the database, so
   * memory stays bounded. Implementations supply their version-specific bucket state collection,
   * active-config filter, and stat expressions.
   */
  protected abstract collectTopBuckets(limit: number): Promise<TopBucketSelection>;

  /**
   * Rank buckets by operation count in the database and compute instance-wide operation totals, reading the
   * pre-aggregated bucket state. One document per bucket, no scan of bucket data.
   *
   * For very large bucket sets the candidates are drawn from a bounded sample of the matched `_id` index
   * range rather than the whole collection (so the request cannot run unbounded or exhaust memory), and the
   * totals are scaled from the sample and flagged estimated. `allowDiskUse: false` makes an over-threshold
   * exact attempt fail fast rather than spill to disk and degrade the live instance.
   *
   * Note: for v1/v2 storage, bucket_state is not backfilled (see models.ts: "only populated by new updates"),
   * so buckets that predate bucket_state tracking and have not been updated or compacted since are missing
   * here and under-counted. v3 always has bucket_state.
   */
  protected async aggregateTopBuckets<T extends { _id: { b: string } }>(
    collection: mongo.Collection<T>,
    match: mongo.Filter<T>,
    limit: number,
    exprs: BucketStateReportExpressions
  ): Promise<TopBucketSelection> {
    const { operations, operationBytes, fullCompact } = exprs;
    // Bucket names are `<definition>[<serialized parameters>]`, so everything before the first `[` groups a
    // bucket into its definition.
    const definitionKey = { $arrayElemAt: [{ $split: ['$_id.b', '['] }, 0] };

    // Reports are bulk reads: keep them off the primary by using the configured bulk read preference,
    // falling back to secondaryPreferred. Staleness does not matter for a report.
    const readPreference =
      this.readPreference ??
      new mongo.ReadPreference('secondaryPreferred', undefined, {
        // 90 is the minimum value.
        maxStalenessSeconds: 90
      });

    // estimatedDocumentCount is O(1) but ignores the match filter, so this is an upper bound on the active
    // bucket count. That is fine for the sampling decision: over-estimating only switches to sampling sooner.
    const estimatedTotalBuckets = await collection.estimatedDocumentCount({ readPreference });

    let matchedBuckets: number | null = null;
    if (estimatedTotalBuckets > BUCKET_SELECTION_SAMPLE_THRESHOLD) {
      // The exact matched-bucket count. `match` is an `_id` range, so this is an index-only scan; it sets
      // the sample rate, scales the sampled sums back up, and doubles as the exact totals.bucketCount.
      // `limit` caps how many index entries the count may touch: hitting the cap means the instance is past
      // what this report is designed to scan, so fail fast rather than read the index without bound.
      matchedBuckets = await collection.countDocuments(match, {
        maxTimeMS: storage.BUCKET_REPORT_TIMEOUT_MS,
        readPreference,
        limit: BUCKET_SELECTION_SCAN_MAX + 1
      });
      if (matchedBuckets > BUCKET_SELECTION_SCAN_MAX) {
        throw new ServiceError({
          status: 422,
          code: ErrorCode.PSYNC_S2001,
          description: `Bucket report is not supported on this instance: more than ${BUCKET_SELECTION_SCAN_MAX} buckets match the active sync configuration`
        });
      }
    }
    const sampleRate = matchedBuckets == null ? 1 : BUCKET_SELECTION_SAMPLE_SIZE / Math.max(matchedBuckets, 1);
    const sampled = sampleRate < 1;

    const pipeline: mongo.Document[] = [{ $match: match }];
    if (sampled) {
      // Sample on the index alone, then fetch only the sampled documents: the range $match plus the _id
      // projection is a covered index scan (explain shows docsExamined: 0), $sampleRate keeps roughly
      // SAMPLE_SIZE ids, and the self-$lookup fetches just those. Sampling after a plain $match would fetch
      // every matched document only to discard most of them.
      pipeline.push(
        { $project: { _id: 1 } },
        { $match: { $sampleRate: sampleRate } },
        { $lookup: { from: collection.collectionName, localField: '_id', foreignField: '_id', as: 'doc' } },
        { $unwind: '$doc' },
        { $replaceRoot: { newRoot: '$doc' } }
      );
    }
    // BSON comparison order places every concrete value above null/missing, so this is true exactly when
    // the bucket has full-compact statistics.
    const hasFullCompact = fullCompact == null ? false : { $gt: [fullCompact.operations, null] };
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
        top: [
          {
            $project: {
              _id: 0,
              bucket: '$_id.b',
              operations,
              operationBytes,
              ...(fullCompact && {
                compactedOperations: { $ifNull: [fullCompact.operations, null] },
                compactedPuts: { $ifNull: [fullCompact.puts, null] },
                lastFullCompactAt: { $ifNull: [fullCompact.at, null] },
                nextCompactAt: { $ifNull: [fullCompact.nextCompactAt, null] }
              })
            }
          },
          { $sort: { operations: -1 } },
          { $limit: limit }
        ],
        definitions: [
          {
            $group: {
              _id: definitionKey,
              operations: { $sum: operations },
              operationBytes: { $sum: operationBytes },
              bucketCount: { $sum: 1 },
              ...(fullCompact && {
                compactedBucketCount: { $sum: { $cond: [hasFullCompact, 1, 0] } },
                compactedOperations: { $sum: { $ifNull: [fullCompact.operations, 0] } },
                compactedPuts: { $sum: { $ifNull: [fullCompact.puts, 0] } }
              })
            }
          },
          { $sort: { operations: -1 } },
          // One past the cap: an extra result only signals that the rollup was truncated.
          { $limit: storage.BUCKET_REPORT_DEFINITION_LIMIT + 1 }
        ]
      }
    });

    type FacetResult = {
      totals: { operations: number; operationBytes: number; bucketCount: number }[];
      top: storage.RankedBucketInput[];
      definitions: {
        _id: string;
        operations: number;
        operationBytes: number;
        bucketCount: number;
        compactedBucketCount?: number;
        compactedOperations?: number;
        compactedPuts?: number;
      }[];
    };
    const [result] = await collection
      .aggregate<FacetResult>(pipeline, {
        allowDiskUse: false,
        maxTimeMS: storage.BUCKET_REPORT_TIMEOUT_MS,
        readPreference
      })
      .toArray();

    const rawTotals = result?.totals[0] ?? { operations: 0, operationBytes: 0, bucketCount: 0 };
    const buckets = result?.top ?? [];
    const rawDefinitions = result?.definitions ?? [];
    const definitionsTruncated = rawDefinitions.length > storage.BUCKET_REPORT_DEFINITION_LIMIT;
    const mapDefinitions = (scale: number): storage.RankedDefinitionInput[] =>
      rawDefinitions.slice(0, storage.BUCKET_REPORT_DEFINITION_LIMIT).map((d) => ({
        definition: d._id,
        bucketCount: Math.round(d.bucketCount * scale),
        operations: Math.round(d.operations * scale),
        operationBytes: Math.round(d.operationBytes * scale),
        ...(fullCompact && {
          compactedBucketCount: Math.round((d.compactedBucketCount ?? 0) * scale),
          compactedOperations: Math.round((d.compactedOperations ?? 0) * scale),
          compactedPuts: Math.round((d.compactedPuts ?? 0) * scale)
        })
      }));

    if (!sampled) {
      return {
        buckets,
        definitions: mapDefinitions(1),
        definitionsTruncated,
        totals: {
          bucketCount: rawTotals.bucketCount,
          operations: rawTotals.operations,
          operationBytes: rawTotals.operationBytes,
          estimated: false
        }
      };
    }

    // Scale the sampled sums up to the full matched set, using the exact matched count from above. The
    // sample is uniform across buckets, so the per-definition sums scale by the same factor; a definition
    // small enough to be missed by the sample entirely is absent. bucketCount itself is exact.
    const scale = matchedBuckets! / Math.max(rawTotals.bucketCount, 1);
    return {
      buckets,
      definitions: mapDefinitions(scale),
      definitionsTruncated,
      totals: {
        bucketCount: matchedBuckets!,
        operations: Math.round(rawTotals.operations * scale),
        operationBytes: Math.round(rawTotals.operationBytes * scale),
        estimated: true
      }
    };
  }

  /**
   * The highest op id persisted for this stream, whether or not covered by a checkpoint.
   *
   * Used as the default `maxOpId` for {@link compactInitialReplication}, which runs after
   * initial replication but before the first checkpoint exists.
   */
  protected abstract fetchPersistedOpHead(): Promise<InternalOpId | null>;

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
    options: MongoGetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>>;

  private async getParameterBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    const nextCheckpoint = requireMongoCheckpoint(options.nextCheckpoint);
    if (options.lastCheckpoint.checkpoint < nextCheckpoint.parameterChangesInvalidBefore) {
      // Parameter compaction may have deleted parameter entries in the range we'd have to query
      // to find the individual changed lookups. Invalidate all parameter buckets instead.
      //
      // The fence is committed before the first delete of a compaction pass, and captured in the
      // same snapshot as the checkpoint, so a checkpoint that could miss a deleted entry always
      // observes the fence as well.
      return {
        invalidateParameterBuckets: true,
        updatedParameterLookups: new Set<string>()
      };
    }
    // The query below runs at the checkpoint snapshot, which still sees entries deleted by a
    // compaction pass that started after the snapshot.
    return this.getParameterBucketChangesImpl({
      lastCheckpoint: options.lastCheckpoint,
      nextCheckpoint
    });
  }

  private checkpointChangesCache = new LRUCache<
    string,
    InternalCheckpointChanges,
    { options: GetCheckpointChangesOptions }
  >({
    max: 50,
    maxSize: 12 * 1024 * 1024,
    // When we have more fetches than the cache size, complete the fetches instead
    // of failing with Error('evicted').
    ignoreFetchAbort: true,
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
    // The invalidation fence is part of the identity: the same checkpoint pair read before and
    // after a compaction pass produces different results (specific lookups vs. invalidate-all).
    const fence = requireMongoCheckpoint(options.nextCheckpoint).parameterChangesInvalidBefore;
    const key = `${options.lastCheckpoint.checkpoint}_${options.lastCheckpoint.lsn}__${options.nextCheckpoint.checkpoint}_${options.nextCheckpoint.lsn}_${fence}`;
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

/**
 * We don't support any other constructions of ReplicationCheckpoint.
 */
function requireMongoCheckpoint(checkpoint: ReplicationCheckpoint): MongoReplicationCheckpoint {
  if (!(checkpoint instanceof MongoReplicationCheckpoint)) {
    throw new ServiceAssertionError(
      `Checkpoint changes require a checkpoint from getCheckpointInternal(), got ${checkpoint.constructor.name}`
    );
  }
  return checkpoint;
}

class MongoReplicationCheckpoint implements MongoSyncBucketStorageCheckpoint {
  #storage: MongoSyncBucketStorage;

  constructor(
    storage: MongoSyncBucketStorage,
    public readonly checkpoint: InternalOpId,
    public readonly lsn: string | null,
    public snapshotTime: mongo.Timestamp,
    public clusterTime: mongo.ClusterTime,
    /** Captured in the same snapshot as the checkpoint. */
    public readonly parameterChangesInvalidBefore: InternalOpId
  ) {
    this.#storage = storage;
  }

  async getParameterSets(lookups: ScopedParameterLookup[], limit: number): Promise<ParameterLookupRows[]> {
    return this.#storage.getParameterSets(this, lookups, limit);
  }
}

/**
 * Used when no checkpoint has been persisted yet. This has no snapshot or invalidation fence, so
 * it cannot be used for checkpoint change detection - see {@link requireMongoCheckpoint}.
 */
class EmptyReplicationCheckpoint implements ReplicationCheckpoint {
  readonly checkpoint: InternalOpId = 0n;
  readonly lsn: string | null = null;

  async getParameterSets(_lookups: ScopedParameterLookup[], _limit: number): Promise<ParameterLookupRows[]> {
    return [];
  }
}
