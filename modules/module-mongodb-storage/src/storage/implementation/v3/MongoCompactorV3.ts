import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, InternalOpId, storage, utils } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { BucketDataKey } from '../models.js';
import { ConcurrentCompactionError, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import { loadBucketDataDocument, maxOpId, serializeBucketData } from './bucket-format.js';
import { BucketDataContextV3 } from './BucketDataContextV3.js';
import { DEFAULT_MAX_DOC_SIZE_BYTES } from './chunking.js';
import { CompactionLease } from './CompactionLease.js';
import { BucketDataDocumentV3, BucketStateDocumentV3 } from './models.js';
import type { MongoSyncBucketStorageV3 } from './MongoSyncBucketStorageV3.js';
import { BucketDataObjectStorage, hydrateBucketDataDocuments } from './object-storage/BucketDataObjectStorage.js';
import { ObjectStorageLifecycle, PreparedObjectStorageUpload } from './object-storage/ObjectStorageLifecycle.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

interface PendingCompactionGroup {
  /**
   * Input documents are ordered from oldest to newest, matching `ops`.
   * Keeping the inputs intact lets unchanged singletons retain their object.
   */
  inputs: BucketDataDocumentV3[];
  ops: BucketDataDoc[];
  changed: boolean;
  targetOp: InternalOpId | null;
}

enum CompactionKind {
  Full = 'full',
  Chunks = 'chunks'
}

interface CompactionDecision {
  kind: CompactionKind | null;
  nextCompactCheck: mongo.Document;
}

interface ScheduledCompactionOptions {
  /** Process checks scheduled this far after the captured job start. */
  dueAheadMs?: number;
  /** Used by initial replication, which must not run a full compact. */
  forceKind?: CompactionKind;
}

class CompactionContext {
  constructor(
    readonly lease: CompactionLease,
    readonly kind: CompactionKind,
    readonly decision: CompactionDecision
  ) {}

  get state() {
    return this.lease.state;
  }

  get startedAt() {
    return this.lease.startedAt;
  }

  get lastOp() {
    return this.lease.lastOp;
  }
}

interface BucketStats {
  count: number;
  bytes: bigint;
  chunks: number;
}

/** Bucket stats read from bucket-data documents, including their checksum. */
interface BucketStatsWithChecksum extends BucketStats {
  checksum: number;
}

interface CompactionResult {
  /** Metadata cached at compacted_state.op_id. */
  compactedState: BucketStatsWithChecksum;
  /** Complete bucket metadata through the op head captured at claim time. */
  bucketStats: BucketStats;
}

const DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_MIN_COMPACT_FULL_INTERVAL_MS = 2 * 60 * 60 * 1000;
const DEFAULT_MAX_COMPACT_FULL_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_COMPACT_LEASE_DURATION_MS = 10 * 60 * 1000;
const FULL_COMPACT_RESCHEDULE_MARGIN_MS = 60 * 1000;
const SCHEDULED_COMPACTION_BATCH_SIZE = 100;
/**
 * Perform chunk compaction if at least this many chunks have been added since
 * the latest compaction.
 *
 * A lower value increases compaction frequency and can increase chunk rewrites
 * and object-storage operations. A higher value leaves more small chunks for
 * longer, which can hurt sync performance.
 */
const MERGE_CHUNKS_THRESHOLD = 8;

/**
 * Read one bounded prefix from a compaction cursor.
 *
 * The document that would cross the byte limit is deliberately not returned:
 * pagination resumes past the last returned `_id`, so that document remains
 * eligible for the next query. The first document is always accepted to
 * ensure progress when a single document exceeds the configured byte limit.
 *
 * `hasMore` is conservative when the document limit is reached. An extra empty
 * query is preferable to exhausting the cursor just to determine whether the
 * limited MongoDB query contained another document.
 */
async function readCompactionBatch(
  cursor: mongo.AggregationCursor<BucketDataDocumentV3>,
  options: { byteLimit: number; documentLimit: number }
): Promise<{ documents: BucketDataDocumentV3[]; hasMore: boolean }> {
  const documents: BucketDataDocumentV3[] = [];
  let cumulativeBytes = 0;

  try {
    for await (const document of cursor) {
      if (documents.length > 0 && cumulativeBytes + document.size > options.byteLimit) {
        return { documents, hasMore: true };
      }

      documents.push(document);
      cumulativeBytes += document.size;

      if (documents.length >= options.documentLimit) {
        return { documents, hasMore: true };
      }
    }
    return { documents, hasMore: false };
  } finally {
    await cursor.close();
  }
}

function bucketStats(state: BucketStateDocumentV3): BucketStats {
  return {
    count: state.bucket_stats.count,
    bytes: state.bucket_stats.bytes,
    chunks: state.bucket_stats.chunks
  };
}

function emptyBucketStats(): BucketStatsWithChecksum {
  return { count: 0, bytes: 0n, chunks: 0, checksum: 0 };
}

function statsForDocument(
  document: Pick<BucketDataDocumentV3, 'count' | 'size' | 'checksum'>
): BucketStatsWithChecksum {
  return {
    count: document.count,
    bytes: BigInt(document.size),
    chunks: 1,
    checksum: addChecksums(0, Number(document.checksum))
  };
}

/** A scheduled bucket always has writes awaiting a full compact. */
function firstUncompactedWrite(state: BucketStateDocumentV3): Date {
  if (state.first_uncompacted_write == null) {
    throw new ReplicationAssertionError(`Scheduled V3 bucket ${state._id.b} has no first uncompacted write`);
  }
  return state.first_uncompacted_write;
}

export class MongoCompactorV3 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  declare protected readonly storage: MongoSyncBucketStorageV3;

  private readonly minCompactChunkIntervalMs: number;
  private readonly minCompactFullIntervalMs: number;
  private readonly maxCompactFullIntervalMs: number;
  private readonly compactLeaseDurationMs: number;
  private readonly maxOpIdCap: InternalOpId | undefined;

  constructor(bucketStorage: MongoSyncBucketStorageV3, db: VersionedPowerSyncMongoV3, options: storage.CompactOptions) {
    super(bucketStorage, db, options);
    this.minCompactChunkIntervalMs = options.minCompactChunkIntervalMs ?? DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS;
    this.minCompactFullIntervalMs = options.minCompactFullIntervalMs ?? DEFAULT_MIN_COMPACT_FULL_INTERVAL_MS;
    this.maxCompactFullIntervalMs = options.maxCompactFullIntervalMs ?? DEFAULT_MAX_COMPACT_FULL_INTERVAL_MS;
    this.compactLeaseDurationMs = options.compactLeaseDurationMs ?? DEFAULT_COMPACT_LEASE_DURATION_MS;
    this.maxOpIdCap = options.maxOpId;
  }

  override async compact(): Promise<number> {
    if (this.storage.objectStorage) {
      // Clean these before compacting - should be quick in most cases.
      try {
        await this.objectStorageLifecycle.cleanup(this.logger);
      } catch (e) {
        // In this case, still continue normal compact process
        this.logger.error(`Failed to clean up object storage deletion markers before compaction`, e);
      }
    }
    await this.deleteOldCheckpointRequests();

    if (this.buckets != null) {
      await this.compactExplicitBuckets(this.buckets);
    } else if (this.compactChunksOnly) {
      // Writers defer their first chunk-compaction check by minCompactChunkIntervalMs.
      // Include that interval so this synchronous initial-replication pass
      // processes the work that existed when it started.
      await this.compactScheduledBuckets({
        dueAheadMs: this.minCompactChunkIntervalMs,
        forceKind: CompactionKind.Chunks
      });
    } else {
      await this.compactScheduledBuckets();
    }
    if (this.storage.objectStorage) {
      // Cleanup for any produced during compacting.
      // Note that markers only expire after a delay, so this may skip many produced during this compact
      // run. However, during long compact runs, this may also have many ones it can clean up.
      await this.objectStorageLifecycle.cleanup(this.logger);
    }
    return this.compactedBucketCount;
  }

  /** An explicit compact request always runs a full compact for its buckets. */
  private async compactExplicitBuckets(buckets: string[]) {
    for (const bucket of buckets) {
      // This is not a super efficient query, but this is not a common use case.
      // May be optimized later.
      const states = await this.db
        .bucketState(this.group_id)
        .find({ '_id.b': bucket }, { projection: { _id: 1 } })
        .toArray();
      for (const state of states) {
        await using lease = await this.claimBucket({ _id: state._id });
        if (lease == null || lease.state.first_uncompacted_write == null) {
          continue;
        }
        const decision = this.chooseCompactionKind(lease.state, lease.startedAt);
        await this.compactClaimedBucket(lease, CompactionKind.Full, decision);
      }
    }
  }

  /**
   * Process scheduled work in bounded batches.
   *
   * Batching specifically help to cover cases of many buckets where no compaction is required:
   * Instead of sequentially claiming and then rescheduling a bucket, this handles it in bulk.
   *
   * Buckets that do need compaction are still claimed and processed sequentially.
   *
   * Any concurrent workers may read the same batch. Rescheduling filters out buckets handled
   * by a concurrent worker or replication write, while buckets that do need compaction are
   * filtered out when claiming a compaction lease.
   *
   * We filter scheduled jobs by the job start date, so that the same bucket is not compacted
   * multiple times in one run. One tweak to this is for the run after initial replication:
   * We use dueAheadMs to also include buckets scheduled during initial replication.
   */
  private async compactScheduledBuckets(options: ScheduledCompactionOptions = {}) {
    const jobStartedAt = new Date();
    const dueBefore = new Date(jobStartedAt.getTime() + (options.dueAheadMs ?? 0));
    const forceKind = options.forceKind;
    while (true) {
      this.signal?.throwIfAborted();
      const states = await this.findScheduledBucketBatch(dueBefore);
      if (states.length == 0) {
        break;
      }

      const scheduled = states.map((state) => ({
        state,
        decision: this.chooseCompactionKind(state, jobStartedAt)
      }));
      const noOpStates = scheduled.filter(
        ({ state, decision }) => state.compact_lease == null && forceKind == null && decision.kind == null
      );
      await this.rescheduleUnclaimedBuckets(noOpStates);

      for (const { state, decision } of scheduled) {
        const kind = forceKind ?? decision.kind;
        if (state.compact_lease == null && kind == null) {
          continue;
        }

        await using lease = await this.claimBucket({ _id: state._id, next_compact_check: { $lte: dueBefore } });
        if (lease == null) {
          continue;
        }
        const claimedDecision = this.chooseCompactionKind(lease.state, lease.startedAt);
        const claimedKind = forceKind ?? claimedDecision.kind;
        if (claimedKind == null) {
          await this.rescheduleClaimedBucket(lease, claimedDecision);
        } else {
          await this.compactClaimedBucket(lease, claimedKind, claimedDecision);
        }
      }
    }
  }

  /** Read a bounded, priority-ordered snapshot of currently claimable scheduled work. */
  private async findScheduledBucketBatch(dueBefore: Date): Promise<BucketStateDocumentV3[]> {
    return this.db
      .bucketState(this.group_id)
      .find({
        next_compact_check: { $lte: dueBefore },
        $expr: {
          $or: [{ $eq: [{ $type: '$compact_lease' }, 'missing'] }, { $lte: ['$compact_lease.expires_at', '$$NOW'] }]
        }
      })
      .sort({ next_compact_check: 1 })
      .limit(SCHEDULED_COMPACTION_BATCH_SIZE)
      .toArray();
  }

  /**
   * Reschedule snapshots that were already known to be no-ops without first
   * taking a lease. Every decision input is compared so a concurrent writer
   * or compactor simply makes the update a no-op instead of losing work.
   */
  private async rescheduleUnclaimedBuckets(states: { state: BucketStateDocumentV3; decision: CompactionDecision }[]) {
    if (states.length == 0) {
      return;
    }
    await this.db.bucketState(this.group_id).bulkWrite(
      states.map(({ state, decision }) => ({
        updateOne: {
          filter: this.unclaimedSnapshotFilter(state),
          update: [{ $set: { next_compact_check: decision.nextCompactCheck } }]
        }
      })),
      { ordered: false }
    );
  }

  /**
   * This checks that the bucket state hasn't changed by a concurrent write since we checked.
   *
   * If it has changed, we'll re-check in the next batch.
   */
  private unclaimedSnapshotFilter(state: BucketStateDocumentV3): mongo.Filter<BucketStateDocumentV3> {
    return {
      _id: state._id,
      last_op: state.last_op,
      next_compact_check: state.next_compact_check,
      first_uncompacted_write: state.first_uncompacted_write ?? { $exists: false },
      bucket_stats: state.bucket_stats,
      compacted_state: state.compacted_state ?? { $exists: false },
      last_full_compact: state.last_full_compact ?? { $exists: false },
      compact_lease: { $exists: false }
    };
  }

  private async claimBucket(
    filter: mongo.Filter<BucketStateDocumentV3>,
    sort?: mongo.Sort
  ): Promise<CompactionLease | null> {
    return CompactionLease.claim(this.db.bucketState(this.group_id), filter, sort, this.compactLeaseDurationMs);
  }

  private async compactClaimedBucket(lease: CompactionLease, kind: CompactionKind, decision: CompactionDecision) {
    const context = new CompactionContext(lease, kind, decision);
    lease.startRenewal();
    await this.retryCompaction(context.state._id.b, () => this.compactSingleBucket(context));
  }

  private chooseCompactionKind(state: BucketStateDocumentV3, now: Date): CompactionDecision {
    // For chunk compaction, we consider the number of chunks added.
    // Right now, we trigger a compact if the interval has passed and at least a threshold of chunks were added.
    // A future policy could also use bytes per chunk or records per chunk to
    // decide when to compact.
    const fullCheckAt = this.fullCompactionCheckAt(state);
    // Schedule a little late so a worker using a slightly earlier clock does
    // not wake before the exact full-compaction condition is true.
    const fullCheckWithMargin = new Date(fullCheckAt.getTime() + FULL_COMPACT_RESCHEDULE_MARGIN_MS);
    const compacted = state.compacted_state;
    const chunksSinceCompact = Math.max(0, state.bucket_stats.chunks - (compacted?.chunks ?? 0));
    const shouldCompactChunks = chunksSinceCompact >= MERGE_CHUNKS_THRESHOLD;
    const canCheckChunks =
      compacted == null || now.getTime() - compacted.at.getTime() >= this.minCompactChunkIntervalMs;
    // Too few new chunks cannot make chunk compaction eligible. Do not poll this
    // bucket at the chunk-compaction interval; only wake it for its full-compact check.
    const nextCompactCheck: mongo.Document = !shouldCompactChunks
      ? fullCheckWithMargin
      : {
          $min: [
            fullCheckWithMargin,
            { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: this.minCompactChunkIntervalMs } }
          ]
        };
    let kind: CompactionKind | null = null;
    if (now >= fullCheckAt) {
      kind = CompactionKind.Full;
    } else if (canCheckChunks && shouldCompactChunks) {
      kind = CompactionKind.Chunks;
    }
    return {
      kind,
      nextCompactCheck
    };
  }

  private async rescheduleClaimedBucket(lease: CompactionLease, decision: CompactionDecision) {
    await lease.reschedule(decision.nextCompactCheck);
  }

  /**
   * Calculate the earliest full compaction time from the first uncompacted
   * write, bounded by the maximum retention interval.
   */
  private fullCompactionCheckAt(state: BucketStateDocumentV3): Date {
    const firstWrite = firstUncompactedWrite(state);
    const stats = bucketStats(state);
    const lastFull = state.last_full_compact;

    // The number of operations since the last full compact.
    // We may make this more specific in the future, to track new updates and deletes only, ignoring
    // full new inserts, but that requires more granular tracking when replicating.
    const uncompactedCount = lastFull == null ? stats.count : Math.max(0, stats.count - lastFull.count);
    const compactedRows = lastFull?.puts ?? 0;

    // If no full compact has ever been performed: ratio = 1, compact after minCompactFullIntervalMs.
    // If every row has been updated or deleted exactly once since the last full compact: ratio = 0.5, compact after minCompactFullIntervalMs * 2.
    // If 10% of rows has been updated since the last full compact, compact after minCompactFullIntervalMs * 11.
    // If every row has been updated multiple times, the ratio tends closer to 1 again.
    const ratio = uncompactedCount == 0 ? 0 : uncompactedCount / (compactedRows + uncompactedCount);
    const fullIntervalMs = ratio == 0 ? this.maxCompactFullIntervalMs : this.minCompactFullIntervalMs / ratio;
    return new Date(firstWrite.getTime() + Math.min(fullIntervalMs, this.maxCompactFullIntervalMs));
  }

  private compactMaxOpId(context: CompactionContext): InternalOpId {
    return this.maxOpIdCap == null || context.lastOp < this.maxOpIdCap ? context.lastOp : this.maxOpIdCap;
  }

  private get objectStorageLifecycle(): ObjectStorageLifecycle {
    if (!this.storage.objectStorage) {
      throw new Error('Object storage is not configured');
    }
    return new ObjectStorageLifecycle(this.db, this.group_id, this.storage.objectStorage);
  }

  private async compactSingleBucket(context: CompactionContext) {
    // A retry restarts finalization after a transient replacement failure.
    context.lease.restartFinalization();
    if (context.kind == CompactionKind.Chunks) {
      return this.compactSingleBucketChunks(context);
    }

    return this.compactSingleBucketFully(context);
  }

  /**
   * Merge adjacent bucket-data chunks without inspecting their operations
   * unless a merge is possible. The metadata contains enough information to
   * update the persisted checksum state and to decide whether a group can fit
   * in one chunk.
   */
  private async compactSingleBucketChunks(context: CompactionContext) {
    const bucket = context.state._id.b;
    const resolvedDefinitionId = context.state._id.d;
    const bucketContext = new BucketDataContextV3(this.db, {
      bucket,
      definitionId: resolvedDefinitionId,
      replicationStreamId: this.group_id
    });
    const collection = this.db.bucketData(this.group_id, resolvedDefinitionId);
    const dataContext = { replicationStreamId: this.group_id, definitionId: resolvedDefinitionId };
    // Include the last previously compacted chunk as well as new chunks. It
    // is the only old chunk which can become mergeable with the new tail.
    let lowerBound =
      context.state.compacted_state?.op_id != null && context.state.compacted_state.op_id > 0n
        ? bucketContext.docId(context.state.compacted_state.op_id - 1n)
        : bucketContext.minId;
    const upperBound = bucketContext.docId(this.compactMaxOpId(context) + 1n);

    let compactedOpId: bigint | null = null;
    let overlappingCompactedChunk: BucketStatsWithChecksum | undefined;
    let preCompactionTail = emptyBucketStats();
    const tailLowerBound = lowerBound;
    let pendingChunks: BucketDataDocumentV3[] = [];
    let pendingSize = 0;

    while (true) {
      this.signal?.throwIfAborted();
      await context.lease.throwIfLost();

      const batch = await readCompactionBatch(
        collection.aggregate<BucketDataDocumentV3>(
          [
            {
              $match: {
                _id: {
                  $gt: lowerBound,
                  $lt: upperBound
                }
              }
            },
            { $sort: { _id: 1 } },
            { $limit: this.moveBatchQueryLimit },
            {
              $project: {
                _id: 1,
                min_op: 1,
                checksum: 1,
                count: 1,
                size: 1,
                target_op: 1,
                storage_ref: 1,
                has_clear_op: 1
              }
            }
          ],
          { batchSize: this.moveBatchQueryLimit + 1 }
        ),
        {
          byteLimit: this.moveBatchByteLimit,
          documentLimit: this.moveBatchQueryLimit
        }
      );

      if (batch.documents.length == 0) {
        break;
      }

      for (const doc of batch.documents) {
        compactedOpId = maxOpId(compactedOpId, doc._id.o);
        const documentStats = statsForDocument(doc);
        preCompactionTail = this.combineAdjacentStats(preCompactionTail, documentStats);
        if (context.state.compacted_state?.op_id === doc._id.o) {
          overlappingCompactedChunk = documentStats;
        }

        const nextSize = pendingSize + doc.size;
        if (pendingChunks.length > 0 && nextSize > DEFAULT_MAX_DOC_SIZE_BYTES) {
          await this.flushChunkMerge(bucket, pendingChunks, collection, dataContext, bucketContext);
          pendingChunks = [];
          pendingSize = 0;
        }

        pendingChunks.push(doc);
        pendingSize += doc.size;
      }

      lowerBound = batch.documents[batch.documents.length - 1]._id;
      if (!batch.hasMore) {
        break;
      }
    }

    if (pendingChunks.length > 1) {
      await this.flushChunkMerge(bucket, pendingChunks, collection, dataContext, bucketContext);
    }

    if (compactedOpId == null) {
      await this.finalizeSkippedBucket(context);
      return;
    }

    const tailStats = await this.readBucketStats(bucket, resolvedDefinitionId, compactedOpId, tailLowerBound);
    const compactedStats = this.combineChunkStats(context.state, tailStats, overlappingCompactedChunk);
    const result = {
      compactedState: compactedStats,
      bucketStats: this.applyCompactionDelta(bucketStats(context.state), preCompactionTail, tailStats)
    };

    await this.finalizeCompactedBucket({ context, compactedOpId, compactionResult: result, puts: 0 });
    this.compactedBucketCount++;
    this.logger.info(`Lightly compacted bucket ${bucket}: ${result.bucketStats.count} ops`);
  }

  private async flushChunkMerge(
    bucket: string,
    inputs: BucketDataDocumentV3[],
    collection: mongo.Collection<BucketDataDocumentV3>,
    context: { replicationStreamId: number; definitionId: string },
    bucketContext: BucketDataContextV3
  ) {
    if (inputs.length < 2) {
      return;
    }

    // The metadata scan deliberately excluded ops. Read inline payloads only
    // for this merge group; object-storage payloads are fetched below using
    // the same rule.
    const inlineInputs = inputs.filter((input) => input.storage_ref == null);
    if (inlineInputs.length > 0) {
      const inlineDocuments = await collection
        .find({ _id: { $in: inlineInputs.map((input) => input._id) } }, { projection: { _id: 1, ops: 1 } })
        .toArray();
      const opsById = new Map(inlineDocuments.map((document) => [document._id.o.toString(), document.ops]));
      for (const input of inlineInputs) {
        input.ops = opsById.get(input._id.o.toString());
      }
    }
    await hydrateBucketDataDocuments(inputs, this.storage.objectStorage, { signal: this.signal });

    const operations = inputs.flatMap((input) => Array.from(loadBucketDataDocument(context, input)));
    const targetOp = inputs.reduce<InternalOpId | null>(
      (maxTarget, input) => maxOpId(maxTarget, input.target_op),
      null
    );
    await this.flushCompactionGroup(
      bucket,
      {
        inputs,
        ops: operations,
        changed: true,
        targetOp
      },
      bucketContext,
      context
    );
  }

  private async finalizeCompactedBucket({
    context,
    compactedOpId,
    compactionResult,
    puts
  }: {
    context: CompactionContext;
    compactedOpId: InternalOpId;
    compactionResult: CompactionResult;
    puts: number;
  }) {
    await context.lease.throwIfLost();
    const startedStats = bucketStats(context.state);
    const delta = {
      count: compactionResult.bucketStats.count - startedStats.count,
      bytes: compactionResult.bucketStats.bytes - startedStats.bytes,
      chunks: compactionResult.bucketStats.chunks - startedStats.chunks
    };
    const coveredStart = compactedOpId >= context.lastOp;
    const concurrentWriteCheck = { $gt: ['$last_op', context.lastOp] };
    const nextAfterConcurrentWrite = new Date(context.startedAt.getTime() + this.minCompactChunkIntervalMs);
    const nextCheckForUncompactedWork = {
      $min: [
        new Date(firstUncompactedWrite(context.state).getTime() + this.maxCompactFullIntervalMs),
        { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: this.minCompactChunkIntervalMs } }
      ]
    };
    const update: mongo.Document = {
      compacted_state: {
        op_id: compactedOpId,
        checksum: BigInt(compactionResult.compactedState.checksum),
        count: compactionResult.compactedState.count,
        bytes: compactionResult.compactedState.bytes,
        chunks: compactionResult.compactedState.chunks,
        at: '$$NOW'
      },
      bucket_stats: {
        count: { $add: ['$bucket_stats.count', delta.count] },
        bytes: { $add: ['$bucket_stats.bytes', delta.bytes] },
        chunks: { $add: ['$bucket_stats.chunks', delta.chunks] }
      },
      first_uncompacted_write:
        context.kind == CompactionKind.Full && coveredStart
          ? { $cond: [concurrentWriteCheck, context.startedAt, '$$REMOVE'] }
          : '$first_uncompacted_write',
      next_compact_check:
        context.kind == CompactionKind.Full && coveredStart
          ? { $cond: [concurrentWriteCheck, nextAfterConcurrentWrite, '$$REMOVE'] }
          : nextCheckForUncompactedWork
    };
    if (context.kind == CompactionKind.Full && coveredStart) {
      update.last_full_compact = {
        op_id: compactedOpId,
        count: compactionResult.bucketStats.count,
        puts,
        at: '$$NOW'
      };
    }

    await context.lease.finalize(update);
  }

  private async finalizeSkippedBucket(context: CompactionContext) {
    await this.rescheduleClaimedBucket(context.lease, context.decision);
  }

  private async readBucketStats(
    bucket: string,
    definitionId: BucketDefinitionId,
    maxOp: InternalOpId,
    lowerBound?: BucketDataKey
  ): Promise<BucketStatsWithChecksum> {
    const context = new BucketDataContextV3(this.db, {
      bucket,
      definitionId,
      replicationStreamId: this.group_id
    });
    const [stats] = await this.db
      .bucketData(this.group_id, definitionId)
      .aggregate<{ count: number; bytes: number | bigint; chunks: number; checksum: bigint }>([
        {
          $match: {
            _id:
              lowerBound == null
                ? { $gte: context.minId, $lte: context.docId(maxOp) }
                : { $gt: lowerBound, $lte: context.docId(maxOp) }
          }
        },
        {
          $group: {
            _id: null,
            count: { $sum: '$count' },
            bytes: { $sum: '$size' },
            chunks: { $sum: 1 },
            checksum: { $sum: '$checksum' }
          }
        }
      ])
      .toArray();
    return {
      count: Number(stats?.count ?? 0),
      bytes: BigInt(stats?.bytes ?? 0),
      chunks: Number(stats?.chunks ?? 0),
      checksum:
        typeof stats?.checksum == 'bigint'
          ? Number(BigInt.asIntN(32, stats.checksum))
          : addChecksums(0, Number(stats?.checksum ?? 0))
    };
  }

  private combineChunkStats(
    state: BucketStateDocumentV3,
    compactedTail: BucketStatsWithChecksum,
    overlappingCompactedChunk: BucketStatsWithChecksum | undefined
  ): BucketStatsWithChecksum {
    const previous = state.compacted_state;
    if (previous == null) {
      return compactedTail;
    }
    if (overlappingCompactedChunk == null) {
      throw new ReplicationAssertionError(`Missing previous compacted chunk for bucket ${state._id.b}`);
    }
    return {
      count: previous.count - overlappingCompactedChunk.count + compactedTail.count,
      bytes: previous.bytes - overlappingCompactedChunk.bytes + compactedTail.bytes,
      chunks: previous.chunks - 1 + compactedTail.chunks,
      checksum: addChecksums(
        addChecksums(Number(previous.checksum), -overlappingCompactedChunk.checksum),
        compactedTail.checksum
      )
    };
  }

  private combineAdjacentStats(
    first: BucketStatsWithChecksum,
    second: BucketStatsWithChecksum
  ): BucketStatsWithChecksum {
    return {
      count: first.count + second.count,
      bytes: first.bytes + second.bytes,
      chunks: first.chunks + second.chunks,
      checksum: addChecksums(first.checksum, second.checksum)
    };
  }

  private applyCompactionDelta(
    total: BucketStats,
    before: BucketStatsWithChecksum,
    after: BucketStatsWithChecksum
  ): BucketStats {
    return {
      count: total.count - before.count + after.count,
      bytes: total.bytes - before.bytes + after.bytes,
      chunks: total.chunks - before.chunks + after.chunks
    };
  }

  private async compactSingleBucketFully(context: CompactionContext) {
    const bucket = context.state._id.b;
    const resolvedDefinitionId = context.state._id.d;
    const bucketContext = new BucketDataContextV3(this.db, {
      bucket,
      definitionId: resolvedDefinitionId,
      replicationStreamId: this.group_id
    });
    const collection = this.db.bucketData(this.group_id, resolvedDefinitionId);
    const dataContext = { replicationStreamId: this.group_id, definitionId: resolvedDefinitionId };
    const lowerBound = bucketContext.minId;
    let upperBound = bucketContext.docId(this.compactMaxOpId(context) + 1n);

    let totalOpCount = 0;
    let preCompactionPrefix = emptyBucketStats();

    let lastNotPut: bigint | null = null;
    let opsSincePut = 0;
    let compactedOpId: bigint | null = null;
    let clearBoundary: { opId: bigint; documentId: BucketDataKey } | null = null;
    const seen = new Map<string, bigint>();
    let trackingSize = 0;
    let putCount = 0;
    let pendingGroup: PendingCompactionGroup | null = null;

    // --- Read batch from MongoDB ---
    while (true) {
      this.signal?.throwIfAborted();
      await context.lease.throwIfLost();

      const pipeline: mongo.Document[] = [
        {
          $match: {
            _id: {
              $gte: lowerBound,
              $lt: upperBound
            }
          }
        },
        { $sort: { _id: -1 } },
        { $limit: this.moveBatchQueryLimit },
        {
          $project: {
            _id: 1,
            min_op: 1,
            checksum: 1,
            count: 1,
            size: 1,
            target_op: 1,
            ops: 1,
            storage_ref: 1
          }
        }
      ];

      const batch = await readCompactionBatch(
        collection.aggregate<BucketDataDocumentV3>(pipeline, {
          batchSize: this.moveBatchQueryLimit + 1
        }),
        {
          byteLimit: this.moveBatchByteLimit,
          documentLimit: this.moveBatchQueryLimit
        }
      );
      const batchDocs = batch.documents;

      if (batchDocs.length == 0) {
        // No more documents in this bucket — compaction complete.
        break;
      }

      await hydrateBucketDataDocuments(batchDocs, this.storage.objectStorage, { signal: this.signal });

      // Compact each document independently, then greedily merge adjacent
      // post-compaction results. This preserves existing boundaries unless
      // merging is useful, and writes each final object at most once.
      for (const doc of batchDocs) {
        compactedOpId ??= doc._id.o;
        preCompactionPrefix = this.combineAdjacentStats(preCompactionPrefix, statsForDocument(doc));
        const originalOps = Array.from(loadBucketDataDocument(dataContext, doc));

        let changed = false;
        const compactedOps: BucketDataDoc[] = [];
        let maxTargetOp: InternalOpId | null = doc.target_op ?? null;
        for (let index = originalOps.length - 1; index >= 0; index--) {
          const op = originalOps[index];
          if (op.op == 'PUT' || op.op == 'REMOVE') {
            const key = `${op.table}/${op.row_id}/${cacheKey(op.source_table!, op.source_key!)}`;
            const targetOp = seen.get(key);
            if (targetOp != null) {
              maxTargetOp = maxOpId(maxTargetOp, targetOp);
              compactedOps.push({
                ...op,
                op: 'MOVE',
                table: undefined,
                row_id: undefined,
                source_table: undefined,
                source_key: undefined,
                data: null
              });
              changed = true;
              if (lastNotPut == null) {
                lastNotPut = op.o;
              }
              opsSincePut += 1;
            } else {
              if (trackingSize < this.idLimitBytes) {
                seen.set(utils.flatstr(key), op.o);
                trackingSize += key.length + 140;
              }
              compactedOps.push(op);
              if (op.op == 'PUT') {
                putCount++;
                lastNotPut = null;
                opsSincePut = 0;
              } else {
                if (lastNotPut == null) {
                  lastNotPut = op.o;
                }
                opsSincePut += 1;
              }
            }
          } else {
            compactedOps.push(op);
            if (op.op != 'CLEAR') {
              if (lastNotPut == null) {
                lastNotPut = op.o;
              }
              opsSincePut += 1;
            }
          }
        }
        compactedOps.reverse();

        totalOpCount += compactedOps.length;

        const candidate: PendingCompactionGroup = {
          inputs: [doc],
          ops: compactedOps,
          changed,
          targetOp: maxTargetOp
        };

        if (pendingGroup == null) {
          pendingGroup = candidate;
        } else {
          const mergedOps: BucketDataDoc[] = [...candidate.ops, ...pendingGroup.ops];
          const mergedSize = serializeBucketData(bucket, mergedOps, { targetOp: maxTargetOp }).size;
          if (mergedSize <= DEFAULT_MAX_DOC_SIZE_BYTES) {
            pendingGroup = {
              inputs: [...candidate.inputs, ...pendingGroup.inputs],
              ops: mergedOps,
              changed: candidate.changed || pendingGroup.changed,
              targetOp: maxOpId(maxTargetOp, pendingGroup.targetOp)
            };
          } else {
            const flushedGroup = pendingGroup;
            const documentId = await this.flushCompactionGroup(bucket, flushedGroup, bucketContext, dataContext);
            if (
              lastNotPut != null &&
              flushedGroup.ops[0].o <= lastNotPut &&
              flushedGroup.ops[flushedGroup.ops.length - 1].o >= lastNotPut
            ) {
              clearBoundary = { opId: lastNotPut, documentId };
            }
            pendingGroup = candidate;
          }
        }
      }

      // --- Advance to next batch ---
      upperBound = batchDocs[batchDocs.length - 1]._id as typeof upperBound;

      if (!batch.hasMore) {
        break;
      }

      this.logger.info(`Compacted batch of ${batchDocs.length} documents for bucket ${bucket}`);
    }

    if (pendingGroup != null) {
      const documentId = await this.flushCompactionGroup(bucket, pendingGroup, bucketContext, dataContext);
      if (
        lastNotPut != null &&
        pendingGroup.ops[0].o <= lastNotPut &&
        pendingGroup.ops[pendingGroup.ops.length - 1].o >= lastNotPut
      ) {
        clearBoundary = { opId: lastNotPut, documentId };
      }
    }
    if (compactedOpId == null) {
      await this.finalizeSkippedBucket(context);
      return;
    }

    // --- Clear: collapse leading MOVE/REMOVE/CLEAR sequence ---
    if (lastNotPut != null && opsSincePut >= 2) {
      if (clearBoundary == null || clearBoundary.opId != lastNotPut) {
        throw new ReplicationAssertionError(`Missing CLEAR boundary document for bucket ${bucket}`);
      }

      totalOpCount += await this.clearBucketLeading(
        lastNotPut,
        clearBoundary.documentId,
        bucketContext,
        collection,
        dataContext
      );
    }

    const compactedStats = await this.readBucketStats(bucket, resolvedDefinitionId, compactedOpId);
    const result = {
      compactedState: compactedStats,
      bucketStats: this.applyCompactionDelta(bucketStats(context.state), preCompactionPrefix, compactedStats)
    };

    // --- Finalize: update bucket checksums and state ---
    await this.finalizeCompactedBucket({ context, compactedOpId, compactionResult: result, puts: putCount });

    this.logger.info(`Compacted bucket ${bucket}: ${totalOpCount} surviving ops`);
  }

  /**
   * Persist replacement objects before starting the transaction, then atomically
   * publish their lifecycle markers alongside the MongoDB document replacement.
   * If verification or the transaction fails, the prepared markers retain enough
   * information for the uploaded objects to be cleaned up later.
   */
  private async flushCompactionGroup(
    bucket: string,
    group: PendingCompactionGroup,
    bucketContext: BucketDataContextV3,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<BucketDataKey> {
    if (group.inputs.length == 1 && !group.changed) {
      return group.inputs[0]._id;
    }

    const inputs = group.inputs;
    const idsToDelete = inputs.map((doc) => doc._id);
    const expectedDocCount = inputs.length;
    const expectedChecksum = inputs.reduce((sum, doc) => sum + doc.checksum, 0n);
    const expectedOpCount = inputs.reduce((sum, doc) => sum + doc.count, 0);
    const oldStoragePaths = inputs.flatMap((doc) => (doc.storage_ref ? [doc.storage_ref.path] : []));
    const {
      documents,
      storagePaths: newStoragePaths,
      uploads
    } = await this.persistBucketData(bucket, [group.ops], context, undefined, { targetOp: group.targetOp });
    const session = this.db.client.startSession();
    try {
      await session.withTransaction(
        async () => {
          const verification = await bucketContext.collection
            .aggregate<{ docCount: number; checksumSum: bigint | null; opCountSum: number | null }>(
              [
                { $match: { _id: { $in: idsToDelete } } },
                {
                  $group: {
                    _id: null,
                    docCount: { $sum: 1 },
                    checksumSum: { $sum: '$checksum' },
                    opCountSum: { $sum: '$count' }
                  }
                }
              ],
              { session }
            )
            .next();

          if (
            verification == null ||
            verification.docCount !== expectedDocCount ||
            verification.checksumSum !== expectedChecksum ||
            verification.opCountSum !== expectedOpCount
          ) {
            throw new ConcurrentCompactionError(
              `Inputs changed while compacting bucket ${bucket}; restarting from the latest bucket state`
            );
          }

          await bucketContext.collection.deleteMany({ _id: { $in: idsToDelete } }, { session });
          await bucketContext.collection.insertMany(documents, { session });
          await this.finishObjectStorageReplacement(oldStoragePaths, newStoragePaths, uploads, session);
        },
        {
          writeConcern: { w: 'majority' },
          readConcern: { level: 'snapshot' }
        }
      );
    } finally {
      await session.endSession();
    }
    return documents[0]._id;
  }

  /**
   * Collapse the leading sequence of MOVE/REMOVE/CLEAR ops at the start
   * of the bucket into a single CLEAR op. Reads whole clearable documents
   * before the known boundary document, then splits that boundary document
   * if it contains ops on both sides of lastNotPut.
   *
   * Returns the op count diff after replacing cleared ops with CLEAR ops.
   */
  private async clearBucketLeading(
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: BucketDataContextV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<number> {
    let opCountDiff = 0;
    const session = this.db.client.startSession();
    try {
      let done = false;
      // First step is to clear full chunks that contain only CLEAR/MOVE/REMOVE operations.
      // There can be many of them, so we do one batch at a time.
      while (!done) {
        const batch = await this.clearLeadingFullDocuments(
          session,
          lastNotPut,
          boundaryDocId,
          bucketContext,
          collection,
          context
        );
        done = batch.done;
        opCountDiff += batch.opCountDiff;
      }

      // The final step is to process the "boundary" document: It may contain some CLEAR/MOVE/REMOVE operations,
      // potentially followed by PUT operations. This is only a single document, so no need for batching.
      opCountDiff += await this.clearBoundaryDocument(
        session,
        lastNotPut,
        boundaryDocId,
        bucketContext,
        collection,
        context
      );
    } finally {
      await session.endSession();
    }

    return opCountDiff;
  }

  private async clearLeadingFullDocuments(
    session: mongo.ClientSession,
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: BucketDataContextV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<{ done: boolean; opCountDiff: number }> {
    const bucket = bucketContext.key.bucket;
    this.signal?.throwIfAborted();
    const prepared = await this.prepareCompactionUploads(bucket, context, [lastNotPut]);
    let done = false;
    let opCountDiff = 0;

    await session.withTransaction(
      async () => {
        done = false;
        opCountDiff = 0;
        const oldStoragePaths: string[] = [];
        const query = collection.find(
          {
            _id: {
              $gte: bucketContext.minId,
              $lt: boundaryDocId
            }
          },
          {
            session,
            sort: { _id: 1 },
            projection: {
              _id: 1,
              min_op: 1,
              checksum: 1,
              count: 1,
              target_op: 1,
              has_clear_op: 1,
              storage_ref: 1
            },
            limit: this.clearBatchLimit
          }
        );

        let combinedChecksum = 0;
        let clearedOpCount = 0;
        let maxTargetOp: bigint | null = null;
        let lastDocId: BucketDataKey | null = null;
        let clearOpCount = 0;
        let gotNonClearOp = false;

        for await (const doc of query.stream()) {
          if (doc.min_op > lastNotPut) {
            throw new ReplicationAssertionError(
              `Unexpected document before CLEAR boundary with min_op ${doc.min_op} > ${lastNotPut} in bucket ${bucket}`
            );
          }

          lastDocId = doc._id;
          if (doc.storage_ref) {
            oldStoragePaths.push(doc.storage_ref.path);
          }

          // The compaction scan established that every operation before the
          // boundary is MOVE/REMOVE/CLEAR. Root metadata is sufficient to fold
          // whole documents into one CLEAR, so avoid downloading their payloads.
          if (doc.has_clear_op) {
            clearOpCount++;
            if (clearOpCount > 1) {
              throw new ReplicationAssertionError(`Unexpected multiple CLEAR operations in bucket ${bucket}`);
            }
          }
          if (!doc.has_clear_op || doc.count > 1) {
            gotNonClearOp = true;
          }
          combinedChecksum = addChecksums(combinedChecksum, Number(doc.checksum));
          clearedOpCount += doc.count;
          if (doc.target_op != null && (maxTargetOp == null || doc.target_op > maxTargetOp)) {
            maxTargetOp = doc.target_op;
          }
        }

        if (!gotNonClearOp) {
          done = true;
          return;
        }

        this.logger.info(`Flushing CLEAR for ${clearedOpCount} ops at ${lastDocId?.o}`);
        await collection.deleteMany(
          {
            _id: {
              $gte: bucketContext.minId,
              $lte: lastDocId!
            }
          },
          { session }
        );

        const clearOp = {
          bucketKey: { ...context, bucket },
          o: lastDocId!.o,
          op: 'CLEAR' as const,
          checksum: BigInt(combinedChecksum),
          data: null
        } satisfies BucketDataDoc;
        const persisted = await this.persistBucketData(bucket, [[clearOp]], context, prepared, {
          targetOp: maxTargetOp
        });
        await collection.insertOne(persisted.documents[0], { session });
        await this.finishObjectStorageReplacement(oldStoragePaths, persisted.storagePaths, persisted.uploads, session);

        opCountDiff = -clearedOpCount + 1;
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    return { done, opCountDiff };
  }

  private async clearBoundaryDocument(
    session: mongo.ClientSession,
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: BucketDataContextV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<number> {
    const bucket = bucketContext.key.bucket;
    this.signal?.throwIfAborted();
    const prepared = await this.prepareCompactionUploads(bucket, context, [lastNotPut, boundaryDocId.o]);
    let opCountDiff = 0;

    await session.withTransaction(
      async () => {
        opCountDiff = 0;
        const oldStoragePaths: string[] = [];
        const query = collection.find(
          {
            // This is a range query, but should only ever return two documents:
            // 1. The CLEAR op from the previous clearLeadingFullDocuments.
            // 2. The boundary document.
            _id: {
              $gte: bucketContext.minId,
              $lte: boundaryDocId
            }
          },
          {
            session,
            sort: { _id: 1 },
            projection: {
              _id: 1,
              min_op: 1,
              checksum: 1,
              count: 1,
              target_op: 1,
              ops: 1,
              storage_ref: 1
            },
            limit: 3
          }
        );

        let docsRead = 0;
        let combinedChecksum = 0;
        let clearedOpCount = 0;
        let maxTargetOp: bigint | null = null;
        const boundarySurvivors: BucketDataDoc[] = [];

        for await (const doc of query.stream()) {
          docsRead++;
          if (docsRead > 2) {
            throw new ReplicationAssertionError(`Unexpected extra document before CLEAR boundary in bucket ${bucket}`);
          }

          const isBoundaryDoc = doc._id.o == boundaryDocId.o;
          if (doc.storage_ref) {
            oldStoragePaths.push(doc.storage_ref.path);
          }
          await hydrateBucketDataDocuments([doc], this.storage.objectStorage, { signal: this.signal });
          maxTargetOp = maxOpId(maxTargetOp, doc.target_op);
          for (const op of loadBucketDataDocument(context, doc)) {
            if (!isBoundaryDoc && op.op != 'CLEAR') {
              throw new ReplicationAssertionError(
                `Unexpected ${op.op} operation before CLEAR boundary in bucket ${bucket}`
              );
            }

            if (op.o <= lastNotPut) {
              if (op.op == 'PUT') {
                throw new ReplicationAssertionError(
                  `Unexpected PUT at op ${op.o} in CLEAR region for bucket ${bucket}`
                );
              }
              combinedChecksum = addChecksums(combinedChecksum, Number(op.checksum));
              clearedOpCount++;
            } else if (isBoundaryDoc) {
              boundarySurvivors.push(op);
            } else {
              throw new ReplicationAssertionError(
                `Unexpected op ${op.o} after CLEAR boundary ${lastNotPut} in bucket ${bucket}`
              );
            }
          }
        }

        if (clearedOpCount == 0) {
          throw new Error(`CLEAR boundary document not found for bucket ${bucket}`);
        }

        this.logger.info(`Flushing CLEAR for ${clearedOpCount} ops at ${lastNotPut}`);
        await collection.deleteMany(
          {
            _id: {
              $gte: bucketContext.minId,
              $lte: boundaryDocId
            }
          },
          { session }
        );

        const clearOp = {
          bucketKey: { ...context, bucket },
          o: lastNotPut,
          op: 'CLEAR' as const,
          checksum: BigInt(combinedChecksum),
          data: null
        } satisfies BucketDataDoc;
        const chunks: BucketDataDoc[][] = [[clearOp]];
        if (boundarySurvivors.length > 0) {
          // These operations are a subset of one existing document, so keeping
          // them together cannot increase its stored ops payload.
          chunks.push(boundarySurvivors);
        }
        const persisted = await this.persistBucketData(bucket, chunks, context, prepared, {
          targetOp: maxTargetOp ?? undefined
        });
        await collection.insertMany(persisted.documents, { session });
        await this.finishObjectStorageReplacement(oldStoragePaths, persisted.storagePaths, persisted.uploads, session);

        opCountDiff = -clearedOpCount + 1;
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    return opCountDiff;
  }

  /**
   * Reserve stable object paths before starting a retryable MongoDB transaction.
   * Each retry can safely overwrite the same paths, while the pre-existing
   * deletion markers remain visible to the transaction that publishes them.
   *
   * CLEAR compaction reserves its maximum output count. Unused markers remain
   * pending so they can clean up a path that an earlier transaction attempt may
   * have uploaded before retrying with fewer output documents.
   */
  private async prepareCompactionUploads(
    bucket: string,
    context: { replicationStreamId: number; definitionId: string },
    opIdHints: bigint[]
  ): Promise<PreparedObjectStorageUpload[]> {
    if (!this.storage.objectStorage) {
      return [];
    }

    const lifecycle = this.objectStorageLifecycle;
    const paths = opIdHints.map((opIdHint) => lifecycle.allocatePath(context.definitionId, bucket, opIdHint, opIdHint));
    return lifecycle.prepareUploads(paths);
  }

  /** Publish replacement uploads and retire superseded objects in the same transaction. */
  private async finishObjectStorageReplacement(
    oldStoragePaths: Iterable<string>,
    newStoragePaths: Set<string>,
    uploads: PreparedObjectStorageUpload[],
    session: mongo.ClientSession
  ): Promise<void> {
    if (!this.storage.objectStorage) {
      return;
    }
    await this.objectStorageLifecycle.publishUploads(uploads, session);
    await this.objectStorageLifecycle.retire(
      Array.from(oldStoragePaths).filter((path) => !newStoragePaths.has(path)),
      session
    );
  }

  private async persistBucketData(
    bucket: string,
    chunks: BucketDataDoc[][],
    context: { replicationStreamId: number; definitionId: string },
    preparedUploads?: PreparedObjectStorageUpload[],
    options?: { targetOp?: InternalOpId | null }
  ): Promise<{ documents: BucketDataDocumentV3[]; storagePaths: Set<string>; uploads: PreparedObjectStorageUpload[] }> {
    const serializedChunks = chunks.map((chunk) => serializeBucketData(bucket, chunk, options));
    if (!this.storage.objectStorage) {
      return {
        documents: serializedChunks,
        storagePaths: new Set(),
        uploads: []
      };
    }

    const store = new BucketDataObjectStorage(this.storage.objectStorage);
    const storagePaths = new Set<string>();
    const lifecycle = this.objectStorageLifecycle;
    // Base placement on the final compacted size. Unchanged documents are not
    // rewritten, while small MOVE/merge results and CLEAR ops stay inline.
    const storedIndexes = serializedChunks.flatMap((document, index) =>
      document.size > this.storage.inlineThresholdBytes ? [index] : []
    );
    const uploadsByIndex = new Map<number, PreparedObjectStorageUpload>();

    if (preparedUploads) {
      for (const index of storedIndexes) {
        const upload = preparedUploads[index];
        if (!upload) {
          throw new ServiceAssertionError(
            `Missing prepared object storage path for compacted document at index ${index}`
          );
        }
        uploadsByIndex.set(index, upload);
      }
    } else {
      const paths = storedIndexes.map((index) => {
        const chunk = chunks[index];
        return lifecycle.allocatePath(context.definitionId, bucket, chunk[0].o, chunk[chunk.length - 1].o);
      });
      const prepared = await lifecycle.prepareUploads(paths);
      storedIndexes.forEach((index, preparedIndex) => uploadsByIndex.set(index, prepared[preparedIndex]));
    }

    // S3ObjectStorage applies one shared concurrency limit across all callers,
    // so compaction can schedule its uploads together without creating a
    // separate limiter here.
    const documents = await Promise.all(
      serializedChunks.map(async (serialized, index) => {
        const upload = uploadsByIndex.get(index);
        if (!upload) {
          return serialized;
        }

        const { ops, ...metadata } = serialized;
        const { fileSize } = await store.store(upload.path, ops!);
        storagePaths.add(upload.path);
        return {
          ...metadata,
          storage_ref: { path: upload.path, file_size: fileSize }
        };
      })
    );

    return { documents, storagePaths, uploads: Array.from(uploadsByIndex.values()) };
  }
}
