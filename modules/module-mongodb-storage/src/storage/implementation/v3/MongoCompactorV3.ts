import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, formatBytes, InternalOpId, storage, utils } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { BucketDataKey } from '../models.js';
import { ConcurrentCompactionError, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import { loadBucketDataDocument, maxOpId, serializeBucketData } from './bucket-format.js';
import { BucketDataContextV3 } from './BucketDataContextV3.js';
import { DEFAULT_MAX_DOC_SIZE_BYTES } from './chunking.js';
import {
  applyStatsReplacement,
  bucketStats,
  BucketStatsWithChecksum,
  chooseCompactionKind,
  combineAdjacentStats,
  combineChunkStats,
  CompactIntervalConfig,
  CompactionContext,
  CompactionDecision,
  CompactionKind,
  CompactionResult,
  CompactTargetConfig,
  emptyBucketStats,
  firstUncompactedWrite,
  forcedCompactionKind,
  PendingCompactionGroup,
  readCompactionBatch,
  ScheduledCompactionOptions,
  statsForDocument,
  statsForDocuments,
  unclaimedSnapshotFilter
} from './compact-utils.js';
import { DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS } from './compaction-constants.js';
import { AVAILABLE_LEASE_EXPR, CompactionLease } from './CompactionLease.js';
import { BucketDataDocumentV3, BucketStateDocumentV3 } from './models.js';
import type { MongoSyncBucketStorageV3 } from './MongoSyncBucketStorageV3.js';
import { BucketDataObjectStorage, hydrateBucketDataDocuments } from './object-storage/BucketDataObjectStorage.js';
import { ObjectStorageLifecycle, PreparedObjectStorageUpload } from './object-storage/ObjectStorageLifecycle.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

const DEFAULT_MIN_COMPACT_FULL_INTERVAL_MS = 2 * 60 * 60 * 1000;
const DEFAULT_MAX_COMPACT_FULL_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_COMPACT_LEASE_DURATION_MS = 10 * 60 * 1000;
const SCHEDULED_COMPACTION_BATCH_SIZE = 100;

interface CompactionGroupResult {
  documentId: BucketDataKey;
  stats: BucketStatsWithChecksum;
}

interface CompactionStatsReplacement {
  before: BucketStatsWithChecksum;
  after: BucketStatsWithChecksum;
}

interface ClearCompactionResult extends CompactionStatsReplacement {
  opCountDiff: number;
}

export class MongoCompactorV3 extends MongoCompactor implements CompactIntervalConfig, CompactTargetConfig {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  declare protected readonly storage: MongoSyncBucketStorageV3;

  readonly minCompactChunkIntervalMs: number;
  readonly minCompactFullIntervalMs: number;
  readonly maxCompactFullIntervalMs: number;
  readonly compactLeaseDurationMs: number;
  readonly maxOpIdCap: InternalOpId | undefined;

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
      // Writers defer their first chunk-compaction check by this fixed default.
      // Include that interval so this synchronous initial-replication pass
      // processes the work that existed when it started.
      await this.compactScheduledBuckets({
        dueAheadMs: DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS,
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
        if (this.isCompactionTargetCovered(lease.state, CompactionKind.Full)) {
          continue;
        }
        const decision = chooseCompactionKind(lease.state, lease.startedAt, this);
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
   * multiple times in one run. Reschedules fall beyond the fixed boundary. For the run after
   * initial replication, dueAheadMs extends that boundary to include the first deferred interval.
   */
  private async compactScheduledBuckets(options: ScheduledCompactionOptions = {}) {
    // Writers derive next_compact_check from MongoDB's $$NOW. Use the same
    // clock for the fixed job boundary so clock skew cannot exclude work at
    // the exact initial-replication interval.
    const [{ now: jobStartedAt }] = await this.db.db
      .aggregate<{ now: Date }>([{ $documents: [{}] }, { $project: { _id: 0, now: '$$NOW' } }])
      .toArray();
    const dueBefore = new Date(jobStartedAt.getTime() + (options.dueAheadMs ?? 0));
    const forceKind = options.forceKind;
    const rescheduleNotBefore = new Date(dueBefore.getTime() + 1);
    while (true) {
      this.signal?.throwIfAborted();
      const states = await this.findScheduledBucketBatch(dueBefore);
      if (states.length == 0) {
        break;
      }

      const scheduled: {
        state: BucketStateDocumentV3;
        decision: CompactionDecision;
        forcedKind: CompactionKind | null;
      }[] = [];
      for (const state of states) {
        try {
          scheduled.push({
            state,
            decision: chooseCompactionKind(state, jobStartedAt, this),
            forcedKind: forcedCompactionKind(state, forceKind, this)
          });
        } catch (error) {
          await this.rescheduleFailedBucket(state, rescheduleNotBefore, error);
        }
      }
      const noOpStates = scheduled.filter(
        ({ state, decision, forcedKind }) =>
          state.compact_lease == null && (forceKind == null ? decision.kind : forcedKind) == null
      );
      await this.rescheduleUnclaimedBuckets(noOpStates, rescheduleNotBefore);

      for (const { state, decision, forcedKind } of scheduled) {
        const kind = forceKind == null ? decision.kind : forcedKind;
        if (state.compact_lease == null && kind == null) {
          continue;
        }

        try {
          await using lease = await this.claimBucket({ _id: state._id, next_compact_check: { $lte: dueBefore } });
          if (lease == null) {
            continue;
          }
          const claimedDecision = chooseCompactionKind(lease.state, lease.startedAt, this);
          const claimedKind =
            forceKind == null ? claimedDecision.kind : forcedCompactionKind(lease.state, forceKind, this);
          if (claimedKind == null) {
            await this.rescheduleClaimedBucket(lease, claimedDecision, rescheduleNotBefore);
          } else if (this.isCompactionTargetCovered(lease.state, claimedKind)) {
            // The run cannot advance this kind's watermark without regressing
            // already-published progress. Keep any newer work scheduled.
            await this.rescheduleClaimedBucket(lease, claimedDecision, rescheduleNotBefore);
          } else {
            await this.compactClaimedBucket(lease, claimedKind, claimedDecision, rescheduleNotBefore);
          }
        } catch (error) {
          if (this.signal?.aborted) {
            // When aborted, stop completely, rather than logging and re-scheduling individual buckets.
            // The lease on the current bucket is still released automatically.
            throw error;
          }
          await this.rescheduleFailedBucket(state, rescheduleNotBefore, error);
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
        ...AVAILABLE_LEASE_EXPR
      })
      .sort({ next_compact_check: 1 })
      .limit(SCHEDULED_COMPACTION_BATCH_SIZE)
      .toArray();
  }

  /**
   * Reschedule snapshots that were already known to be no-ops without first
   * taking a lease. Every decision input is compared so a concurrent writer
   * or compactor simply makes the update a no-op instead of losing work. A
   * successful reschedule moves beyond this run's fixed selection boundary.
   */
  private async rescheduleUnclaimedBuckets(
    states: { state: BucketStateDocumentV3; decision: CompactionDecision }[],
    notBefore: Date
  ) {
    if (states.length == 0) {
      return;
    }
    await this.db.bucketState(this.group_id).bulkWrite(
      states.map(({ state, decision }) => ({
        updateOne: {
          filter: unclaimedSnapshotFilter(state),
          update: [{ $set: { next_compact_check: this.rescheduleAtOrAfter(decision.nextCompactCheck, notBefore) } }]
        }
      })),
      { ordered: false }
    );
  }

  /**
   * Isolate a malformed bucket so it cannot prevent other scheduled buckets
   * from compacting. The snapshot filter preserves any concurrent write or
   * compactor result instead of overwriting its next check.
   */
  private async rescheduleFailedBucket(state: BucketStateDocumentV3, notBefore: Date, error: unknown) {
    this.logger.error(`Failed to compact scheduled bucket ${state._id.b}; rescheduling it`, error);
    try {
      await this.db
        .bucketState(this.group_id)
        .updateOne(unclaimedSnapshotFilter(state), [{ $set: { next_compact_check: notBefore } }]);
    } catch (rescheduleError) {
      this.logger.error(`Failed to reschedule bucket ${state._id.b} after a compaction error`, rescheduleError);
    }
  }

  /**
   * Given a bucket filter, claim a lease on the bucket. The filter should include a filter on _id.
   *
   * Resolves to null if the bucket is already claimed, not found, or filtered out.
   */
  private async claimBucket(
    filter: mongo.Filter<BucketStateDocumentV3>,
    sort?: mongo.Sort
  ): Promise<CompactionLease | null> {
    return CompactionLease.claim(this.db.bucketState(this.group_id), filter, sort, this.compactLeaseDurationMs);
  }

  private async compactClaimedBucket(
    lease: CompactionLease,
    kind: CompactionKind,
    decision: CompactionDecision,
    rescheduleNotBefore?: Date
  ) {
    const context = new CompactionContext(
      lease,
      kind,
      decision,
      rescheduleNotBefore,
      this.compactionTarget(lease.state)
    );
    lease.startRenewal();
    await this.compactSingleBucket(context);
  }

  private async rescheduleClaimedBucket(lease: CompactionLease, decision: CompactionDecision, notBefore?: Date) {
    await lease.reschedule(this.rescheduleAtOrAfter(decision.nextCompactCheck, notBefore));
  }

  private rescheduleAtOrAfter(nextCompactCheck: mongo.Document, notBefore: Date | undefined): mongo.Document {
    return notBefore == null ? nextCompactCheck : { $max: [nextCompactCheck, notBefore] };
  }

  private compactionTarget(state: BucketStateDocumentV3): InternalOpId {
    return this.maxOpIdCap == null || state.last_op < this.maxOpIdCap ? state.last_op : this.maxOpIdCap;
  }

  private isCompactionTargetCovered(state: BucketStateDocumentV3, kind: CompactionKind): boolean {
    const target = this.compactionTarget(state);
    if (kind == CompactionKind.Chunks) {
      return state.compacted_state != null && state.compacted_state.op_id >= target;
    }
    if (state.last_full_compact != null && state.last_full_compact.op_id >= target) {
      return true;
    }
    // A full compact may change counts before the checksum-cache boundary.
    // Wait for the safe target to catch up instead of publishing an older or
    // stale cache. At the same boundary, full coverage can still advance.
    return state.compacted_state != null && state.compacted_state.op_id > target;
  }

  private get objectStorageLifecycle(): ObjectStorageLifecycle {
    if (!this.storage.objectStorage) {
      throw new Error('Object storage is not configured');
    }
    return new ObjectStorageLifecycle(this.db, this.group_id, this.storage.objectStorage);
  }

  private async compactSingleBucket(context: CompactionContext) {
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
    let previousCompactedState = context.state.compacted_state;
    // A zero boundary represents an empty prefix, so there is no stored chunk
    // whose statistics need to be carried into this pass.
    if (previousCompactedState?.op_id === 0n) {
      previousCompactedState = undefined;
    }
    // Include the last previously compacted chunk as well as new chunks. It
    // is the only old chunk which can become mergeable with the new tail.
    let lowerBound =
      previousCompactedState != null ? bucketContext.docId(previousCompactedState.op_id - 1n) : bucketContext.minId;
    const upperBound = bucketContext.docId(context.targetOp + 1n);
    let cachedBoundaryToVerify = previousCompactedState?.op_id;

    let compactedOpId: bigint | null = null;
    let overlappingCompactedChunk: BucketStatsWithChecksum | undefined;
    let compactedTail = emptyBucketStats();
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
                storage_ref: 1
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

      if (cachedBoundaryToVerify != null) {
        const cachedBoundary = cachedBoundaryToVerify;
        cachedBoundaryToVerify = undefined;
        if (batch.documents[0]?._id.o !== cachedBoundary) {
          // A previous attempt may have replaced the cached boundary before
          // finalizing bucket state. Keep the persisted cache available to
          // readers, but ignore it in this attempt and calculate its
          // replacement through the normal scan from the bucket beginning.
          previousCompactedState = undefined;
          lowerBound = bucketContext.minId;
          continue;
        }
      }

      if (batch.documents.length == 0) {
        break;
      }

      for (const doc of batch.documents) {
        compactedOpId = maxOpId(compactedOpId, doc._id.o);
        const documentStats = statsForDocument(doc);
        if (previousCompactedState?.op_id === doc._id.o) {
          overlappingCompactedChunk = documentStats;
        }

        const nextSize = pendingSize + doc.size;
        if (pendingChunks.length > 0 && nextSize > DEFAULT_MAX_DOC_SIZE_BYTES) {
          const groupStats = await this.flushChunkMerge(bucket, pendingChunks, collection, dataContext, bucketContext);
          compactedTail = combineAdjacentStats(compactedTail, groupStats);
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

    if (pendingChunks.length > 0) {
      const groupStats = await this.flushChunkMerge(bucket, pendingChunks, collection, dataContext, bucketContext);
      compactedTail = combineAdjacentStats(compactedTail, groupStats);
    }

    if (compactedOpId == null) {
      await this.finalizeSkippedBucket(context);
      return;
    }

    const compactedState =
      previousCompactedState == null
        ? compactedTail
        : combineChunkStats(previousCompactedState, compactedTail, overlappingCompactedChunk!);
    const tailStats =
      compactedOpId == context.lastOp
        ? undefined
        : await this.readBucketStats(bucket, resolvedDefinitionId, context.lastOp, bucketContext.docId(compactedOpId));
    const result: CompactionResult = {
      compactedState,
      bucketStats: tailStats == null ? compactedState : combineAdjacentStats(compactedState, tailStats)
    };

    await this.finalizeCompactedBucket({ context, compactedOpId, compactionResult: result, puts: 0 });
    this.compactedBucketCount++;
    this.logger.info(
      `Compacted bucket chunks ${bucket}: ${result.bucketStats.count} ops, ${result.bucketStats.chunks} chunks, ${formatBytes(result.bucketStats.bytes)}`
    );
  }

  private async flushChunkMerge(
    bucket: string,
    inputs: BucketDataDocumentV3[],
    collection: mongo.Collection<BucketDataDocumentV3>,
    context: { replicationStreamId: number; definitionId: string },
    bucketContext: BucketDataContextV3
  ): Promise<BucketStatsWithChecksum> {
    if (inputs.length == 1) {
      return statsForDocument(inputs[0]);
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
    const result = await this.flushCompactionGroup(
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
    return result.stats;
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
    const coveredClaimedHead = compactedOpId >= context.lastOp;
    const concurrentWriteCheck = { $gt: ['$last_op', context.lastOp] };
    const remainingFullWorkCheck = coveredClaimedHead ? concurrentWriteCheck : true;
    const nextAfterPartialFullCompact = this.rescheduleAtOrAfter(
      { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: this.minCompactChunkIntervalMs } },
      context.rescheduleNotBefore
    );
    const nextCheckForUncompactedWork = this.rescheduleAtOrAfter(
      {
        $min: [
          new Date(firstUncompactedWrite(context.state).getTime() + this.maxCompactFullIntervalMs),
          { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: this.minCompactChunkIntervalMs } }
        ]
      },
      context.rescheduleNotBefore
    );
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
        context.kind == CompactionKind.Full
          ? { $cond: [remainingFullWorkCheck, '$$NOW', '$$REMOVE'] }
          : '$first_uncompacted_write',
      next_compact_check:
        context.kind == CompactionKind.Full
          ? { $cond: [remainingFullWorkCheck, nextAfterPartialFullCompact, '$$REMOVE'] }
          : nextCheckForUncompactedWork
    };
    if (context.kind == CompactionKind.Full) {
      update.last_full_compact = {
        op_id: compactedOpId,
        count: compactionResult.compactedState.count,
        puts,
        at: '$$NOW'
      };
    }

    await context.lease.finalize(update);
  }

  private async finalizeSkippedBucket(context: CompactionContext) {
    // A maxOpId cap can exclude the first remaining document entirely. Avoid
    // immediately claiming the same no-progress bucket again in this run.
    await this.rescheduleClaimedBucket(
      context.lease,
      {
        ...context.decision,
        nextCompactCheck: {
          $max: [
            context.decision.nextCompactCheck,
            { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: this.minCompactChunkIntervalMs } }
          ]
        }
      },
      context.rescheduleNotBefore
    );
  }

  /**
   * Read bucket stats directly from bucket_data documents.
   */
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
    let upperBound = bucketContext.docId(context.targetOp + 1n);

    let totalOpCount = 0;

    let lastNotPut: bigint | null = null;
    let opsSincePut = 0;
    let compactedOpId: bigint | null = null;
    let clearBoundary: { opId: bigint; documentId: BucketDataKey } | null = null;
    let compactedStats = emptyBucketStats();
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
                subkey: undefined,
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
            const result = await this.flushCompactionGroup(bucket, flushedGroup, bucketContext, dataContext);
            compactedStats = combineAdjacentStats(compactedStats, result.stats);
            if (
              lastNotPut != null &&
              flushedGroup.ops[0].o <= lastNotPut &&
              flushedGroup.ops[flushedGroup.ops.length - 1].o >= lastNotPut
            ) {
              clearBoundary = { opId: lastNotPut, documentId: result.documentId };
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
      const result = await this.flushCompactionGroup(bucket, pendingGroup, bucketContext, dataContext);
      compactedStats = combineAdjacentStats(compactedStats, result.stats);
      if (
        lastNotPut != null &&
        pendingGroup.ops[0].o <= lastNotPut &&
        pendingGroup.ops[pendingGroup.ops.length - 1].o >= lastNotPut
      ) {
        clearBoundary = { opId: lastNotPut, documentId: result.documentId };
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

      const clearResult = await this.clearBucketLeading(
        lastNotPut,
        clearBoundary.documentId,
        bucketContext,
        collection,
        dataContext
      );
      totalOpCount += clearResult.opCountDiff;
      compactedStats = applyStatsReplacement(compactedStats, clearResult.before, clearResult.after);
    }

    const tailStats =
      compactedOpId == context.lastOp
        ? undefined
        : await this.readBucketStats(bucket, resolvedDefinitionId, context.lastOp, bucketContext.docId(compactedOpId));
    const result: CompactionResult = {
      compactedState: compactedStats,
      bucketStats: tailStats == null ? compactedStats : combineAdjacentStats(compactedStats, tailStats)
    };

    // --- Finalize: update bucket checksums and state ---
    await this.finalizeCompactedBucket({ context, compactedOpId, compactionResult: result, puts: putCount });

    this.compactedBucketCount++;
    this.logger.info(
      `Compacted bucket ${bucket}: ${totalOpCount} ops, ${result.bucketStats.chunks} chunks, ${formatBytes(result.bucketStats.bytes)}`
    );
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
  ): Promise<CompactionGroupResult> {
    if (group.inputs.length == 1 && !group.changed) {
      return {
        documentId: group.inputs[0]._id,
        stats: statsForDocument(group.inputs[0])
      };
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
    return {
      documentId: documents[0]._id,
      stats: statsForDocuments(documents)
    };
  }

  /**
   * Collapse the leading sequence of MOVE/REMOVE/CLEAR ops at the start
   * of the bucket into a single CLEAR op. Reads whole clearable documents
   * before the known boundary document, then splits that boundary document
   * if it contains ops on both sides of lastNotPut.
   *
   * Returns the op count and stored-stat changes after replacing cleared ops
   * with CLEAR ops.
   */
  private async clearBucketLeading(
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: BucketDataContextV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<ClearCompactionResult> {
    let opCountDiff = 0;
    let before = emptyBucketStats();
    let after = emptyBucketStats();
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
        before = combineAdjacentStats(before, batch.before);
        after = combineAdjacentStats(after, batch.after);
      }

      // The final step is to process the "boundary" document: It may contain some CLEAR/MOVE/REMOVE operations,
      // potentially followed by PUT operations. This is only a single document, so no need for batching.
      const boundaryResult = await this.clearBoundaryDocument(
        session,
        lastNotPut,
        boundaryDocId,
        bucketContext,
        collection,
        context
      );
      opCountDiff += boundaryResult.opCountDiff;
      before = combineAdjacentStats(before, boundaryResult.before);
      after = combineAdjacentStats(after, boundaryResult.after);
    } finally {
      await session.endSession();
    }

    return { opCountDiff, before, after };
  }

  private async clearLeadingFullDocuments(
    session: mongo.ClientSession,
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: BucketDataContextV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<{ done: boolean; opCountDiff: number } & CompactionStatsReplacement> {
    const bucket = bucketContext.key.bucket;
    this.signal?.throwIfAborted();
    let prepared: PreparedObjectStorageUpload[] | undefined;
    let done = false;
    let opCountDiff = 0;
    let before = emptyBucketStats();
    let after = emptyBucketStats();

    await session.withTransaction(
      async () => {
        done = false;
        opCountDiff = 0;
        before = emptyBucketStats();
        after = emptyBucketStats();
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
              size: 1,
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
        const inputStats = emptyBucketStats();

        for await (const doc of query.stream()) {
          if (doc.min_op > lastNotPut) {
            throw new ReplicationAssertionError(
              `Unexpected document before CLEAR boundary with min_op ${doc.min_op} > ${lastNotPut} in bucket ${bucket}`
            );
          }

          lastDocId = doc._id;
          const documentStats = statsForDocument(doc);
          inputStats.count += documentStats.count;
          inputStats.bytes += documentStats.bytes;
          inputStats.chunks += documentStats.chunks;
          inputStats.checksum = addChecksums(inputStats.checksum, documentStats.checksum);
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

        prepared ??= await this.prepareCompactionUploads(bucket, context, [lastNotPut]);
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
        before = inputStats;
        after = statsForDocuments(persisted.documents);
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    return { done, opCountDiff, before, after };
  }

  private async clearBoundaryDocument(
    session: mongo.ClientSession,
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: BucketDataContextV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<ClearCompactionResult> {
    const bucket = bucketContext.key.bucket;
    this.signal?.throwIfAborted();
    const prepared = await this.prepareCompactionUploads(bucket, context, [lastNotPut, boundaryDocId.o]);
    let opCountDiff = 0;
    let before = emptyBucketStats();
    let after = emptyBucketStats();

    await session.withTransaction(
      async () => {
        opCountDiff = 0;
        before = emptyBucketStats();
        after = emptyBucketStats();
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
              size: 1,
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
        const inputStats = emptyBucketStats();

        for await (const doc of query.stream()) {
          docsRead++;
          if (docsRead > 2) {
            throw new ReplicationAssertionError(`Unexpected extra document before CLEAR boundary in bucket ${bucket}`);
          }

          const documentStats = statsForDocument(doc);
          inputStats.count += documentStats.count;
          inputStats.bytes += documentStats.bytes;
          inputStats.chunks += documentStats.chunks;
          inputStats.checksum = addChecksums(inputStats.checksum, documentStats.checksum);

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
        before = inputStats;
        after = statsForDocuments(persisted.documents);
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    return { opCountDiff, before, after };
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
