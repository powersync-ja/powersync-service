import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, InternalOpId } from '@powersync/service-core';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { AVAILABLE_LEASE_EXPR, CompactionLease } from './CompactionLease.js';
import { BucketDataDocumentV3, BucketStateDocumentV3 } from './models.js';

/**
 * Perform chunk compaction if at least this many chunks have been added since
 * the latest compaction.
 *
 * A lower value increases compaction frequency and can increase chunk rewrites
 * and object-storage operations. A higher value leaves more small chunks for
 * longer, which can hurt sync performance.
 */
const MERGE_CHUNKS_THRESHOLD = 8;

export const FULL_COMPACT_RESCHEDULE_MARGIN_MS = 60 * 1000;

export interface PendingCompactionGroup {
  /**
   * Input documents are ordered from oldest to newest, matching `ops`.
   * Keeping the inputs intact lets unchanged singletons retain their object.
   */
  inputs: BucketDataDocumentV3[];
  ops: BucketDataDoc[];
  changed: boolean;
  targetOp: InternalOpId | null;
}

export interface CompactIntervalConfig {
  readonly minCompactChunkIntervalMs: number;
  readonly minCompactFullIntervalMs: number;
  readonly maxCompactFullIntervalMs: number;
}
export interface CompactTargetConfig {
  readonly maxOpIdCap: InternalOpId | undefined;
}

export enum CompactionKind {
  Full = 'full',
  Chunks = 'chunks'
}

export interface CompactionDecision {
  kind: CompactionKind | null;
  nextCompactCheck: mongo.Document;
}

export interface ScheduledCompactionOptions {
  /** Process checks scheduled this far after the captured job start. */
  dueAheadMs?: number;
  /** Used by initial replication, which must not run a full compact. */
  forceKind?: CompactionKind;
}

export class CompactionContext {
  constructor(
    readonly lease: CompactionLease,
    readonly kind: CompactionKind,
    readonly decision: CompactionDecision,
    readonly rescheduleNotBefore: Date | undefined
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

export interface BucketStats {
  count: number;
  bytes: bigint;
  chunks: number;
}

/** Bucket stats read from bucket-data documents, including their checksum. */
export interface BucketStatsWithChecksum extends BucketStats {
  checksum: number;
}

export interface CompactionResult {
  /** Metadata cached at compacted_state.op_id. */
  compactedState: BucketStatsWithChecksum;
  /** Complete bucket metadata through the op head captured at claim time. */
  bucketStats: BucketStats;
}

export function bucketStats(state: BucketStateDocumentV3): BucketStats {
  return {
    count: state.bucket_stats.count,
    bytes: state.bucket_stats.bytes,
    chunks: state.bucket_stats.chunks
  };
}

export function emptyBucketStats(): BucketStatsWithChecksum {
  return { count: 0, bytes: 0n, chunks: 0, checksum: 0 };
}

export function statsForDocument(
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
export function firstUncompactedWrite(state: BucketStateDocumentV3): Date {
  if (state.first_uncompacted_write == null) {
    throw new ReplicationAssertionError(`Scheduled V3 bucket ${state._id.b} has no first uncompacted write`);
  }
  return state.first_uncompacted_write;
}

export function combineChunkStats(
  previous: NonNullable<BucketStateDocumentV3['compacted_state']>,
  compactedTail: BucketStatsWithChecksum,
  overlappingCompactedChunk: BucketStatsWithChecksum
): BucketStatsWithChecksum {
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

export function combineAdjacentStats(
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

export function applyCompactionDelta(
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

/**
 * This checks that the bucket state hasn't changed by a concurrent write since we checked.
 *
 * If it has changed, we'll re-check in the next batch.
 */
export function unclaimedSnapshotFilter(state: BucketStateDocumentV3): mongo.Filter<BucketStateDocumentV3> {
  return {
    _id: state._id,
    last_op: state.last_op,
    next_compact_check: state.next_compact_check,
    first_uncompacted_write: state.first_uncompacted_write ?? { $exists: false },
    bucket_stats: state.bucket_stats,
    compacted_state: state.compacted_state ?? { $exists: false },
    last_full_compact: state.last_full_compact ?? { $exists: false },
    ...AVAILABLE_LEASE_EXPR
  };
}

/**
 * Calculate the earliest full compaction time from the first uncompacted
 * write, bounded by the maximum retention interval.
 */
export function fullCompactionCheckAt(state: BucketStateDocumentV3, config: CompactIntervalConfig): Date {
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
  const fullIntervalMs = ratio == 0 ? config.maxCompactFullIntervalMs : config.minCompactFullIntervalMs / ratio;
  return new Date(firstWrite.getTime() + Math.min(fullIntervalMs, config.maxCompactFullIntervalMs));
}

export function chooseCompactionKind(
  state: BucketStateDocumentV3,
  now: Date,
  config: CompactIntervalConfig
): CompactionDecision {
  // For chunk compaction, we consider the number of chunks added.
  // Right now, we trigger a compact if the interval has passed and at least a threshold of chunks were added.
  // A future policy could also use bytes per chunk or records per chunk to
  // decide when to compact.
  const fullCheckAt = fullCompactionCheckAt(state, config);
  // Schedule a little late so a worker using a slightly earlier clock does
  // not wake before the exact full-compaction condition is true.
  const fullCheckWithMargin = new Date(fullCheckAt.getTime() + FULL_COMPACT_RESCHEDULE_MARGIN_MS);
  const compacted = state.compacted_state;
  const chunksSinceCompact = Math.max(0, state.bucket_stats.chunks - (compacted?.chunks ?? 0));
  const shouldCompactChunks = chunksSinceCompact >= MERGE_CHUNKS_THRESHOLD;
  const canCheckChunks =
    compacted == null || now.getTime() - compacted.at.getTime() >= config.minCompactChunkIntervalMs;
  // Too few new chunks cannot make chunk compaction eligible. Do not poll this
  // bucket at the chunk-compaction interval; only wake it for its full-compact check.
  const nextCompactCheck: mongo.Document = !shouldCompactChunks
    ? fullCheckWithMargin
    : {
        $min: [
          fullCheckWithMargin,
          { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: config.minCompactChunkIntervalMs } }
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

export function forcedCompactionKind(
  state: BucketStateDocumentV3,
  forceKind: CompactionKind | undefined,
  config: CompactTargetConfig
): CompactionKind | null {
  if (forceKind == null) {
    return null;
  }
  const maxOpId = config.maxOpIdCap == null || state.last_op < config.maxOpIdCap ? state.last_op : config.maxOpIdCap;
  return state.compacted_state == null || state.compacted_state.op_id < maxOpId ? forceKind : null;
}

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
export async function readCompactionBatch(
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
