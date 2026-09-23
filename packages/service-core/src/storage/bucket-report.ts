/**
 * Per-bucket storage report for an active sync config.
 *
 * - An **operation** is any entry in a bucket's append-only history (`PUT`, `REMOVE`, `MOVE`, `CLEAR`).
 * - A **row** is a distinct live object currently in the bucket.
 *
 * A new client downloads every operation, not just live rows, so `operations / rows` is effectively a
 * fragmentation / compaction-efficiency score: a fully compacted bucket trends towards ~1, while a high
 * ratio is the usual cause of an unexpectedly high "Data Synced" metric and is reclaimable via compact/defragment.
 *
 * Scaling note: the report reads only the pre-aggregated per-bucket state (`bucket_state`), never the
 * operation history itself. Operation counts are exact; row counts come from the statistics captured by the
 * last full compact of each bucket, so they are a snapshot as of that compact rather than a live counter.
 * Storage versions that do not capture compact statistics (v1/v2) report operations only, with row-derived
 * fields null and the suggested action `unknown`.
 */
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';

/** Number of worst-offender buckets returned when the request omits a `limit`. */
export const DEFAULT_BUCKET_REPORT_LIMIT: number = 50;

/** Highest `limit` a request may ask for, bounding the response size. */
export const MAX_BUCKET_REPORT_LIMIT: number = 1_000;

/**
 * Maximum number of bucket definitions in the report's definition rollup. Configs rarely approach this many
 * definitions.
 */
export const BUCKET_REPORT_DEFINITION_LIMIT: number = 20;

/** Fragmentation below this is considered healthy: no maintenance action is suggested. */
export const BUCKET_ACTION_MIN_FRAGMENTATION: number = 3;

/**
 * When at least this share of a bucket's operations is compaction residue (MOVE/CLEAR, no row identity),
 * compaction has already done its work and only a defragment reduces what new clients download.
 */
export const BUCKET_ACTION_RESIDUE_SHARE: number = 0.5;

/**
 * When at least this share of a bucket's row-bearing operations (PUT/REMOVE) is superseded history (more
 * operations than rows), a compact reclaims it (as MOVE conversions and a CLEAR prefix).
 */
export const BUCKET_ACTION_SUPERSEDED_SHARE: number = 0.5;

/**
 * Suggested maintenance action for a bucket or definition. `unknown` means the storage has no compact
 * statistics to derive one from (v1/v2 storage, or a bucket that has never been fully compacted).
 * See {@link suggestBucketAction}.
 */
export type BucketAction = 'none' | 'compact' | 'defragment' | 'both' | 'unknown';

export interface BucketStorageStats {
  /** Full bucket name, e.g. `by_user["u1"]`. */
  bucket: string;
  /** Total operations in the bucket's history. Exact and current. */
  operations: number;
  /** Approximate size of the operation history in bytes. */
  operationBytes: number;
  /**
   * Operations written after the last full compact — the staleness indicator for `rows` and
   * `fragmentation`, which are snapshots from that compact. Equals `operations` when the bucket has
   * never been fully compacted (the whole history is uncompacted).
   */
  uncompactedOperations: number;
  /**
   * Live rows in the bucket as of the last full compact, or null if the bucket has never been fully
   * compacted (or the storage version does not capture compact statistics).
   */
  rows: number | null;
  /**
   * `operations / max(rows, 1)`. ~1 is healthy (fully compacted); higher means more operation-history
   * overhead that a compact/defragment can reclaim. Null whenever `rows` is null.
   */
  fragmentation: number | null;
  /** When the bucket was last fully compacted, which is when `rows` was captured. */
  lastFullCompactAt: Date | null;
  /**
   * When the scheduled compactor will next consider this bucket, if scheduling data exists. A suggested
   * compact with a future `nextCompactAt` means the compact is already planned but throttled until then.
   */
  nextCompactAt: Date | null;
  /** Suggested maintenance action derived from the compact statistics. See {@link suggestBucketAction}. */
  suggestedAction: BucketAction;
}

/** Aggregated stats for one bucket definition (one `bucket_definitions` entry in the sync config). */
export interface BucketDefinitionStats {
  /** Definition name as it prefixes bucket names, e.g. `1#by_user` (versioned in storage v2 and later). */
  definition: string;
  /** Number of buckets in this definition with stored operations. */
  bucketCount: number;
  /** Total operations across the definition's buckets. Exact and current. */
  operations: number;
  /** Approximate size of the definition's operation history in bytes. */
  operationBytes: number;
  /**
   * Operations not covered by any bucket's last full compact, i.e. written since (or in buckets never
   * fully compacted). The staleness indicator for `rows` and `fragmentation`.
   */
  uncompactedOperations: number;
  /**
   * Live rows across the definition's buckets, counting a row once per bucket that contains it (the
   * download-relevant meaning). Derived from each bucket's last full compact; when only some buckets have
   * been fully compacted the count is extrapolated from those, and it is null when none have.
   */
  rows: number | null;
  /** `operations / max(rows, 1)` across the whole definition. Null whenever `rows` is null. */
  fragmentation: number | null;
  /** Suggested maintenance action derived from the compact statistics. See {@link suggestBucketAction}. */
  suggestedAction: BucketAction;
}

export interface BucketReportTotals {
  /** Number of buckets with stored operations. Exact, even when the rest of the totals are estimates. */
  bucketCount: number;
  /** Sum of operations across all buckets. Estimated when the bucket set was sampled. */
  operations: number;
  /** Sum of operation-history bytes across all buckets. Estimated when the bucket set was sampled. */
  operationBytes: number;
  /**
   * True if the totals are estimated because the bucket set was too large to scan in full and was sampled.
   * Row counts are never totalled here.
   */
  estimated: boolean;
}

export interface BucketReport {
  /** Worst-offender buckets, ranked by operation count then fragmentation. */
  buckets: BucketStorageStats[];
  /**
   * Per-definition rollup, ranked by operation count. Answers "which sync-rules definition should I look
   * at" where `buckets` answers "which exact buckets". Capped at {@link BUCKET_REPORT_DEFINITION_LIMIT}.
   */
  definitions: BucketDefinitionStats[];
  /** Instance-wide operation totals. Does not include row counts. */
  totals: BucketReportTotals;
  /** True if there are more buckets than returned (more than `limit`). */
  bucketsTruncated: boolean;
  /** True if the definition rollup is incomplete (more definitions exist than the rollup cap). */
  definitionsTruncated: boolean;
}

export interface GetBucketReportOptions {
  /**
   * Maximum number of buckets to return, ranked by operation count descending (worst offenders first).
   * Must be an integer between 1 and {@link MAX_BUCKET_REPORT_LIMIT}; anything else is rejected with a
   * validation error. Defaults to {@link DEFAULT_BUCKET_REPORT_LIMIT}.
   */
  limit?: number;
}

/**
 * A bucket's exact operation stats plus the statistics captured by its last full compact, before ranking.
 * The compact fields are null/absent when the bucket has never been fully compacted or the storage version
 * does not capture them (v1/v2).
 */
export interface RankedBucketInput {
  bucket: string;
  operations: number;
  operationBytes: number;
  /** Operations in the prefix covered by the last full compact. */
  compactedOperations?: number | null;
  /**
   * PUT operations in that compacted prefix. After a full compact each PUT is generally a unique row, so
   * this doubles as the bucket's live row count as of the compact.
   */
  compactedPuts?: number | null;
  /** When the last full compact ran. */
  lastFullCompactAt?: Date | null;
  /** When the scheduled compactor will next consider this bucket. */
  nextCompactAt?: Date | null;
}

/** A definition's aggregated operation stats plus its buckets' summed compact statistics, before ranking. */
export interface RankedDefinitionInput {
  definition: string;
  bucketCount: number;
  operations: number;
  operationBytes: number;
  /** Number of the definition's buckets that have full-compact statistics. */
  compactedBucketCount?: number;
  /** Sum of {@link RankedBucketInput.compactedOperations} across those buckets. */
  compactedOperations?: number;
  /** Sum of {@link RankedBucketInput.compactedPuts} across those buckets. */
  compactedPuts?: number;
}

/**
 * Resolve the requested limit, falling back to {@link DEFAULT_BUCKET_REPORT_LIMIT}. Invalid values are
 * rejected rather than clamped, so a caller asking for more than the maximum fails loudly instead of
 * silently getting fewer buckets than requested.
 */
export function resolveBucketReportLimit(limit?: number): number {
  if (limit == null) {
    return DEFAULT_BUCKET_REPORT_LIMIT;
  }
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_BUCKET_REPORT_LIMIT) {
    throw new ServiceError({
      status: 400,
      code: ErrorCode.PSYNC_S2001,
      description: `limit must be an integer between 1 and ${MAX_BUCKET_REPORT_LIMIT}`
    });
  }
  return limit;
}

/**
 * Suggest the maintenance action that reduces what new clients download from a bucket, based on its
 * operation mix. Grounded in the compaction semantics (see `docs/storage/compacting-operations.md`):
 *
 * - A **compact** converts superseded PUT/REMOVE operations into MOVE operations (reclaiming their bytes)
 *   and collapses a leading run of REMOVE/MOVE operations into one CLEAR. It helps when a bucket carries
 *   un-compacted superseded history: `rowOperations` well above `rows`.
 * - A **defragment** (touch every row, then compact) is what collapses the operation count once the history
 *   is mostly MOVE/CLEAR residue that a compact alone preserves: `operations` well above `rowOperations`.
 * - When both kinds of overhead are present, or the mix is inconclusive, suggest both.
 *
 * The thresholds are heuristics; the report is intended to be re-run after acting on it. Inputs derive from
 * the last full compact's statistics, which is fine at these margins.
 */
export function suggestBucketAction(operations: number, rowOperations: number, rows: number): BucketAction {
  const fragmentation = operations / Math.max(rows, 1);
  if (fragmentation < BUCKET_ACTION_MIN_FRAGMENTATION) {
    return 'none';
  }
  const residueShare = (operations - rowOperations) / Math.max(operations, 1);
  const supersededShare = (rowOperations - rows) / Math.max(rowOperations, 1);
  const defragmentNeeded = residueShare >= BUCKET_ACTION_RESIDUE_SHARE;
  const compactUseful = supersededShare >= BUCKET_ACTION_SUPERSEDED_SHARE;
  if (defragmentNeeded && compactUseful) {
    return 'both';
  }
  if (defragmentNeeded) {
    return 'defragment';
  }
  if (compactUseful) {
    return 'compact';
  }
  // Fragmented, but neither share dominates: a mixed history where a compact reclaims part and the rest
  // needs a defragment.
  return 'both';
}

/**
 * Derive rows / fragmentation / suggested action from full-compact statistics.
 *
 * The compacted prefix holds `compactedPuts` row-bearing operations (one per live row) and
 * `compactedOperations - compactedPuts` residue operations (MOVE/CLEAR) that only a defragment reclaims.
 * Operations written after the compact are raw PUT/REMOVE history, so the total row-bearing count is
 * `operations - residue`. Without compact statistics nothing row-related can be derived.
 */
function deriveRowStats(
  operations: number,
  compactedOperations: number | null | undefined,
  compactedPuts: number | null | undefined
): Pick<BucketStorageStats, 'rows' | 'fragmentation' | 'suggestedAction'> {
  if (compactedOperations == null || compactedPuts == null) {
    return { rows: null, fragmentation: null, suggestedAction: 'unknown' };
  }
  const rows = compactedPuts;
  const residue = Math.max(0, compactedOperations - compactedPuts);
  const rowOperations = Math.max(0, operations - residue);
  return {
    rows,
    fragmentation: operations / Math.max(rows, 1),
    suggestedAction: suggestBucketAction(operations, rowOperations, rows)
  };
}

/**
 * Assemble the final {@link BucketReport} from per-bucket stats, per-definition stats, and instance-wide
 * totals. Storage adapters select the buckets however is cheapest for them; this owns the shared
 * fragmentation / ranking / truncation / action logic so it cannot drift. Pure (no I/O) so it is
 * unit-testable.
 *
 * Bucket truncation is derived from the totals; only the adapter knows whether the definition list was cut,
 * so it passes `definitionsTruncated` in.
 */
export function assembleBucketReport(
  buckets: RankedBucketInput[],
  definitions: RankedDefinitionInput[],
  totals: BucketReportTotals,
  definitionsTruncated = false
): BucketReport {
  const stats: BucketStorageStats[] = buckets.map((b) => ({
    bucket: b.bucket,
    operations: b.operations,
    operationBytes: b.operationBytes,
    uncompactedOperations: Math.max(0, b.operations - (b.compactedOperations ?? 0)),
    lastFullCompactAt: b.lastFullCompactAt ?? null,
    nextCompactAt: b.nextCompactAt ?? null,
    ...deriveRowStats(b.operations, b.compactedOperations, b.compactedPuts)
  }));

  const definitionStats: BucketDefinitionStats[] = definitions.map((d) => {
    // When only some of the definition's buckets have full-compact statistics, extrapolate from those
    // buckets to the whole definition, assuming the compacted subset is representative.
    const compactedBucketCount = d.compactedBucketCount ?? 0;
    const scale = compactedBucketCount > 0 ? d.bucketCount / compactedBucketCount : 0;
    const derived =
      compactedBucketCount > 0 && d.compactedOperations != null && d.compactedPuts != null
        ? deriveRowStats(d.operations, Math.round(d.compactedOperations * scale), Math.round(d.compactedPuts * scale))
        : deriveRowStats(d.operations, null, null);
    return {
      definition: d.definition,
      bucketCount: d.bucketCount,
      operations: d.operations,
      operationBytes: d.operationBytes,
      // Unlike the row derivation above, this uses the plain (non-extrapolated) compacted sum: it counts
      // the operations no full compact has covered, which is exact rather than an estimate.
      uncompactedOperations: Math.max(0, d.operations - (d.compactedOperations ?? 0)),
      ...derived
    };
  });

  // Worst-first: most operations, then most fragmented.
  const worstFirst = (a: { operations: number; fragmentation: number | null }, b: typeof a) =>
    b.operations - a.operations || (b.fragmentation ?? 0) - (a.fragmentation ?? 0);
  stats.sort(worstFirst);
  definitionStats.sort(worstFirst);

  return {
    buckets: stats,
    definitions: definitionStats,
    totals,
    bucketsTruncated: totals.bucketCount > stats.length,
    definitionsTruncated
  };
}
