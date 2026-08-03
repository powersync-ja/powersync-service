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
 * Scaling note: the report does NOT scan all storage. It ranks buckets by their pre-aggregated operation
 * count and returns the worst offenders (top-N). Row counts (and therefore fragmentation) for those buckets
 * are derived by sampling the operation history, so on large buckets they are estimates, flagged per bucket.
 */

/**
 * Time budget for the per-bucket report's bucket-selection aggregation (`maxTimeMS`). Bounded so an admin
 * request on a large instance fails fast instead of running unbounded.
 */
export const BUCKET_REPORT_TIMEOUT_MS: number = 60_000;

/**
 * Number of worst-offender buckets returned when the request omits a `limit`. Row counts are sampled per
 * returned bucket, so this also bounds how much sampling work the report does.
 */
export const DEFAULT_BUCKET_REPORT_LIMIT: number = 50;

/**
 * Maximum number of bucket definitions in the report's definition rollup. Rows are sampled per returned
 * definition (like per-bucket rows), so this bounds that sampling work. Configs rarely approach this many
 * definitions.
 */
export const BUCKET_REPORT_DEFINITION_LIMIT: number = 20;

/** Fragmentation below this is considered healthy: no maintenance action is suggested. */
export const BUCKET_ACTION_MIN_FRAGMENTATION: number = 2;

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

/** Suggested maintenance action for a bucket or definition. See {@link suggestBucketAction}. */
export type BucketAction = 'none' | 'compact' | 'defragment' | 'both';

export interface BucketStorageStats {
  /** Full bucket name, e.g. `by_user["u1"]`. */
  bucket: string;
  /** Total operations in the bucket's history. */
  operations: number;
  /** Live rows in the bucket. Exact for small buckets, otherwise a sampled estimate (see `rowsEstimated`). */
  rows: number;
  /** Approximate size of the operation history in bytes. */
  operationBytes: number;
  /**
   * `operations / max(rows, 1)`. ~1 is healthy (fully compacted); higher means more operation-history
   * overhead that a compact/defragment can reclaim.
   */
  fragmentation: number;
  /** True if `rows` (and therefore `fragmentation`) is a sampled estimate rather than an exact count. */
  rowsEstimated: boolean;
  /** Suggested maintenance action derived from the operation mix. See {@link suggestBucketAction}. */
  suggestedAction: BucketAction;
  /**
   * Tables making up the (sampled) operation history, ordered by their share of it, largest first. These
   * are the tables whose rows a defragment should touch.
   */
  tables: string[];
}

/** Aggregated stats for one bucket definition (one `bucket_definitions` entry in the sync config). */
export interface BucketDefinitionStats {
  /** Definition name as it prefixes bucket names, e.g. `1#by_user` (versioned in storage v2 and later). */
  definition: string;
  /** Number of buckets in this definition with stored operations. */
  bucketCount: number;
  /** Total operations across the definition's buckets. */
  operations: number;
  /** Approximate size of the definition's operation history in bytes. */
  operationBytes: number;
  /**
   * Live rows across the definition's buckets, counting a row once per bucket that contains it (the
   * download-relevant meaning). Sampled estimate for all but tiny definitions (see `rowsEstimated`).
   */
  rows: number;
  /** `operations / max(rows, 1)` across the whole definition. */
  fragmentation: number;
  /** True if `rows` (and therefore `fragmentation`) is a sampled estimate rather than an exact count. */
  rowsEstimated: boolean;
  /** Suggested maintenance action derived from the operation mix. See {@link suggestBucketAction}. */
  suggestedAction: BucketAction;
  /**
   * Tables making up the (sampled) operation history, ordered by their share of it, largest first. These
   * are the tables whose rows a defragment should touch.
   */
  tables: string[];
}

export interface BucketReportTotals {
  /** Number of buckets with stored operations. Estimated when the bucket set was sampled (see `estimated`). */
  bucketCount: number;
  /** Sum of operations across all buckets. Estimated when the bucket set was sampled. */
  operations: number;
  /** Sum of operation-history bytes across all buckets. Estimated when the bucket set was sampled. */
  operationBytes: number;
  /**
   * True if the totals are estimated because the bucket set was too large to scan in full and was sampled.
   * Row counts are never totalled here (they are sampled per returned bucket, not across the whole instance).
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
  /** Instance-wide operation totals. Does not include row counts (those are per-bucket estimates only). */
  totals: BucketReportTotals;
  /** True if there are more buckets than returned (more than `limit`). */
  bucketsTruncated: boolean;
  /**
   * True if the definition rollup is incomplete: more definitions exist than
   * {@link BUCKET_REPORT_DEFINITION_LIMIT}, or a definition was dropped because sampling it failed.
   */
  definitionsTruncated: boolean;
}

export interface GetBucketReportOptions {
  /**
   * Maximum number of buckets to return, ranked by operation count descending (worst offenders first).
   * Row counts are sampled per returned bucket, so this also bounds the report's cost. Non-integer or
   * negative values are floored and clamped to 1. Defaults to {@link DEFAULT_BUCKET_REPORT_LIMIT}.
   */
  limit?: number;
}

/** A bucket's exact operation stats plus its (possibly sampled) row count, before ranking. */
export interface RankedBucketInput {
  bucket: string;
  operations: number;
  operationBytes: number;
  rows: number;
  /**
   * Operations that carry a row identity (PUT/REMOVE), i.e. everything except compaction residue
   * (MOVE/CLEAR). Estimated alongside `rows` for sampled buckets.
   */
  rowOperations: number;
  rowsEstimated: boolean;
  /** Tables in the (sampled) operation history, ordered by their share of it, largest first. */
  tables: string[];
}

/** A definition's aggregated operation stats plus its (possibly sampled) row count, before ranking. */
export interface RankedDefinitionInput {
  definition: string;
  bucketCount: number;
  operations: number;
  operationBytes: number;
  rows: number;
  /** As in {@link RankedBucketInput.rowOperations}, across the whole definition. */
  rowOperations: number;
  rowsEstimated: boolean;
  /** As in {@link RankedBucketInput.tables}, across the whole definition. */
  tables: string[];
}

/**
 * Normalize a requested limit to a positive integer, falling back to {@link DEFAULT_BUCKET_REPORT_LIMIT}.
 */
export function resolveBucketReportLimit(limit?: number): number {
  if (limit == null) {
    return DEFAULT_BUCKET_REPORT_LIMIT;
  }
  return Math.max(1, Math.floor(limit));
}

/**
 * Estimate the true distinct row count of a bucket from a random sample of its operations.
 *
 * The signal is repetition: a sample that keeps landing on the same rows means few rows, while a sample
 * where every operation lands on a new row means many. Formally, each operation is included in the sample
 * with probability `r = sampledOps / operations`, so a row with `k` operations appears with probability
 * `1 - (1 - r)^k`. Assuming operations are spread roughly evenly across `R` rows (`k = operations / R`),
 * the expected number of distinct rows in the sample is `R * (1 - (1 - r)^(operations / R))`. That grows
 * with `R`, so a binary search finds the `R` matching the observed distinct count.
 *
 * The naive `distinctRows / r` ignores repetition and over-counts rows (under-stating fragmentation) on
 * exactly the highly fragmented buckets the report exists to surface.
 *
 * Pure (no I/O) so it is unit-testable; storage adapters supply the sampled counts.
 */
export function estimateDistinctRows(operations: number, sampledOps: number, distinctRows: number): number {
  const r = Math.min(1, sampledOps / operations);
  if (r >= 1) {
    return distinctRows;
  }
  const expectedDistinct = (rows: number) => rows * (1 - Math.pow(1 - r, operations / rows));
  // The true row count is between the observed distinct count (a lower bound) and one row per operation.
  // Binary-search that range until it is narrower than a single row, at which point rounding is exact.
  let lo = distinctRows;
  let hi = operations;
  while (hi - lo > 0.5) {
    const mid = (lo + hi) / 2;
    if (expectedDistinct(mid) < distinctRows) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  return Math.round((lo + hi) / 2);
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
 * The thresholds are heuristics; the report is intended to be re-run after acting on it. Inputs may be
 * sampled estimates, which is fine at these margins.
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
 * Assemble the final {@link BucketReport} from per-bucket stats, per-definition stats, and instance-wide
 * totals. Storage adapters select and sample the buckets however is cheapest for them; this owns the shared
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
    rows: b.rows,
    operationBytes: b.operationBytes,
    fragmentation: b.operations / Math.max(b.rows, 1),
    rowsEstimated: b.rowsEstimated,
    suggestedAction: suggestBucketAction(b.operations, b.rowOperations, b.rows),
    tables: b.tables
  }));

  const definitionStats: BucketDefinitionStats[] = definitions.map((d) => ({
    definition: d.definition,
    bucketCount: d.bucketCount,
    operations: d.operations,
    operationBytes: d.operationBytes,
    rows: d.rows,
    fragmentation: d.operations / Math.max(d.rows, 1),
    rowsEstimated: d.rowsEstimated,
    suggestedAction: suggestBucketAction(d.operations, d.rowOperations, d.rows),
    tables: d.tables
  }));

  // Worst-first: most operations, then most fragmented.
  const worstFirst = (a: { operations: number; fragmentation: number }, b: typeof a) =>
    b.operations - a.operations || b.fragmentation - a.fragmentation;
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
