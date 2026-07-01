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
  /** Instance-wide operation totals. Does not include row counts (those are per-bucket estimates only). */
  totals: BucketReportTotals;
  /** True if there are more buckets than returned (more than `limit`). */
  truncated: boolean;
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
  rowsEstimated: boolean;
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
 * Estimate the true distinct row count of a bucket from a sample of its operations.
 *
 * Each operation is included in the sample with probability `r = sampledOps / operations`, so a row with
 * `k` operations is seen with probability `1 - (1 - r)^k`. Assuming operations are spread roughly evenly
 * across rows (so each of `R` rows has about `operations / R` of them), the expected number of distinct
 * rows in the sample is `R * (1 - (1 - r)^(operations / R))`. This is monotonic in `R`, so we binary-search
 * for the `R` that matches the observed distinct count.
 *
 * The naive `distinctRows / r` over-counts rows (and so under-states fragmentation) whenever the sample
 * already covered most rows - exactly the highly-fragmented buckets the report exists to surface.
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
 * Assemble the final {@link BucketReport} from per-bucket stats and instance-wide totals. Storage adapters
 * select and sample the buckets however is cheapest for them; this owns the shared fragmentation / ranking /
 * truncation logic so it cannot drift. Pure (no I/O) so it is unit-testable.
 */
export function assembleBucketReport(buckets: RankedBucketInput[], totals: BucketReportTotals): BucketReport {
  const stats: BucketStorageStats[] = buckets.map((b) => ({
    bucket: b.bucket,
    operations: b.operations,
    rows: b.rows,
    operationBytes: b.operationBytes,
    fragmentation: b.operations / Math.max(b.rows, 1),
    rowsEstimated: b.rowsEstimated
  }));

  // Worst-first: most operations, then most fragmented.
  stats.sort((a, b) => b.operations - a.operations || b.fragmentation - a.fragmentation);

  return {
    buckets: stats,
    totals,
    truncated: totals.bucketCount > stats.length
  };
}
