/**
 * Per-bucket storage report for an active sync config.
 *
 * - An **operation** is any entry in a bucket's append-only history (`PUT`, `REMOVE`, `MOVE`, `CLEAR`).
 * - A **row** is a distinct live object currently in the bucket.
 *
 * A new client downloads every operation, not just live rows, so `operations / rows` is effectively a
 * fragmentation / compaction-efficiency score: a fully compacted bucket trends towards ~1, while a high
 * ratio is the usual cause of an unexpectedly high "Data Synced" metric and is reclaimable via compact/defragment.
 */

/**
 * Time budget for the per-bucket report's storage aggregations (Mongo `maxTimeMS`, Postgres
 * `statement_timeout`). The report scans current rows (and, on Postgres, all bucket data), which can be
 * expensive on large instances, so the queries are bounded rather than allowed to run unbounded.
 */
export const BUCKET_REPORT_TIMEOUT_MS: number = 60_000;

export interface BucketOperationStat {
  /** Total operations in the bucket's history (PUT/REMOVE/MOVE/CLEAR). */
  operations: number;
  /** Approximate size of the operation history in bytes. */
  operationBytes: number;
}

export interface BucketStorageStats {
  /** Full bucket name, e.g. `by_user["u1"]`. */
  bucket: string;
  /** Total operations in the bucket's history. */
  operations: number;
  /** Live rows currently in the bucket. */
  rows: number;
  /** Approximate size of the operation history in bytes. */
  operationBytes: number;
  /**
   * `operations / max(rows, 1)`. ~1 is healthy (fully compacted); higher means more operation-history
   * overhead that a compact/defragment can reclaim.
   */
  fragmentation: number;
}

export interface BucketReportTotals {
  /** Number of buckets with stored operations or rows (before any `limit`). */
  bucketCount: number;
  /** Sum of operations across all buckets. */
  operations: number;
  /**
   * Sum of per-bucket live rows. Note this double-counts rows that belong to multiple buckets,
   * so it is a sum of per-bucket counts rather than a distinct instance-wide row total.
   */
  rows: number;
  /** Sum of operation-history bytes across all buckets. */
  operationBytes: number;
  /**
   * Instance-wide fragmentation: `operations / max(rows, 1)`, i.e. the row-weighted average of the
   * per-bucket ratios. ~1 is healthy; a higher value means a new client downloads that many operations
   * per live row across the whole instance, which is the headline cause of a high "Data Synced" metric.
   */
  fragmentation: number;
}

export interface BucketReport {
  /** Per-bucket stats, ranked worst-first (most operations, then most fragmented). */
  buckets: BucketStorageStats[];
  /** Instance-wide totals, computed across all buckets even when `buckets` is truncated by `limit`. */
  totals: BucketReportTotals;
  /** True if `buckets` was truncated by `limit`. `totals` still reflects all buckets. */
  truncated: boolean;
}

export interface GetBucketReportOptions {
  /**
   * Maximum number of buckets to return, ranked by operation count descending (worst offenders first).
   * This caps the response size only: every backend still aggregates all buckets (and `totals` covers
   * them all), so it is not a query-cost bound. Non-integer or negative values are floored and clamped
   * to 0. Defaults to no limit.
   */
  limit?: number;
}

/**
 * Combine per-bucket operation stats and live-row counts (each keyed by full bucket name) into a
 * ranked {@link BucketReport}. Backend storage adapters collect the two maps however is cheapest for
 * them; this builder owns the shared merge/rank/total logic so that part stays identical across
 * backends. The inputs are not identical: operation counts are exact on Postgres (a `COUNT(*)`) but a
 * pre-aggregated estimate on MongoDB, so the same data can yield slightly different counts per backend.
 */
export function buildBucketReport(
  operationStats: Map<string, BucketOperationStat>,
  rowCounts: Map<string, number>,
  options?: GetBucketReportOptions
): BucketReport {
  const bucketNames = new Set<string>([...operationStats.keys(), ...rowCounts.keys()]);

  const buckets: BucketStorageStats[] = [];
  const totals: BucketReportTotals = { bucketCount: 0, operations: 0, rows: 0, operationBytes: 0, fragmentation: 0 };

  for (const bucket of bucketNames) {
    const opStat = operationStats.get(bucket);
    const operations = opStat?.operations ?? 0;
    const operationBytes = opStat?.operationBytes ?? 0;
    const rows = rowCounts.get(bucket) ?? 0;

    buckets.push({
      bucket,
      operations,
      rows,
      operationBytes,
      fragmentation: operations / Math.max(rows, 1)
    });

    totals.bucketCount += 1;
    totals.operations += operations;
    totals.rows += rows;
    totals.operationBytes += operationBytes;
  }

  totals.fragmentation = totals.operations / Math.max(totals.rows, 1);

  // Worst-first: most operations, then most fragmented.
  buckets.sort((a, b) => b.operations - a.operations || b.fragmentation - a.fragmentation);

  let truncated = false;
  let reported = buckets;
  if (options?.limit != null) {
    const limit = Math.max(0, Math.floor(options.limit));
    if (buckets.length > limit) {
      reported = buckets.slice(0, limit);
      truncated = true;
    }
  }

  return { buckets: reported, totals, truncated };
}
