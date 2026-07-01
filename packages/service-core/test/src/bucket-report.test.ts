import {
  assembleBucketReport,
  BucketReportTotals,
  DEFAULT_BUCKET_REPORT_LIMIT,
  estimateDistinctRows,
  RankedBucketInput,
  resolveBucketReportLimit
} from '@/storage/bucket-report.js';
import { describe, expect, it } from 'vitest';

describe('assembleBucketReport', () => {
  const bucket = (
    name: string,
    operations: number,
    rows: number,
    extra?: Partial<RankedBucketInput>
  ): RankedBucketInput => ({
    bucket: name,
    operations,
    rows,
    operationBytes: extra?.operationBytes ?? 0,
    rowsEstimated: extra?.rowsEstimated ?? false
  });

  const totals = (bucketCount: number, extra?: Partial<BucketReportTotals>): BucketReportTotals => ({
    bucketCount,
    operations: extra?.operations ?? 0,
    operationBytes: extra?.operationBytes ?? 0,
    estimated: extra?.estimated ?? false
  });

  it('derives fragmentation and passes through rowsEstimated', () => {
    const report = assembleBucketReport(
      [bucket('global[]', 100, 10, { operationBytes: 1024 }), bucket('by_user["u1"]', 30, 30, { rowsEstimated: true })],
      totals(2)
    );

    expect(report.buckets.find((b) => b.bucket === 'global[]')).toMatchObject({
      operations: 100,
      rows: 10,
      operationBytes: 1024,
      fragmentation: 10,
      rowsEstimated: false
    });
    expect(report.buckets.find((b) => b.bucket === 'by_user["u1"]')).toMatchObject({
      fragmentation: 1,
      rowsEstimated: true
    });
  });

  it('ranks buckets worst-first by operations then fragmentation', () => {
    const report = assembleBucketReport([bucket('a[]', 5, 5), bucket('b[]', 50, 5), bucket('c[]', 50, 50)], totals(3));

    // b and c both have 50 ops; b is more fragmented (10 vs 1) so it ranks first.
    expect(report.buckets.map((b) => b.bucket)).toEqual(['b[]', 'c[]', 'a[]']);
  });

  it('floors rows at 1 so a bucket with operations but no rows is fully fragmented', () => {
    const report = assembleBucketReport([bucket('gone[]', 42, 0)], totals(1));

    expect(report.buckets[0]).toMatchObject({ operations: 42, rows: 0, fragmentation: 42 });
  });

  it('marks truncated when there are more buckets than returned', () => {
    const truncated = assembleBucketReport([bucket('a[]', 10, 1), bucket('b[]', 5, 1)], totals(5));
    expect(truncated.truncated).toBe(true);

    const complete = assembleBucketReport([bucket('a[]', 10, 1), bucket('b[]', 5, 1)], totals(2));
    expect(complete.truncated).toBe(false);
  });

  it('carries the totals through unchanged', () => {
    const t = totals(2, { operations: 120, operationBytes: 15, estimated: true });
    const report = assembleBucketReport([bucket('a[]', 100, 4), bucket('b[]', 20, 2)], t);

    expect(report.totals).toEqual({ bucketCount: 2, operations: 120, operationBytes: 15, estimated: true });
  });
});

describe('estimateDistinctRows', () => {
  it('returns the observed distinct count when the whole bucket was sampled', () => {
    // r >= 1: nothing was left out, so the observed distinct count is already exact.
    expect(estimateDistinctRows(100, 100, 40)).toBe(40);
    expect(estimateDistinctRows(100, 150, 40)).toBe(40);
  });

  it('recovers a heavily fragmented bucket the naive estimate would inflate', () => {
    // 10 rows x 1000 ops each; a 10% sample sees ~1000 ops but still only the same 10 distinct rows.
    // Naive distinct/rate would report 10 / 0.1 = 100 rows (10x too many, so 10x too little fragmentation).
    const rows = estimateDistinctRows(10_000, 1_000, 10);
    expect(rows).toBeGreaterThanOrEqual(9);
    expect(rows).toBeLessThanOrEqual(12);
  });

  it('recovers a moderately fragmented bucket', () => {
    // 500 rows x 2 ops each, 50% sample. Ground truth: 500*(1-0.5^2) = 375 distinct sampled rows.
    // Naive distinct/rate would report 375 / 0.5 = 750 rows; the estimator should recover ~500.
    const rows = estimateDistinctRows(1_000, 500, 375);
    expect(rows).toBeGreaterThan(480);
    expect(rows).toBeLessThan(520);
  });

  it('matches the naive estimate when there are no sampling collisions', () => {
    // 2000 rows, 1 op each, 50% sample: no row is seen twice, so distinct/rate is already correct (~2000).
    const rows = estimateDistinctRows(2_000, 1_000, 1_000);
    expect(rows).toBeGreaterThan(1_900);
    expect(rows).toBeLessThan(2_100);
  });
});

describe('resolveBucketReportLimit', () => {
  it('defaults when no limit is given', () => {
    expect(resolveBucketReportLimit(undefined)).toBe(DEFAULT_BUCKET_REPORT_LIMIT);
  });

  it('floors and clamps to a positive integer', () => {
    expect(resolveBucketReportLimit(2.7)).toBe(2);
    expect(resolveBucketReportLimit(-5)).toBe(1);
    expect(resolveBucketReportLimit(0)).toBe(1);
    expect(resolveBucketReportLimit(20)).toBe(20);
  });
});
