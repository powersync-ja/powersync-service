import {
  assembleBucketReport,
  BucketReportTotals,
  DEFAULT_BUCKET_REPORT_LIMIT,
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
