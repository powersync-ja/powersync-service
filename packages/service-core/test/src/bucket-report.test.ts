import { buildBucketReport, type BucketOperationStat } from '@/storage/bucket-report.js';
import { describe, expect, it } from 'vitest';

describe('buildBucketReport', () => {
  const ops = (operations: number, operationBytes = 0): BucketOperationStat => ({ operations, operationBytes });

  it('merges operation stats and row counts per bucket and derives fragmentation', () => {
    const report = buildBucketReport(
      new Map([
        ['global[]', ops(100, 1024)],
        ['by_user["u1"]', ops(10, 256)]
      ]),
      new Map([
        ['global[]', 10],
        ['by_user["u1"]', 10]
      ])
    );

    const global = report.buckets.find((b) => b.bucket === 'global[]')!;
    expect(global).toMatchObject({
      operations: 100,
      rows: 10,
      operationBytes: 1024,
      fragmentation: 10
    });

    const byUser = report.buckets.find((b) => b.bucket === 'by_user["u1"]')!;
    expect(byUser.fragmentation).toBe(1);
  });

  it('ranks buckets worst-first by operations', () => {
    const report = buildBucketReport(
      new Map([
        ['a[]', ops(5)],
        ['b[]', ops(50)],
        ['c[]', ops(20)]
      ]),
      new Map()
    );

    expect(report.buckets.map((b) => b.bucket)).toEqual(['b[]', 'c[]', 'a[]']);
  });

  it('treats a bucket with operations but no live rows as fully fragmented (rows floored at 1)', () => {
    const report = buildBucketReport(new Map([['gone[]', ops(42)]]), new Map());

    expect(report.buckets[0]).toMatchObject({ operations: 42, rows: 0, fragmentation: 42 });
  });

  it('includes buckets that have rows but no recorded operations', () => {
    const report = buildBucketReport(new Map(), new Map([['fresh[]', 7]]));

    expect(report.buckets[0]).toMatchObject({ bucket: 'fresh[]', operations: 0, rows: 7, fragmentation: 0 });
  });

  it('computes instance-wide totals across all buckets', () => {
    const report = buildBucketReport(
      new Map([
        ['a[]', ops(100, 10)],
        ['b[]', ops(20, 5)]
      ]),
      new Map([
        ['a[]', 4],
        ['b[]', 2]
      ])
    );

    // fragmentation is the row-weighted ratio 120/6 = 20, not the mean of the per-bucket ratios (25 and 10).
    expect(report.totals).toEqual({ bucketCount: 2, operations: 120, rows: 6, operationBytes: 15, fragmentation: 20 });
  });

  it('truncates the bucket list by limit but keeps totals across all buckets', () => {
    const report = buildBucketReport(
      new Map([
        ['a[]', ops(100)],
        ['b[]', ops(50)],
        ['c[]', ops(10)]
      ]),
      new Map(),
      { limit: 2 }
    );

    expect(report.truncated).toBe(true);
    expect(report.buckets.map((b) => b.bucket)).toEqual(['a[]', 'b[]']);
    expect(report.totals).toMatchObject({ bucketCount: 3, operations: 160 });
  });

  it('is not truncated when the limit exceeds the bucket count', () => {
    const report = buildBucketReport(new Map([['a[]', ops(1)]]), new Map(), { limit: 10 });

    expect(report.truncated).toBe(false);
    expect(report.buckets).toHaveLength(1);
  });
});
