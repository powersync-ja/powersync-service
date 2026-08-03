import {
  assembleBucketReport,
  BucketReportTotals,
  DEFAULT_BUCKET_REPORT_LIMIT,
  estimateDistinctRows,
  RankedBucketInput,
  RankedDefinitionInput,
  resolveBucketReportLimit,
  suggestBucketAction
} from '@/storage/bucket-report.js';
import { describe, expect, it } from 'vitest';

// Row-bearing operations default to all operations (no compaction residue) unless overridden.
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
  rowOperations: extra?.rowOperations ?? operations,
  rowsEstimated: extra?.rowsEstimated ?? false,
  tables: extra?.tables ?? []
});

const definition = (
  name: string,
  operations: number,
  rows: number,
  extra?: Partial<RankedDefinitionInput>
): RankedDefinitionInput => ({
  definition: name,
  bucketCount: extra?.bucketCount ?? 1,
  operations,
  rows,
  operationBytes: extra?.operationBytes ?? 0,
  rowOperations: extra?.rowOperations ?? operations,
  rowsEstimated: extra?.rowsEstimated ?? false,
  tables: extra?.tables ?? []
});

const totals = (bucketCount: number, extra?: Partial<BucketReportTotals>): BucketReportTotals => ({
  bucketCount,
  operations: extra?.operations ?? 0,
  operationBytes: extra?.operationBytes ?? 0,
  estimated: extra?.estimated ?? false
});

describe('assembleBucketReport', () => {
  it('derives fragmentation and passes through rowsEstimated and tables', () => {
    const report = assembleBucketReport(
      [
        bucket('global[]', 100, 10, { operationBytes: 1024, tables: ['todos', 'lists'] }),
        bucket('by_user["u1"]', 30, 30, { rowsEstimated: true })
      ],
      [],
      totals(2)
    );

    expect(report.buckets.find((b) => b.bucket === 'global[]')).toMatchObject({
      operations: 100,
      rows: 10,
      operationBytes: 1024,
      fragmentation: 10,
      rowsEstimated: false,
      tables: ['todos', 'lists']
    });
    expect(report.buckets.find((b) => b.bucket === 'by_user["u1"]')).toMatchObject({
      fragmentation: 1,
      rowsEstimated: true
    });
  });

  it('ranks buckets worst-first by operations then fragmentation', () => {
    const report = assembleBucketReport(
      [bucket('a[]', 5, 5), bucket('b[]', 50, 5), bucket('c[]', 50, 50)],
      [],
      totals(3)
    );

    // b and c both have 50 ops; b is more fragmented (10 vs 1) so it ranks first.
    expect(report.buckets.map((b) => b.bucket)).toEqual(['b[]', 'c[]', 'a[]']);
  });

  it('floors rows at 1 so a bucket with operations but no rows is fully fragmented', () => {
    const report = assembleBucketReport([bucket('gone[]', 42, 0)], [], totals(1));

    expect(report.buckets[0]).toMatchObject({ operations: 42, rows: 0, fragmentation: 42 });
  });

  it('marks the bucket list truncated when there are more buckets than returned', () => {
    const truncated = assembleBucketReport([bucket('a[]', 10, 1), bucket('b[]', 5, 1)], [], totals(5));
    expect(truncated.bucketsTruncated).toBe(true);

    const complete = assembleBucketReport([bucket('a[]', 10, 1), bucket('b[]', 5, 1)], [], totals(2));
    expect(complete.bucketsTruncated).toBe(false);
  });

  it('passes the definition truncation flag through, defaulting to complete', () => {
    expect(assembleBucketReport([], [definition('a', 1, 1)], totals(1)).definitionsTruncated).toBe(false);
    expect(assembleBucketReport([], [definition('a', 1, 1)], totals(1), true).definitionsTruncated).toBe(true);
  });

  it('carries the totals through unchanged', () => {
    const t = totals(2, { operations: 120, operationBytes: 15, estimated: true });
    const report = assembleBucketReport([bucket('a[]', 100, 4), bucket('b[]', 20, 2)], [], t);

    expect(report.totals).toEqual({ bucketCount: 2, operations: 120, operationBytes: 15, estimated: true });
  });

  it('assembles and ranks the definition rollup with derived fragmentation and action', () => {
    const report = assembleBucketReport(
      [],
      [
        definition('1#by_user', 100, 100, { bucketCount: 10 }),
        definition('1#by_org', 500, 100, { bucketCount: 5, operationBytes: 2048 })
      ],
      totals(15)
    );

    // Ranked by operations: by_org (500) before by_user (100).
    expect(report.definitions.map((d) => d.definition)).toEqual(['1#by_org', '1#by_user']);
    expect(report.definitions[0]).toMatchObject({
      definition: '1#by_org',
      bucketCount: 5,
      operations: 500,
      operationBytes: 2048,
      rows: 100,
      fragmentation: 5,
      suggestedAction: 'compact'
    });
    expect(report.definitions[1]).toMatchObject({ fragmentation: 1, suggestedAction: 'none' });
  });

  it('derives per-bucket suggested actions from the operation mix', () => {
    const report = assembleBucketReport(
      [
        // Healthy: one op per row.
        bucket('healthy[]', 100, 100),
        // Un-compacted churn: every op carries a row identity, far more ops than rows.
        bucket('churned[]', 1000, 100),
        // Compacted residue: mostly MOVE/CLEAR ops left behind by a compact.
        bucket('compacted[]', 1000, 100, { rowOperations: 150 })
      ],
      [],
      totals(3)
    );

    const action = (name: string) => report.buckets.find((b) => b.bucket === name)?.suggestedAction;
    expect(action('healthy[]')).toEqual('none');
    expect(action('churned[]')).toEqual('compact');
    expect(action('compacted[]')).toEqual('defragment');
  });
});

describe('suggestBucketAction', () => {
  it('suggests nothing for healthy buckets', () => {
    expect(suggestBucketAction(100, 100, 100)).toEqual('none');
    expect(suggestBucketAction(150, 150, 100)).toEqual('none');
    expect(suggestBucketAction(0, 0, 0)).toEqual('none');
  });

  it('suggests compact for un-compacted superseded history', () => {
    // All operations carry row identity, but there are 10x more of them than rows.
    expect(suggestBucketAction(1000, 1000, 100)).toEqual('compact');
  });

  it('suggests defragment when compaction residue dominates', () => {
    // 850 of 1000 ops are MOVE/CLEAR: a compact already ran and cannot reclaim more.
    expect(suggestBucketAction(1000, 150, 100)).toEqual('defragment');
    // A bucket of only MOVE/CLEAR ops (rows 0) is pure residue.
    expect(suggestBucketAction(500, 0, 0)).toEqual('defragment');
  });

  it('suggests both when residue and fresh superseded history are both present', () => {
    // 600 residue ops plus 400 row-bearing ops over 100 rows: defragment for the residue, compact for the churn.
    expect(suggestBucketAction(1000, 400, 100)).toEqual('both');
  });

  it('suggests both for a fragmented but inconclusive mix', () => {
    // Fragmented (frag 2.5), yet neither residue (40%) nor superseded share (33%) dominates.
    expect(suggestBucketAction(1000, 600, 400)).toEqual('both');
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
