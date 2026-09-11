import {
  assembleBucketReport,
  BucketReportTotals,
  DEFAULT_BUCKET_REPORT_LIMIT,
  MAX_BUCKET_REPORT_LIMIT,
  RankedBucketInput,
  RankedDefinitionInput,
  resolveBucketReportLimit,
  suggestBucketAction
} from '@/storage/bucket-report.js';
import { describe, expect, it } from 'vitest';

const bucket = (name: string, operations: number, extra?: Partial<RankedBucketInput>): RankedBucketInput => ({
  bucket: name,
  operations,
  operationBytes: 0,
  ...extra
});

/** Full-compact statistics where the compacted prefix is the whole history (no writes since). */
const compacted = (operations: number, puts: number): Partial<RankedBucketInput> => ({
  compactedOperations: operations,
  compactedPuts: puts
});

const definition = (
  name: string,
  operations: number,
  extra?: Partial<RankedDefinitionInput>
): RankedDefinitionInput => ({
  definition: name,
  bucketCount: 1,
  operations,
  operationBytes: 0,
  ...extra
});

const totals = (bucketCount: number, extra?: Partial<BucketReportTotals>): BucketReportTotals => ({
  bucketCount,
  operations: extra?.operations ?? 0,
  operationBytes: extra?.operationBytes ?? 0,
  estimated: extra?.estimated ?? false
});

describe('assembleBucketReport', () => {
  it('derives rows and fragmentation from the full-compact statistics', () => {
    const compactedAt = new Date('2026-01-01T00:00:00Z');
    const nextCompact = new Date('2026-01-02T00:00:00Z');
    const report = assembleBucketReport(
      [
        bucket('global[]', 100, {
          operationBytes: 1024,
          ...compacted(100, 10),
          lastFullCompactAt: compactedAt,
          nextCompactAt: nextCompact
        }),
        bucket('by_user["u1"]', 30, compacted(30, 30))
      ],
      [],
      totals(2)
    );

    expect(report.buckets.find((b) => b.bucket === 'global[]')).toMatchObject({
      operations: 100,
      operationBytes: 1024,
      rows: 10,
      fragmentation: 10,
      lastFullCompactAt: compactedAt,
      nextCompactAt: nextCompact
    });
    expect(report.buckets.find((b) => b.bucket === 'by_user["u1"]')).toMatchObject({
      rows: 30,
      fragmentation: 1,
      lastFullCompactAt: null,
      nextCompactAt: null
    });
  });

  it('reports null rows and an unknown action without full-compact statistics', () => {
    const report = assembleBucketReport([bucket('global[]', 100)], [], totals(1));

    expect(report.buckets[0]).toMatchObject({
      operations: 100,
      rows: null,
      fragmentation: null,
      suggestedAction: 'unknown',
      // Never compacted: the whole history is uncompacted.
      uncompactedOperations: 100
    });
  });

  it('reports the operations written since the last full compact', () => {
    const report = assembleBucketReport(
      [
        // Fully covered by the compact: nothing uncompacted.
        bucket('fresh[]', 100, compacted(100, 100)),
        // 900 operations landed after the compact: rows/fragmentation are that much out of date.
        bucket('stale[]', 1000, compacted(100, 100))
      ],
      [
        // Definition grain uses the plain compacted sum (not the extrapolated one used for rows):
        // 1000 total ops, 50 covered by compacts of half the buckets, so 950 are uncompacted.
        definition('1#partial', 1000, {
          bucketCount: 10,
          compactedBucketCount: 5,
          compactedOperations: 50,
          compactedPuts: 50
        })
      ],
      totals(12)
    );

    const by = (name: string) => report.buckets.find((b) => b.bucket === name)!;
    expect(by('fresh[]').uncompactedOperations).toEqual(0);
    expect(by('stale[]').uncompactedOperations).toEqual(900);
    expect(report.definitions[0].uncompactedOperations).toEqual(950);
  });

  it('ranks buckets worst-first by operations then fragmentation', () => {
    const report = assembleBucketReport(
      [bucket('a[]', 5, compacted(5, 5)), bucket('b[]', 50, compacted(50, 5)), bucket('c[]', 50, compacted(50, 50))],
      [],
      totals(3)
    );

    // b and c both have 50 ops; b is more fragmented (10 vs 1) so it ranks first.
    expect(report.buckets.map((b) => b.bucket)).toEqual(['b[]', 'c[]', 'a[]']);
  });

  it('floors rows at 1 so a bucket with operations but no rows is fully fragmented', () => {
    const report = assembleBucketReport([bucket('gone[]', 42, compacted(42, 0))], [], totals(1));

    expect(report.buckets[0]).toMatchObject({ operations: 42, rows: 0, fragmentation: 42 });
  });

  it('marks the bucket list truncated when there are more buckets than returned', () => {
    const truncated = assembleBucketReport([bucket('a[]', 10), bucket('b[]', 5)], [], totals(5));
    expect(truncated.bucketsTruncated).toBe(true);

    const complete = assembleBucketReport([bucket('a[]', 10), bucket('b[]', 5)], [], totals(2));
    expect(complete.bucketsTruncated).toBe(false);
  });

  it('passes the definition truncation flag through, defaulting to complete', () => {
    expect(assembleBucketReport([], [definition('a', 1)], totals(1)).definitionsTruncated).toBe(false);
    expect(assembleBucketReport([], [definition('a', 1)], totals(1), true).definitionsTruncated).toBe(true);
  });

  it('carries the totals through unchanged', () => {
    const t = totals(2, { operations: 120, operationBytes: 15, estimated: true });
    const report = assembleBucketReport([bucket('a[]', 100), bucket('b[]', 20)], [], t);

    expect(report.totals).toEqual({ bucketCount: 2, operations: 120, operationBytes: 15, estimated: true });
  });

  it('assembles and ranks the definition rollup with derived fragmentation and action', () => {
    const report = assembleBucketReport(
      [],
      [
        definition('1#by_user', 100, {
          bucketCount: 10,
          compactedBucketCount: 10,
          compactedOperations: 100,
          compactedPuts: 100
        }),
        definition('1#by_org', 500, {
          bucketCount: 5,
          operationBytes: 2048,
          compactedBucketCount: 5,
          compactedOperations: 500,
          compactedPuts: 100
        })
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
      suggestedAction: 'defragment'
    });
    expect(report.definitions[1]).toMatchObject({ fragmentation: 1, suggestedAction: 'none' });
  });

  it('extrapolates definition rows when only some buckets have been fully compacted', () => {
    const report = assembleBucketReport(
      [],
      [
        // 5 of 10 buckets compacted, holding 50 rows between them: assume the other half looks the same.
        definition('1#partial', 1000, {
          bucketCount: 10,
          compactedBucketCount: 5,
          compactedOperations: 50,
          compactedPuts: 50
        }),
        // No compacted buckets: nothing row-related can be derived.
        definition('1#uncompacted', 1000, { bucketCount: 10, compactedBucketCount: 0 })
      ],
      totals(20)
    );

    expect(report.definitions.find((d) => d.definition === '1#partial')).toMatchObject({
      rows: 100,
      fragmentation: 10
    });
    expect(report.definitions.find((d) => d.definition === '1#uncompacted')).toMatchObject({
      rows: null,
      fragmentation: null,
      suggestedAction: 'unknown'
    });
  });

  it('derives per-bucket suggested actions from the compact statistics', () => {
    const report = assembleBucketReport(
      [
        // Healthy: one op per row.
        bucket('healthy[]', 100, compacted(100, 100)),
        // Un-compacted churn since the last full compact: 900 raw ops on top of a clean 100-row prefix.
        bucket('churned[]', 1000, compacted(100, 100)),
        // Compacted residue: the compact kept 150 rows but left 850 MOVE/CLEAR ops behind.
        bucket('residue[]', 1000, compacted(1000, 150))
      ],
      [],
      totals(3)
    );

    const action = (name: string) => report.buckets.find((b) => b.bucket === name)?.suggestedAction;
    expect(action('healthy[]')).toEqual('none');
    expect(action('churned[]')).toEqual('compact');
    expect(action('residue[]')).toEqual('defragment');
  });
});

describe('suggestBucketAction', () => {
  it('suggests nothing for buckets under 3x fragmentation', () => {
    expect(suggestBucketAction(100, 100, 100)).toEqual('none');
    expect(suggestBucketAction(250, 250, 100)).toEqual('none');
    expect(suggestBucketAction(0, 0, 0)).toEqual('none');
  });

  it('suggests compact for un-compacted superseded history', () => {
    // All operations carry row identity, but there are 10x more of them than rows.
    expect(suggestBucketAction(1000, 1000, 100)).toEqual('compact');
    // 3x fragmentation is the threshold where actions start.
    expect(suggestBucketAction(300, 300, 100)).toEqual('compact');
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
    // Fragmented (frag 3), yet neither residue (40%) nor superseded share (44%) dominates.
    expect(suggestBucketAction(1200, 720, 400)).toEqual('both');
  });
});

describe('resolveBucketReportLimit', () => {
  it('defaults when no limit is given', () => {
    expect(resolveBucketReportLimit(undefined)).toBe(DEFAULT_BUCKET_REPORT_LIMIT);
  });

  it('accepts integers up to the maximum', () => {
    expect(resolveBucketReportLimit(1)).toBe(1);
    expect(resolveBucketReportLimit(20)).toBe(20);
    expect(resolveBucketReportLimit(MAX_BUCKET_REPORT_LIMIT)).toBe(MAX_BUCKET_REPORT_LIMIT);
  });

  it('rejects invalid limits instead of clamping them', () => {
    for (const invalid of [0, -5, 2.7, MAX_BUCKET_REPORT_LIMIT + 1]) {
      let error: any;
      try {
        resolveBucketReportLimit(invalid);
      } catch (e) {
        error = e;
      }
      expect(error?.errorData, `limit ${invalid}`).toMatchObject({ status: 400 });
    }
  });
});
