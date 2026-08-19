import {
  applyCompactionDelta,
  applyStatsReplacement,
  bucketStats,
  chooseCompactionKind,
  combineAdjacentStats,
  combineChunkStats,
  CompactIntervalConfig,
  CompactionKind,
  emptyBucketStats,
  forcedCompactionKind,
  fullCompactionCheckAt,
  statsForDocument,
  statsForDocuments
} from '@module/storage/implementation/v3/compact-utils.js';
import { BucketStateDocumentV3 } from '@module/storage/implementation/v3/models.js';
import { describe, expect, test } from 'vitest';

const INTERVAL_CONFIG: CompactIntervalConfig = {
  minCompactChunkIntervalMs: 1_000,
  minCompactFullIntervalMs: 10_000,
  maxCompactFullIntervalMs: 100_000
};

function bucketState(overrides: Partial<BucketStateDocumentV3> = {}): BucketStateDocumentV3 {
  return {
    _id: { d: 'definition', b: 'bucket[]' },
    last_op: 10n,
    next_compact_check: new Date(0),
    first_uncompacted_write: new Date(0),
    bucket_stats: { count: 10, bytes: 100n, chunks: 1 },
    ...overrides
  };
}

function compactedState(overrides: Partial<NonNullable<BucketStateDocumentV3['compacted_state']>> = {}) {
  return {
    op_id: 5n,
    checksum: 50n,
    count: 5,
    bytes: 50n,
    chunks: 1,
    at: new Date(0),
    ...overrides
  };
}

describe('V3 compact utilities', () => {
  test.each([
    {
      name: 'uses the minimum interval before the first full compact',
      state: bucketState(),
      expected: new Date(10_000)
    },
    {
      name: 'scales the interval by the uncompacted row ratio',
      state: bucketState({
        bucket_stats: { count: 100, bytes: 100n, chunks: 1 },
        last_full_compact: { op_id: 90n, count: 90, puts: 90, at: new Date(0) }
      }),
      expected: new Date(100_000)
    },
    {
      name: 'uses the maximum interval when there are no uncompacted rows',
      state: bucketState({
        bucket_stats: { count: 100, bytes: 100n, chunks: 1 },
        last_full_compact: { op_id: 10n, count: 100, puts: 100, at: new Date(0) }
      }),
      expected: new Date(100_000)
    }
  ])('$name', ({ state, expected }) => {
    expect(fullCompactionCheckAt(state, INTERVAL_CONFIG)).toEqual(expected);
  });

  test('rejects a scheduled bucket without an uncompacted write', () => {
    expect(() => fullCompactionCheckAt(bucketState({ first_uncompacted_write: undefined }), INTERVAL_CONFIG)).toThrow(
      'Scheduled V3 bucket bucket[] has no first uncompacted write'
    );
  });

  test('chooses a full compact when its check is due', () => {
    const decision = chooseCompactionKind(
      bucketState({ bucket_stats: { count: 10, bytes: 100n, chunks: 10 } }),
      new Date(10_000),
      INTERVAL_CONFIG
    );

    expect(decision.kind).toBe(CompactionKind.Full);
    expect(decision.nextCompactCheck).toEqual({
      $min: [new Date(70_000), { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: 1_000 } }]
    });
  });

  test('chooses a chunk compact after enough chunks and the minimum interval', () => {
    const decision = chooseCompactionKind(
      bucketState({
        bucket_stats: { count: 10, bytes: 100n, chunks: 9 },
        compacted_state: compactedState({ chunks: 1 })
      }),
      new Date(5_000),
      INTERVAL_CONFIG
    );

    expect(decision.kind).toBe(CompactionKind.Chunks);
  });

  test('waits when the chunk interval has not elapsed', () => {
    const decision = chooseCompactionKind(
      bucketState({
        bucket_stats: { count: 10, bytes: 100n, chunks: 9 },
        compacted_state: compactedState({ chunks: 1, at: new Date(4_500) })
      }),
      new Date(5_000),
      INTERVAL_CONFIG
    );

    expect(decision.kind).toBeNull();
  });

  test('schedules only the full check when too few chunks were added', () => {
    const decision = chooseCompactionKind(
      bucketState({ bucket_stats: { count: 7, bytes: 70n, chunks: 7 } }),
      new Date(5_000),
      INTERVAL_CONFIG
    );

    expect(decision).toEqual({ kind: null, nextCompactCheck: new Date(70_000) });
  });

  test.each([
    {
      name: 'does not force an unspecified kind',
      forceKind: undefined,
      compactedOpId: undefined,
      maxOpIdCap: undefined,
      expected: null
    },
    {
      name: 'forces work without previous compacted state',
      forceKind: CompactionKind.Chunks,
      compactedOpId: undefined,
      maxOpIdCap: undefined,
      expected: CompactionKind.Chunks
    },
    {
      name: 'skips work already compacted through the cap',
      forceKind: CompactionKind.Chunks,
      compactedOpId: 5n,
      maxOpIdCap: 5n,
      expected: null
    },
    {
      name: 'skips work already compacted through the bucket head',
      forceKind: CompactionKind.Chunks,
      compactedOpId: 10n,
      maxOpIdCap: undefined,
      expected: null
    },
    {
      name: 'forces work beyond the compacted state',
      forceKind: CompactionKind.Chunks,
      compactedOpId: 5n,
      maxOpIdCap: 6n,
      expected: CompactionKind.Chunks
    }
  ])('$name', ({ forceKind, compactedOpId, maxOpIdCap, expected }) => {
    const state = bucketState({
      compacted_state: compactedOpId == null ? undefined : compactedState({ op_id: compactedOpId })
    });
    expect(forcedCompactionKind(state, forceKind, { maxOpIdCap })).toBe(expected);
  });

  test('derives and combines bucket statistics', () => {
    const state = bucketState({ bucket_stats: { count: 12, bytes: 120n, chunks: 3 } });
    expect(bucketStats(state)).toEqual({ count: 12, bytes: 120n, chunks: 3 });
    expect(emptyBucketStats()).toEqual({ count: 0, bytes: 0n, chunks: 0, checksum: 0 });
    expect(statsForDocument({ count: 2, size: 20, checksum: 10n })).toEqual({
      count: 2,
      bytes: 20n,
      chunks: 1,
      checksum: 10
    });
    expect(
      statsForDocuments([
        { count: 2, size: 20, checksum: 10n },
        { count: 3, size: 30, checksum: 20n }
      ])
    ).toEqual({ count: 5, bytes: 50n, chunks: 2, checksum: 30 });
    expect(
      combineAdjacentStats(
        { count: 2, bytes: 20n, chunks: 1, checksum: 10 },
        { count: 3, bytes: 30n, chunks: 2, checksum: 20 }
      )
    ).toEqual({ count: 5, bytes: 50n, chunks: 3, checksum: 30 });
  });

  test('replaces an overlapping chunk in cached statistics', () => {
    expect(
      combineChunkStats(
        compactedState({ count: 10, bytes: 100n, chunks: 4, checksum: 100n }),
        { count: 6, bytes: 60n, chunks: 2, checksum: 50 },
        { count: 3, bytes: 30n, chunks: 1, checksum: 30 }
      )
    ).toEqual({ count: 13, bytes: 130n, chunks: 5, checksum: 120 });
  });

  test('applies compacted statistics as a delta to total bucket statistics', () => {
    expect(
      applyCompactionDelta(
        { count: 20, bytes: 200n, chunks: 8 },
        { count: 5, bytes: 50n, chunks: 3, checksum: 10 },
        { count: 3, bytes: 30n, chunks: 1, checksum: 10 }
      )
    ).toEqual({ count: 18, bytes: 180n, chunks: 6 });
  });

  test('applies a stored range replacement to statistics including its checksum', () => {
    expect(
      applyStatsReplacement(
        { count: 20, bytes: 200n, chunks: 8, checksum: 100 },
        { count: 5, bytes: 50n, chunks: 3, checksum: 40 },
        { count: 3, bytes: 30n, chunks: 1, checksum: 20 }
      )
    ).toEqual({ count: 18, bytes: 180n, chunks: 6, checksum: 80 });
  });
});
