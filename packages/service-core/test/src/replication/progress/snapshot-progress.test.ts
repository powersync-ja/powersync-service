import {
  aggregateTableSnapshotProgress,
  formatGlobalProgress,
  formatTableProgress,
  globalProgress,
  mergeTableProgress,
  newestObservation,
  SNAPSHOT_RATE_FRESHNESS_WINDOW_MS,
  SNAPSHOT_RATE_RESET_GAP_MS,
  TableSnapshotReplicationProgress,
  toTableSnapshotProgress
} from '@/replication/progress/snapshot-progress.js';
import { TableSnapshotStatus } from '@/storage/SourceTable.js';
import { describe, expect, test } from 'vitest';

const START = new Date('2026-10-19T10:00:00.000Z');

function at(seconds: number) {
  return new Date(START.getTime() + seconds * 1000);
}

function record(
  replicatedCount: number,
  totalEstimatedCount: number | null,
  options: { complete: boolean; rate?: number | null; updatedAt?: Date | null }
): TableSnapshotReplicationProgress {
  return toTableSnapshotProgress(options.complete, {
    replicatedCount,
    totalEstimatedCount,
    lastKey: null,
    replicationRate: options.rate ?? null,
    updatedAt: options.updatedAt ?? null
  });
}

describe('mergeTableProgress', () => {
  test('first replicated count observation only sets updatedAt', () => {
    const merged = mergeTableProgress(undefined, { replicatedCount: 1000 }, START);
    expect(merged).toEqual({
      totalEstimatedCount: null,
      replicatedCount: 1000,
      lastKey: null,
      updatedAt: START,
      replicationRate: null
    });
  });

  test('second observation sets the rate, later ones are smoothed', () => {
    let status = mergeTableProgress(undefined, { replicatedCount: 0, totalEstimatedCount: 10_000 }, START);
    status = mergeTableProgress(status, { replicatedCount: 1000 }, at(10));
    expect(status.replicationRate).toEqual(100);
    expect(status.updatedAt).toEqual(at(10));

    status = mergeTableProgress(status, { replicatedCount: 3000 }, at(20));
    // Sample of 200/s, weighted by 1 - exp(-10s / 60s)
    const alpha = 1 - Math.exp(-10 / 60);
    expect(status.replicationRate).toBeCloseTo(100 + alpha * 100, 6);
    expect(status.totalEstimatedCount).toEqual(10_000);
  });

  test('slow chunks weigh more than fast ones', () => {
    const base = mergeTableProgress(
      { replicatedCount: 0, totalEstimatedCount: 100_000, lastKey: null, updatedAt: START, replicationRate: 100 },
      { replicatedCount: 200 },
      at(1)
    );
    const slow = mergeTableProgress(
      { replicatedCount: 0, totalEstimatedCount: 100_000, lastKey: null, updatedAt: START, replicationRate: 100 },
      { replicatedCount: 12_000 },
      at(60)
    );
    // Both samples are 200/s
    expect(slow.replicationRate! - 100).toBeGreaterThan(base.replicationRate! - 100);
  });

  test('estimate-only updates leave updatedAt and rate unchanged', () => {
    const previous: TableSnapshotStatus = {
      replicatedCount: 500,
      totalEstimatedCount: 1000,
      lastKey: new Uint8Array([1]),
      updatedAt: at(10),
      replicationRate: 50
    };
    const merged = mergeTableProgress(previous, { totalEstimatedCount: 2000 }, at(100));
    expect(merged).toEqual({ ...previous, totalEstimatedCount: 2000 });
  });

  test('estimate pass followed by a much later first chunk does not produce a rate', () => {
    let status = mergeTableProgress(undefined, { totalEstimatedCount: 1000 }, START);
    expect(status.updatedAt).toBeNull();
    expect(status.replicatedCount).toEqual(0);

    // Other tables were snapshotted in the meantime
    status = mergeTableProgress(status, { replicatedCount: 100, lastKey: new Uint8Array([1]) }, at(3600));
    expect(status.updatedAt).toEqual(at(3600));
    expect(status.replicationRate).toBeNull();

    status = mergeTableProgress(status, { replicatedCount: 200 }, at(3610));
    expect(status.replicationRate).toEqual(10);
  });

  test('a re-estimate in the same update does not affect the rate', () => {
    const previous: TableSnapshotStatus = {
      replicatedCount: 1000,
      totalEstimatedCount: 5000,
      lastKey: null,
      updatedAt: START,
      replicationRate: 100
    };
    // Sample of 200/s, so the rate must move away from 100
    const withoutEstimate = mergeTableProgress(previous, { replicatedCount: 3000 }, at(10));
    const withEstimate = mergeTableProgress(previous, { replicatedCount: 3000, totalEstimatedCount: 8000 }, at(10));

    expect(withoutEstimate.replicationRate).toBeGreaterThan(100);
    expect(withEstimate.replicationRate).toEqual(withoutEstimate.replicationRate);
    expect(withEstimate.totalEstimatedCount).toEqual(8000);
  });

  describe('resets', () => {
    const previous: TableSnapshotStatus = {
      replicatedCount: 5000,
      totalEstimatedCount: 10_000,
      lastKey: null,
      updatedAt: START,
      replicationRate: 100
    };
    const gap = SNAPSHOT_RATE_RESET_GAP_MS / 1000;

    test.each([
      { name: 'a decreasing count', replicatedCount: 1000, now: at(10) },
      { name: 'a gap longer than the reset gap', replicatedCount: 6000, now: at(gap + 1) },
      { name: 'a clock moving backwards', replicatedCount: 6000, now: at(-10) }
    ])('$name resets the rate', ({ replicatedCount, now }) => {
      expect(mergeTableProgress(previous, { replicatedCount }, now)).toMatchObject({
        replicatedCount,
        updatedAt: now,
        replicationRate: null
      });
    });

    test('a slow chunk within the reset gap still counts', () => {
      expect(mergeTableProgress(previous, { replicatedCount: 6000 }, at(gap)).replicationRate).not.toBeNull();
    });
  });

  test('keeps the rate when no time has passed', () => {
    const previous: TableSnapshotStatus = {
      replicatedCount: 5000,
      totalEstimatedCount: 10_000,
      lastKey: null,
      updatedAt: START,
      replicationRate: 100
    };
    const merged = mergeTableProgress(previous, { replicatedCount: 6000 }, START);
    expect(merged.replicationRate).toEqual(100);
    expect(merged.replicatedCount).toEqual(6000);
  });

  test('distinguishes not estimated from unknown', () => {
    expect(mergeTableProgress(undefined, { replicatedCount: 10 }, START).totalEstimatedCount).toBeNull();
    expect(mergeTableProgress(undefined, { totalEstimatedCount: -1 }, START).totalEstimatedCount).toEqual(-1);
  });

  test('keeps the previous lastKey when not updated', () => {
    const lastKey = new Uint8Array([1, 2, 3]);
    const previous = mergeTableProgress(undefined, { replicatedCount: 10, lastKey }, START);
    expect(mergeTableProgress(previous, { replicatedCount: 20 }, at(1)).lastKey).toEqual(lastKey);
  });
});

describe('toTableSnapshotProgress', () => {
  test('incomplete with estimate and rate', () => {
    expect(record(250, 1000, { complete: false, rate: 50, updatedAt: START })).toEqual({
      replicatedCount: 250,
      totalEstimatedCount: 1000,
      percent: 25,
      replicationRate: 50,
      etaSeconds: 15,
      complete: false,
      updatedAt: START
    });
  });

  test('percent is rounded down', () => {
    expect(record(1999, 10_000, { complete: false }).percent).toEqual(19);
  });

  test('ETA is rounded up', () => {
    expect(record(0, 10, { complete: false, rate: 3 }).etaSeconds).toEqual(4);
  });

  test.each([
    { name: 'unknown', estimate: -1 },
    { name: 'not estimated', estimate: null }
  ])('$name total omits total, percent and ETA', ({ estimate }) => {
    expect(record(500, estimate, { complete: false, rate: 10 })).toMatchObject({
      totalEstimatedCount: null,
      percent: null,
      etaSeconds: null,
      replicationRate: 10
    });
  });

  test('no status at all', () => {
    expect(toTableSnapshotProgress(false, undefined)).toEqual({
      replicatedCount: 0,
      totalEstimatedCount: null,
      percent: null,
      replicationRate: null,
      etaSeconds: null,
      complete: false,
      updatedAt: null
    });
  });

  test('under-estimate uses the replicated count as total and caps percent at 99', () => {
    // Includes an old total_estimated_count = 0
    for (const estimate of [0, 400]) {
      const p = record(500, estimate, { complete: false, rate: 10 });
      expect(p.totalEstimatedCount).toEqual(500);
      expect(p.percent).toEqual(99);
      expect(p.etaSeconds).toEqual(0);
    }
  });

  test('percent stays at 99 until complete', () => {
    expect(record(999_999, 1_000_000, { complete: false }).percent).toEqual(99);
    expect(record(1_000_000, 1_000_000, { complete: false }).percent).toEqual(99);
  });

  test('an estimate of zero with nothing replicated is 0%', () => {
    expect(record(0, 0, { complete: false }).percent).toEqual(0);
  });

  test('zero rate has no ETA', () => {
    const p = record(500, 1000, { complete: false, rate: 0 });
    expect(p.replicationRate).toEqual(0);
    expect(p.etaSeconds).toBeNull();
  });

  test('negative or non-finite rate is ignored', () => {
    expect(record(500, 1000, { complete: false, rate: -5 }).replicationRate).toBeNull();
    expect(record(500, 1000, { complete: false, rate: NaN }).replicationRate).toBeNull();
    expect(record(500, 1000, { complete: false, rate: Infinity }).replicationRate).toBeNull();
  });

  test('complete uses actual count as total, no rate and zero ETA', () => {
    expect(record(1100, 1000, { complete: true, rate: 50, updatedAt: START })).toEqual({
      replicatedCount: 1100,
      totalEstimatedCount: 1100,
      percent: 100,
      replicationRate: null,
      etaSeconds: 0,
      complete: true,
      updatedAt: START
    });
  });

  test('complete without retained counts does not invent counts', () => {
    const p = toTableSnapshotProgress(true, undefined);
    expect(p.replicatedCount).toEqual(0);
    expect(p.totalEstimatedCount).toEqual(0);
    expect(p.complete).toBe(true);
  });
});

describe('aggregateTableSnapshotProgress', () => {
  test('sums counts instead of averaging percentages', () => {
    const records = [record(900, 1000, { complete: false }), record(0, 9000, { complete: false })];
    const p = aggregateTableSnapshotProgress(records, null);
    expect(p.replicatedCount).toEqual(900);
    expect(p.totalEstimatedCount).toEqual(10_000);
    // Averaging the percentages would give 45%
    expect(p.percent).toEqual(9);
    expect(p.complete).toBe(false);
  });

  test('shared and new records of one table (section 2.1)', () => {
    // Unfinished record shared with the ACTIVE config, plus a new record for added definitions
    let shared = record(400, 1000, { complete: false, rate: 100, updatedAt: at(0) });
    let added = record(0, 1000, { complete: false });
    let p = aggregateTableSnapshotProgress([shared, added], newestObservation([shared, added]));
    expect(p).toMatchObject({ replicatedCount: 400, totalEstimatedCount: 2000, percent: 20, complete: false });
    expect(p.etaSeconds).toEqual(16);

    // Completing the shared record retains its contribution while the new record finishes
    shared = record(1050, 1000, { complete: true, updatedAt: at(10) });
    added = record(200, 1000, { complete: false, rate: 40, updatedAt: at(15) });
    p = aggregateTableSnapshotProgress([shared, added], newestObservation([shared, added]));
    expect(p).toMatchObject({
      replicatedCount: 1250,
      totalEstimatedCount: 2050,
      percent: 60,
      replicationRate: 40,
      etaSeconds: 20,
      complete: false,
      updatedAt: at(15)
    });

    added = record(980, 1000, { complete: true, updatedAt: at(40) });
    p = aggregateTableSnapshotProgress([shared, added], newestObservation([shared, added]));
    expect(p).toEqual({
      replicatedCount: 2030,
      totalEstimatedCount: 2030,
      percent: 100,
      replicationRate: null,
      etaSeconds: 0,
      complete: true,
      updatedAt: at(40)
    });
  });

  test('an unknown total on any record omits the table total, percent and ETA', () => {
    const p = aggregateTableSnapshotProgress(
      [record(100, 1000, { complete: false, rate: 10, updatedAt: START }), record(50, -1, { complete: false })],
      START
    );
    expect(p).toMatchObject({
      replicatedCount: 150,
      totalEstimatedCount: null,
      percent: null,
      etaSeconds: null,
      replicationRate: 10
    });
  });

  test('sums fresh rates of incomplete records and derives ETA from combined remaining work', () => {
    const records = [
      record(0, 1000, { complete: false, rate: 10, updatedAt: at(0) }),
      record(0, 1000, { complete: false, rate: 30, updatedAt: at(5) }),
      record(500, 500, { complete: true, rate: 1000, updatedAt: at(5) })
    ];
    const p = aggregateTableSnapshotProgress(records, at(5));
    expect(p.replicationRate).toEqual(40);
    expect(p.etaSeconds).toEqual(50);
  });

  test('uses the given config-wide reference for freshness', () => {
    const window = SNAPSHOT_RATE_FRESHNESS_WINDOW_MS / 1000;
    const records = [record(0, 1000, { complete: false, rate: 10, updatedAt: at(0) })];
    expect(aggregateTableSnapshotProgress(records, at(window)).replicationRate).toEqual(10);
    // Another table of the same config was observed much later: this record's rate is stale
    expect(aggregateTableSnapshotProgress(records, at(window + 1)).replicationRate).toBeNull();
  });

  test('records without a rate count as zero, unknown only if none has a rate', () => {
    expect(
      aggregateTableSnapshotProgress(
        [
          record(0, 1000, { complete: false, updatedAt: START }),
          record(0, 1000, { complete: false, rate: 5, updatedAt: START })
        ],
        START
      ).replicationRate
    ).toEqual(5);
    expect(
      aggregateTableSnapshotProgress([record(0, 1000, { complete: false, updatedAt: START })], START).replicationRate
    ).toBeNull();
  });

  test('empty input is not complete', () => {
    expect(aggregateTableSnapshotProgress([], null)).toMatchObject({
      complete: false,
      replicatedCount: 0,
      percent: null
    });
  });
});

describe('globalProgress', () => {
  test('worked example A / B', () => {
    // A at 500
    let a = record(500, 1000, { complete: false, rate: 100, updatedAt: at(5) });
    let b = record(0, 3000, { complete: false });
    expect(globalProgress([a, b])).toMatchObject({ replicatedCount: 500, totalEstimatedCount: 4000, percent: 12 });

    // A done (actual 1,100). 1,100 / 4,100 = 26.8%, rounded down.
    a = record(1100, 1000, { complete: true, updatedAt: at(11) });
    let p = globalProgress([a, b]);
    expect(p).toMatchObject({ replicatedCount: 1100, totalEstimatedCount: 4100, percent: 26 });
    // Short gap at the record boundary: no rate until B's second chunk
    expect(p.replicationRate).toBeNull();
    expect(p.etaSeconds).toBeNull();

    // B at 1,500
    b = record(1500, 3000, { complete: false, rate: 136, updatedAt: at(30) });
    p = globalProgress([a, b]);
    expect(p).toMatchObject({
      replicatedCount: 2600,
      totalEstimatedCount: 4100,
      percent: 63,
      replicationRate: 136,
      etaSeconds: 12,
      updatedAt: at(30)
    });

    // B done (actual 2,900)
    b = record(2900, 3000, { complete: true, updatedAt: at(40) });
    expect(globalProgress([a, b])).toEqual({
      replicatedCount: 4000,
      totalEstimatedCount: 4000,
      percent: 100,
      replicationRate: null,
      etaSeconds: 0,
      updatedAt: at(40)
    });
  });

  test('omits the total while any record is not estimated', () => {
    expect(
      globalProgress([record(100, 1000, { complete: false }), record(0, null, { complete: false })])
    ).toMatchObject({
      totalEstimatedCount: null,
      percent: null,
      etaSeconds: null
    });
  });

  test('excludes stale rates from interrupted records', () => {
    const window = SNAPSHOT_RATE_FRESHNESS_WINDOW_MS / 1000;
    const p = globalProgress([
      record(100, 1000, { complete: false, rate: 500, updatedAt: at(0) }),
      record(100, 1000, { complete: false, rate: 20, updatedAt: at(window + 60) })
    ]);
    expect(p.replicationRate).toEqual(20);
    expect(p.etaSeconds).toEqual(90);
  });

  test('no records', () => {
    expect(globalProgress([])).toEqual({
      replicatedCount: 0,
      totalEstimatedCount: null,
      percent: null,
      replicationRate: null,
      etaSeconds: null,
      updatedAt: null
    });
  });
});

describe('formatting', () => {
  test('table progress', () => {
    expect(
      formatTableProgress({
        replicatedCount: 1_200_000,
        totalEstimatedCount: 5_000_000,
        percent: 24,
        replicationRate: 8100,
        etaSeconds: 469,
        complete: false,
        updatedAt: START
      })
    ).toEqual('1.2M/~5.0M (24%, 8.1k records/s, ETA 7m49s)');

    expect(formatTableProgress(record(50, -1, { complete: false }))).toEqual('50/?');
    expect(formatTableProgress(record(50, -1, { complete: false, rate: 2.5 }))).toEqual('50/? (2.5 records/s)');
    expect(formatTableProgress(record(50, 100, { complete: false }))).toEqual('50/~100 (50%)');
    expect(formatTableProgress(record(999_950, 2_000_000_000, { complete: false }))).toEqual('1.0M/~2.0B (0%)');
    expect(formatTableProgress(record(1100, 1000, { complete: true }))).toEqual('1.1k (100%)');
  });

  test('durations', () => {
    const eta = (etaSeconds: number) => formatTableProgress({ ...record(0, 100, { complete: false }), etaSeconds });
    expect(eta(0)).toEqual('0/~100 (0%, ETA 0m00s)');
    expect(eta(3599)).toEqual('0/~100 (0%, ETA 59m59s)');
    expect(eta(3600 * 2 + 5 * 60)).toEqual('0/~100 (0%, ETA 2h05m)');
    expect(eta(86400 * 3 + 3600 * 4)).toEqual('0/~100 (0%, ETA 3d04h)');
  });

  test('global progress', () => {
    const a = record(1100, 1000, { complete: true, updatedAt: at(11) });
    const b = record(1500, 3000, { complete: false, rate: 136, updatedAt: at(30) });
    expect(formatGlobalProgress(globalProgress([a, b]), { complete: 1, total: 2 })).toEqual(
      '2.6k/~4.1k records (63%, 136 records/s, ETA 0m12s), 1/2 tables complete'
    );
    expect(formatGlobalProgress(globalProgress([a]))).toEqual('1.1k records (100%)');
  });
});
