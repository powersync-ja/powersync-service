import { RollingBucketMax } from '@/metrics/RollingBucketMax.js';
import { describe, expect, it } from 'vitest';

describe('RollingBucketMax', () => {
  it('returns undefined before any values are reported', () => {
    const tracker = new RollingBucketMax();

    expect(tracker.getRollingMax(0)).toBeUndefined();
  });

  it('tracks the maximum value within a single bucket', () => {
    const tracker = new RollingBucketMax();

    tracker.report(3, 100);
    tracker.report(9, 1_000);
    tracker.report(5, 4_999);

    expect(tracker.getRollingMax(4_999)).toBe(9);
  });

  it('keeps the rolling max across the last six 5s buckets', () => {
    const tracker = new RollingBucketMax();

    tracker.report(20, 0);
    tracker.report(11, 5_000);
    tracker.report(12, 10_000);
    tracker.report(13, 15_000);
    tracker.report(14, 20_000);
    tracker.report(15, 25_000);

    expect(tracker.getRollingMax(29_999)).toBe(20);
    expect(tracker.getRollingMax(30_000)).toBe(15);
  });

  it('slides the rolling max forward as older buckets age out', () => {
    const tracker = new RollingBucketMax();

    tracker.report(20, 0);
    expect(tracker.getRollingMax(0)).toBe(20);

    tracker.report(18, 5_000);
    expect(tracker.getRollingMax(5_000)).toBe(20);

    tracker.report(16, 10_000);
    expect(tracker.getRollingMax(10_000)).toBe(20);

    tracker.report(14, 15_000);
    expect(tracker.getRollingMax(15_000)).toBe(20);

    tracker.report(12, 20_000);
    expect(tracker.getRollingMax(20_000)).toBe(20);

    tracker.report(10, 25_000);
    expect(tracker.getRollingMax(29_999)).toBe(20);

    tracker.report(8, 30_000);
    expect(tracker.getRollingMax(30_000)).toBe(18);

    tracker.report(6, 35_000);
    expect(tracker.getRollingMax(35_000)).toBe(16);

    tracker.report(4, 40_000);
    expect(tracker.getRollingMax(40_000)).toBe(14);
  });

  it('keeps newer buckets in the rolling window while older peaks fall out', () => {
    const tracker = new RollingBucketMax();

    tracker.report(50, 0);
    tracker.report(11, 5_000);
    tracker.report(12, 10_000);
    tracker.report(13, 15_000);
    tracker.report(14, 20_000);
    tracker.report(15, 25_000);

    expect(tracker.getRollingMax(29_999)).toBe(50);

    tracker.report(40, 30_000);
    expect(tracker.getRollingMax(30_000)).toBe(40);
    expect(tracker.getRollingMax(34_999)).toBe(40);
  });

  it('expires values after the rolling window passes with no new reports', () => {
    const tracker = new RollingBucketMax();

    tracker.report(7, 0);

    expect(tracker.getRollingMax(29_999)).toBe(7);
    expect(tracker.getRollingMax(30_000)).toBeUndefined();
  });

  it('supports custom bucket and window sizes', () => {
    const tracker = new RollingBucketMax({
      bucketSizeMs: 1_000,
      windowSizeMs: 3_000
    });

    tracker.report(4, 0);
    tracker.report(6, 1_000);
    tracker.report(5, 2_000);

    expect(tracker.getRollingMax(2_999)).toBe(6);
    expect(tracker.getRollingMax(3_000)).toBe(6);
    expect(tracker.getRollingMax(4_000)).toBe(5);
  });
});
