import {
  computeWalBudgetReport,
  formatBytes,
  formatDuration,
  formatWalBudgetLine
} from '@module/replication/wal-budget-utils.js';
import { describe, expect, test } from 'vitest';

describe('formatBytes', () => {
  test('formats bytes', () => {
    expect(formatBytes(500)).toBe('500B');
  });

  test('formats kilobytes', () => {
    expect(formatBytes(1024)).toBe('1.0KB');
    expect(formatBytes(1536)).toBe('1.5KB');
  });

  test('formats megabytes', () => {
    expect(formatBytes(1024 * 1024)).toBe('1.0MB');
    expect(formatBytes(1.5 * 1024 * 1024)).toBe('1.5MB');
  });

  test('formats gigabytes', () => {
    expect(formatBytes(1024 * 1024 * 1024)).toBe('1.0GB');
    expect(formatBytes(10.5 * 1024 * 1024 * 1024)).toBe('10.5GB');
  });
});

describe('formatDuration', () => {
  test('formats minutes', () => {
    expect(formatDuration(0.5)).toBe('30 minutes');
  });

  test('formats hours', () => {
    expect(formatDuration(1.0)).toBe('1.0 hours');
    expect(formatDuration(5.5)).toBe('5.5 hours');
  });

  test('formats days', () => {
    expect(formatDuration(24)).toBe('1.0 days');
    expect(formatDuration(48)).toBe('2.0 days');
  });
});

describe('computeWalBudgetReport', () => {
  const GB = 1024 * 1024 * 1024;

  test('computes budget percentage', () => {
    const report = computeWalBudgetReport({
      safeWalSize: 8 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      prevSample: null,
      now: 1000
    });
    expect(report.budgetRemainingPct).toBe(80);
    expect(report.isWarning).toBe(false);
  });

  test('warns at 50% threshold', () => {
    const report = computeWalBudgetReport({
      safeWalSize: 5 * GB,
      maxSize: 10 * GB,
      walStatus: 'extended',
      prevSample: null,
      now: 1000
    });
    expect(report.budgetRemainingPct).toBe(50);
    expect(report.isWarning).toBe(true);
  });

  test('warns below 50%', () => {
    const report = computeWalBudgetReport({
      safeWalSize: 4.2 * GB,
      maxSize: 10 * GB,
      walStatus: 'extended',
      prevSample: null,
      now: 1000
    });
    expect(report.budgetRemainingPct).toBe(42);
    expect(report.isWarning).toBe(true);
  });

  test('no rate without previous sample', () => {
    const report = computeWalBudgetReport({
      safeWalSize: 8 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      prevSample: null,
      now: 1000
    });
    expect(report.ratePerHour).toBeNull();
    expect(report.etaHours).toBeNull();
  });

  test('computes rate from two samples', () => {
    const twoMinutesMs = 2 * 60 * 1000;
    const report = computeWalBudgetReport({
      safeWalSize: 7 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      prevSample: { safeWalSize: 8 * GB, timestamp: 0 },
      now: twoMinutesMs
    });
    // 1 GB consumed in 2 minutes = 30 GB/hr
    expect(report.ratePerHour).toBeCloseTo(30 * GB, -5);
    // 7 GB remaining at 30 GB/hr ≈ 0.233 hours
    expect(report.etaHours).toBeCloseTo(7 / 30, 3);
  });

  test('suppresses ETA when > 48 hours', () => {
    const oneHourMs = 3_600_000;
    const report = computeWalBudgetReport({
      safeWalSize: 9.99 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      prevSample: { safeWalSize: 10 * GB, timestamp: 0 },
      now: oneHourMs
    });
    // 0.01 GB/hr → ETA = 999 hours → suppressed
    expect(report.ratePerHour).toBeCloseTo(0.01 * GB, -5);
    expect(report.etaHours).toBeNull();
  });

  test('null rate when no WAL consumed', () => {
    const report = computeWalBudgetReport({
      safeWalSize: 8 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      prevSample: { safeWalSize: 8 * GB, timestamp: 0 },
      now: 60_000
    });
    // safe_wal_size unchanged → consumed = 0 → no rate
    expect(report.ratePerHour).toBeNull();
    expect(report.etaHours).toBeNull();
  });
});

describe('formatWalBudgetLine', () => {
  const GB = 1024 * 1024 * 1024;

  test('formats basic budget line without rate', () => {
    const line = formatWalBudgetLine({
      budgetRemainingPct: 80,
      safeWalSize: 8 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      ratePerHour: null,
      etaHours: null,
      isWarning: false
    });
    expect(line).toBe('WAL budget: 8.0GB remaining of 10.0GB limit (80% remaining). Slot status: reserved.');
  });

  test('formats budget line with rate and ETA', () => {
    const line = formatWalBudgetLine({
      budgetRemainingPct: 65,
      safeWalSize: 6.5 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      ratePerHour: 1.2 * GB,
      etaHours: 5.4,
      isWarning: false
    });
    expect(line).toContain('WAL consumption: ~1.2GB/hr.');
    expect(line).toContain('ETA to exhaustion: ~5.4 hours.');
  });

  test('formats budget line with rate but no ETA (suppressed)', () => {
    const line = formatWalBudgetLine({
      budgetRemainingPct: 99,
      safeWalSize: 9.9 * GB,
      maxSize: 10 * GB,
      walStatus: 'reserved',
      ratePerHour: 0.01 * GB,
      etaHours: null,
      isWarning: false
    });
    expect(line).toContain('WAL consumption:');
    expect(line).not.toContain('ETA to exhaustion');
  });
});
