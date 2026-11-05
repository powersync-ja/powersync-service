import type { TableSnapshotStatus } from '../../storage/SourceTable.js';

/**
 * Time constant of the replication rate moving window (EWMA).
 *
 * A sample taken Δt after the previous one gets weight `1 - exp(-Δt / τ)`, so slow chunks weigh more than fast ones.
 */
export const SNAPSHOT_RATE_TIME_CONSTANT_MS = 60_000;

/**
 * A gap between two replicated count observations larger than this resets the rate, e.g. on resume after a restart.
 */
export const SNAPSHOT_RATE_RESET_GAP_MS = 5 * 60_000;

/**
 * Only records observed within this window of the newest observation contribute their rate to an aggregate,
 * so that a record left half-done by an interrupted run doesn't keep adding a stale rate.
 */
export const SNAPSHOT_RATE_FRESHNESS_WINDOW_MS = 5 * 60_000;

/**
 *  Snapshot Replication progress
 */
export interface SnapshotReplicationProgress {
  replicatedCount: number;
  /**
   * Null when unknown or not estimated. Equal to `replicatedCount` once complete.
   */
  totalEstimatedCount: number | null;
  /**
   * Whole number, rounded down. At most 99 until replication is complete.
   */
  percent: number | null;
  /**
   * Records / second. Null once complete.
   */
  replicationRate: number | null;
  /**
   * Seconds from `updatedAt` until expected completion. 0 once complete.
   */
  etaSeconds: number | null;
  updatedAt: Date | null;
}

/**
 * Normalized replication snapshot progress for a table
 */
export interface TableSnapshotReplicationProgress extends SnapshotReplicationProgress {
  complete: boolean;
}

/**
 * Merge a progress update into the previous status of a snapshot record.
 *
 * - The first `replicatedCount` observation only sets `updatedAt`; the rate needs a second observation.
 * - The rate is reset when `replicatedCount` decreases (non-resumable restart), or when the previous observation is
 *   older than {@link SNAPSHOT_RATE_RESET_GAP_MS} (resume after a restart).
 * - Estimate-only updates (no `replicatedCount`) leave `updatedAt` and `replicationRate` unchanged, so that an
 *   up-front estimate pass does not become the baseline for the first chunk.
 * - A missing total stays "not estimated". It is not defaulted to `0`.
 */
export function mergeTableProgress(
  previous: TableSnapshotStatus | undefined,
  update: Partial<TableSnapshotStatus>,
  now: Date
): TableSnapshotStatus {
  const merged: TableSnapshotStatus = {
    totalEstimatedCount: update.totalEstimatedCount ?? previous?.totalEstimatedCount ?? null,
    replicatedCount: update.replicatedCount ?? previous?.replicatedCount ?? 0,
    lastKey: update.lastKey ?? previous?.lastKey ?? null,
    updatedAt: previous?.updatedAt ?? null,
    replicationRate: previous?.replicationRate ?? null
  };

  if (update.replicatedCount == null) {
    // Estimate-only update
    return merged;
  }

  merged.updatedAt = now;
  const previousAt = previous?.updatedAt;
  if (previousAt == null) {
    // First observation: baseline only
    merged.replicationRate = null;
    return merged;
  }

  const elapsedMs = now.getTime() - previousAt.getTime();
  const previousCount = previous!.replicatedCount;
  if (update.replicatedCount < previousCount || elapsedMs < 0 || elapsedMs > SNAPSHOT_RATE_RESET_GAP_MS) {
    // Restarted snapshot, clock moved backwards, or resumed after a long gap: restart the measurement from here.
    merged.replicationRate = null;
    return merged;
  }
  if (elapsedMs == 0) {
    // No time base for a sample. Keep the current rate.
    return merged;
  }

  const sample = ((update.replicatedCount - previousCount) * 1000) / elapsedMs;
  const previousRate = previous!.replicationRate;
  if (previousRate == null || !Number.isFinite(previousRate) || previousRate < 0) {
    merged.replicationRate = sample;
  } else {
    const alpha = 1 - Math.exp(-elapsedMs / SNAPSHOT_RATE_TIME_CONSTANT_MS);
    merged.replicationRate = previousRate + alpha * (sample - previousRate);
  }
  return merged;
}

/**
 * Normalize the progress of one snapshot record.
 *
 * A total below the replicated count (an under-estimate) is treated as equal to the replicated count.
 */
export function toTableSnapshotProgress(
  complete: boolean,
  status: TableSnapshotStatus | undefined
): TableSnapshotReplicationProgress {
  const replicatedCount = status?.replicatedCount ?? 0;
  const updatedAt = status?.updatedAt ?? null;
  if (complete) {
    return {
      replicatedCount,
      totalEstimatedCount: replicatedCount,
      percent: 100,
      replicationRate: null,
      etaSeconds: 0,
      complete: true,
      updatedAt
    };
  }

  const estimate = status?.totalEstimatedCount;
  const totalEstimatedCount = estimate == null || estimate < 0 ? null : Math.max(estimate, replicatedCount);
  const rate = status?.replicationRate;
  const replicationRate = rate == null || !Number.isFinite(rate) || rate < 0 ? null : rate;
  return {
    replicatedCount,
    totalEstimatedCount,
    percent: incompletePercent(replicatedCount, totalEstimatedCount),
    replicationRate,
    etaSeconds: etaSeconds(replicatedCount, totalEstimatedCount, replicationRate),
    complete: false,
    updatedAt
  };
}

/**
 * Combine the snapshot records of one physical table.
 *
 * Counts and totals are summed, not averaged. The table is complete only when every record is complete. The rate is
 * the sum of fresh rates of incomplete records, with freshness relative to `newestObservation` (the newest observation
 * across all records of the sync config).
 */
export function aggregateTableSnapshotProgress(
  records: TableSnapshotReplicationProgress[],
  newestObservation: Date | null
): TableSnapshotReplicationProgress {
  const combined = combineProgress(records, newestObservation);
  return { ...combined, complete: records.length > 0 && records.every((record) => record.complete) };
}

/**
 *  Calculate the global snapshot progress based on all the supplied table snapshot progress records.
 */
export function globalProgress(records: TableSnapshotReplicationProgress[]): SnapshotReplicationProgress {
  return combineProgress(records, newestObservation(records));
}

function combineProgress(
  records: TableSnapshotReplicationProgress[],
  newestObservation: Date | null
): SnapshotReplicationProgress {
  let replicatedCount = 0;
  // No records yet means nothing is estimated yet
  let totalEstimatedCount: number | null = records.length == 0 ? null : 0;
  let replicationRate: number | null = null;
  let updatedAt: Date | null = null;
  let complete = records.length > 0;

  for (const record of records) {
    replicatedCount += record.replicatedCount;
    if (record.totalEstimatedCount == null || totalEstimatedCount == null) {
      totalEstimatedCount = null;
    } else {
      totalEstimatedCount += record.totalEstimatedCount;
    }
    if (record.updatedAt != null && (updatedAt == null || record.updatedAt > updatedAt)) {
      updatedAt = record.updatedAt;
    }
    if (!record.complete) {
      complete = false;
      if (isFresh(record.updatedAt, newestObservation) && record.replicationRate != null) {
        replicationRate = (replicationRate ?? 0) + record.replicationRate;
      }
    }
  }

  if (complete) {
    return {
      replicatedCount,
      totalEstimatedCount: replicatedCount,
      percent: 100,
      replicationRate: null,
      etaSeconds: 0,
      updatedAt
    };
  }
  return {
    replicatedCount,
    totalEstimatedCount,
    percent: incompletePercent(replicatedCount, totalEstimatedCount),
    replicationRate,
    etaSeconds: etaSeconds(replicatedCount, totalEstimatedCount, replicationRate),
    updatedAt
  };
}

function isFresh(updatedAt: Date | null, newestObservation: Date | null): boolean {
  if (updatedAt == null || newestObservation == null) {
    return false;
  }
  return newestObservation.getTime() - updatedAt.getTime() <= SNAPSHOT_RATE_FRESHNESS_WINDOW_MS;
}

function incompletePercent(replicatedCount: number, totalEstimatedCount: number | null): number | null {
  if (totalEstimatedCount == null) {
    return null;
  } else if (totalEstimatedCount <= 0) {
    return 0;
  }
  return Math.min(99, Math.floor((replicatedCount * 100) / totalEstimatedCount));
}

function etaSeconds(
  replicatedCount: number,
  totalEstimatedCount: number | null,
  replicationRate: number | null
): number | null {
  if (totalEstimatedCount == null || replicationRate == null || replicationRate <= 0) {
    return null;
  }
  return Math.ceil(Math.max(0, totalEstimatedCount - replicatedCount) / replicationRate);
}

/**
 * The newest `updatedAt` across the given records.
 *
 * Use this over all records of a sync config as the freshness reference for {@link aggregateTableSnapshotProgress}, so that
 * table and global rates agree.
 */
export function newestObservation(records: TableSnapshotReplicationProgress[]): Date | null {
  let newest: Date | null = null;
  for (const record of records) {
    if (record.updatedAt != null && (newest == null || record.updatedAt > newest)) {
      newest = record.updatedAt;
    }
  }
  return newest;
}

/**
 * Format per-record or per-table progress for logs.
 *
 * Example: `1.2M/~5.0M (24%, 8.1k records/s, ETA 7m49s)`.
 */
export function formatTableProgress(progress: TableSnapshotReplicationProgress): string {
  return formatProgress(progress, progress.complete, '');
}

/**
 * Format sync config progress for logs.
 *
 * Example: `2.6k/~4.1k records (63%, 136 records/s, ETA 0m11s), 1/2 tables complete`.
 */
export function formatGlobalProgress(
  progress: SnapshotReplicationProgress,
  tables?: { complete: number; total: number }
): string {
  const formatted = formatProgress(progress, progress.percent == 100, ' records');
  return tables == null ? formatted : `${formatted}, ${tables.complete}/${tables.total} tables complete`;
}

function formatProgress(progress: SnapshotReplicationProgress, complete: boolean, unit: string): string {
  if (complete) {
    return `${formatCount(progress.replicatedCount)}${unit} (100%)`;
  }
  const total = progress.totalEstimatedCount == null ? '?' : `~${formatCount(progress.totalEstimatedCount)}`;
  const details: string[] = [];
  if (progress.percent != null) {
    details.push(`${progress.percent}%`);
  }
  if (progress.replicationRate != null) {
    details.push(`${formatCount(progress.replicationRate)} records/s`);
  }
  if (progress.etaSeconds != null) {
    details.push(`ETA ${formatDuration(progress.etaSeconds)}`);
  }
  const suffix = details.length == 0 ? '' : ` (${details.join(', ')})`;
  return `${formatCount(progress.replicatedCount)}/${total}${unit}${suffix}`;
}

const COUNT_UNITS: [number, string][] = [
  [1e9, 'B'],
  [1e6, 'M'],
  [1e3, 'k']
];

/**
 * Compact count: `950`, `8.1k`, `1.2M`.
 */
function formatCount(n: number): string {
  for (const [size, suffix] of COUNT_UNITS) {
    // Compare after rounding, so that 999,950 formats as 1.0M rather than 1000.0k
    if (Math.round((n / size) * 10) >= 10) {
      return `${(n / size).toFixed(1)}${suffix}`;
    }
  }
  return n < 10 && !Number.isInteger(n) ? n.toFixed(1) : Math.round(n).toString();
}

/**
 * Compact duration: `0m11s`, `7m49s`, `2h05m`, `3d04h`.
 */
function formatDuration(totalSeconds: number): string {
  const seconds = Math.ceil(totalSeconds);
  const pad = (n: number) => n.toString().padStart(2, '0');
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m${pad(seconds % 60)}s`;
  } else if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h${pad(Math.floor((seconds % 3600) / 60))}m`;
  } else {
    return `${Math.floor(seconds / 86400)}d${pad(Math.floor((seconds % 86400) / 3600))}h`;
  }
}
