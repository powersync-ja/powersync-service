export function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}GB`;
  } else if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  } else if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)}KB`;
  }
  return `${bytes}B`;
}

export function formatDuration(hours: number): string {
  if (hours >= 24) {
    return `${(hours / 24).toFixed(1)} days`;
  } else if (hours >= 1) {
    return `${hours.toFixed(1)} hours`;
  }
  return `${Math.round(hours * 60)} minutes`;
}

export interface WalBudgetSample {
  safeWalSize: number;
  timestamp: number;
}

export interface WalBudgetReport {
  budgetRemainingPct: number;
  safeWalSize: number;
  maxSize: number;
  walStatus: string;
  ratePerHour: number | null;
  etaHours: number | null;
  isWarning: boolean;
}

export function computeWalBudgetReport(opts: {
  safeWalSize: number;
  maxSize: number;
  walStatus: string;
  prevSample: WalBudgetSample | null;
  now: number;
}): WalBudgetReport {
  const budgetRemainingPct = Math.round((opts.safeWalSize / opts.maxSize) * 100);

  let ratePerHour: number | null = null;
  let etaHours: number | null = null;

  if (opts.prevSample != null) {
    const elapsedMs = opts.now - opts.prevSample.timestamp;
    const consumed = opts.prevSample.safeWalSize - opts.safeWalSize;
    if (elapsedMs > 0 && consumed > 0) {
      ratePerHour = (consumed / elapsedMs) * 3_600_000;
      const eta = opts.safeWalSize / ratePerHour;
      etaHours = eta < 48 ? eta : null;
    }
  }

  return {
    budgetRemainingPct,
    safeWalSize: opts.safeWalSize,
    maxSize: opts.maxSize,
    walStatus: opts.walStatus,
    ratePerHour,
    etaHours,
    isWarning: budgetRemainingPct <= 50
  };
}

export function formatWalBudgetLine(report: WalBudgetReport): string {
  let line =
    `WAL budget: ${formatBytes(report.safeWalSize)} remaining of ` +
    `${formatBytes(report.maxSize)} limit (${report.budgetRemainingPct}% remaining).`;

  if (report.ratePerHour != null) {
    line += ` WAL consumption: ~${formatBytes(report.ratePerHour)}/hr.`;
    if (report.etaHours != null) {
      line += ` ETA to exhaustion: ~${formatDuration(report.etaHours)}.`;
    }
  }

  line += ` Slot status: ${report.walStatus}.`;
  return line;
}
