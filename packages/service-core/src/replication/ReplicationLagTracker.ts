import { RollingBucketMax } from '../metrics/RollingBucketMax.js';

/**
 * Tracks replication lag across the current in-flight transaction and a rolling
 * max of recently observed lag values.
 */
export class ReplicationLagTracker {
  private readonly rollingReplicationLag = new RollingBucketMax();
  private _oldestUncommittedChange: Date | null = null;
  private _isStartingReplication = true;

  /**
   * The oldest source timestamp still part of the current in-flight work.
   */
  get oldestUncommittedChange(): Date | null {
    return this._oldestUncommittedChange;
  }

  /**
   * True until replication has seen its first completed commit or equivalent keepalive.
   */
  get isStartingReplication(): boolean {
    return this._isStartingReplication;
  }

  /**
   * Registers the first source timestamp for the current in-flight work,
   * for example the start of a transaction
   */
  trackUncommittedChange(timestamp: Date | null | undefined): void {
    if (this._oldestUncommittedChange == null && timestamp != null) {
      this._oldestUncommittedChange = timestamp;
    }
  }

  /**
   * Clears the current in-flight timestamp without changing startup state.
   */
  clearUncommittedChange(): void {
    this._oldestUncommittedChange = null;
  }

  /**
   * Marks replication as started even if no committed transaction lag was recorded.
   */
  markStarted(): void {
    this._isStartingReplication = false;
  }

  /**
   * Mark the current pending changes as "committed".
   *
   * Records the current in-flight lag into the rolling window and clears it.
   * The current lag is calculated as the differnence between current time and the oldest change,
   * as marked by trackUncommittedChange.
   */
  markCommitted(timestampMs = Date.now()): void {
    if (this._oldestUncommittedChange != null) {
      this.rollingReplicationLag.report(timestampMs - this._oldestUncommittedChange.getTime(), timestampMs);
    }
    this.clearUncommittedChange();
    this.markStarted();
  }

  /**
   * Returns the lag for the current in-flight work.
   *
   * 0 if idle (no pending changes to replicate).
   *
   * undefined when replication is still starting up.
   */
  getCurrentLagMillis(timestampMs = Date.now()): number | undefined {
    if (this._oldestUncommittedChange == null) {
      return this._isStartingReplication ? undefined : 0;
    }
    return timestampMs - this._oldestUncommittedChange.getTime();
  }

  /**
   * Returns the rolling lag metric value, including the current in-flight lag when present.
   */
  getLagMillis(timestampMs = Date.now()): number | undefined {
    this.rollingReplicationLag.report(this.getCurrentLagMillis(timestampMs), timestampMs);
    return this.rollingReplicationLag.getRollingMax(timestampMs);
  }
}
