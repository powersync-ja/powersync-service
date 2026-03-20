import { ReplicationLagTracker } from '@/replication/ReplicationLagTracker.js';
import { describe, expect, it } from 'vitest';

describe('ReplicationLagTracker', () => {
  it('returns undefined before replication has started', () => {
    const tracker = new ReplicationLagTracker();

    expect(tracker.getLagMillis(0)).toBeUndefined();
  });

  it('tracks the oldest in-flight change and returns current lag', () => {
    const tracker = new ReplicationLagTracker();

    tracker.trackUncommittedChange(new Date(1_000));
    tracker.trackUncommittedChange(new Date(2_000));

    expect(tracker.oldestUncommittedChange?.getTime()).toBe(1_000);
    expect(tracker.getCurrentLagMillis(4_000)).toBe(3_000);
    expect(tracker.getLagMillis(4_000)).toBe(3_000);
  });

  it('records commit lag into the rolling window and clears in-flight state', () => {
    const tracker = new ReplicationLagTracker();

    tracker.trackUncommittedChange(new Date(0));
    tracker.markCommitted(5_000);

    expect(tracker.oldestUncommittedChange).toBeNull();
    expect(tracker.isStartingReplication).toBe(false);
    expect(tracker.getLagMillis(5_000)).toBe(5_000);
    expect(tracker.getLagMillis(35_000)).toBe(0);
  });

  it('can clear in-flight state without changing started state', () => {
    const tracker = new ReplicationLagTracker();

    tracker.trackUncommittedChange(new Date(0));
    tracker.clearUncommittedChange();

    expect(tracker.oldestUncommittedChange).toBeNull();
    expect(tracker.isStartingReplication).toBe(true);
    expect(tracker.getLagMillis(5_000)).toBeUndefined();
  });

  it('can mark replication as started without a committed transaction', () => {
    const tracker = new ReplicationLagTracker();

    tracker.markStarted();

    expect(tracker.isStartingReplication).toBe(false);
    expect(tracker.getLagMillis(0)).toBe(0);
  });
});
