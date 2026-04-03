/**
 * Replication slot WAL status from `pg_replication_slots.wal_status` (PG 13+).
 * - `reserved`: WAL needed by the slot is within `max_wal_size`
 * - `extended`: WAL needed exceeds `max_wal_size` but is protected by `max_slot_wal_keep_size`
 * - `unreserved`: WAL may be removed at next checkpoint — slot is at risk
 * - `lost`: WAL has been removed — slot is invalidated and unusable
 * - `missing`: synthetic value — the slot row is absent from `pg_replication_slots`
 */
export type WalStatus = 'reserved' | 'extended' | 'unreserved' | 'lost' | 'missing';

export class MissingReplicationSlotError extends Error {
  /** Slot WAL status at the time the error was detected. */
  walStatus: WalStatus;
  /** Replication phase when the error occurred — controls retry behavior. */
  phase: 'snapshot' | 'streaming';
  /** PG 14+ invalidation reason from `pg_replication_slots.invalidation_reason`. */
  invalidationReason?: string;

  constructor(
    message: string,
    options: {
      walStatus: WalStatus;
      phase: 'snapshot' | 'streaming';
      invalidationReason?: string;
      cause?: any;
    }
  ) {
    super(message);
    this.cause = options.cause;
    this.walStatus = options.walStatus;
    this.phase = options.phase;
    this.invalidationReason = options.invalidationReason;
  }
}

/**
 * Determines whether replication should be retried after a slot invalidation.
 *
 * Returns false when retry would be futile (e.g. slot lost during snapshot
 * due to WAL budget exhaustion — retrying would repeat the same long snapshot
 * and likely fail again).
 *
 * Blocks retry when walStatus is 'lost' during snapshot phase (unless the
 * invalidation reason is 'rows_removed', which is not a WAL budget issue).
 * Allows retry in all other cases.
 */
export function shouldRetryReplication(error: MissingReplicationSlotError): boolean {
  if (error.walStatus === 'lost' && error.phase === 'snapshot' && error.invalidationReason !== 'rows_removed') {
    return false;
  }
  return true;
}
