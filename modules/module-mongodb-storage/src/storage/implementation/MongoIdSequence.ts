import { ReplicationAssertionError } from '@powersync/lib-services-framework';

/**
 * Manages op_id or similar sequence in memory.
 *
 * This is typically used within a transaction, with the last value persisted
 * at the end of the transaction.
 */
export class MongoIdSequence {
  private _last: bigint;

  constructor(
    last: bigint,
    private readonly ranges?: readonly OpIdRange[]
  ) {
    if (typeof last != 'bigint') {
      throw new ReplicationAssertionError(`BigInt required, got ${last} ${typeof last}`);
    }
    this._last = last;
  }

  next() {
    if (this.ranges != null) {
      const next = this.ranges.find((range) => range.end > this._last);
      if (next == null) {
        throw new OpIdRangeExhausted();
      }
      this._last = this._last + 1n < next.start ? next.start : this._last + 1n;
      return this._last;
    }
    return ++this._last;
  }

  last() {
    return this._last;
  }
}

/** Inclusive, durably reserved operation IDs. Gaps between ranges are valid. */
export interface OpIdRange {
  start: bigint;
  end: bigint;
}

/** Abort the transaction before reserving more IDs and retrying its evaluation. */
export class OpIdRangeExhausted extends Error {}
