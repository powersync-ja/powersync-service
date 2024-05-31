/**
 * Manages op_id or similar sequence in memory.
 *
 * This is typically used within a transaction, with the last value persisted
 * at the end of the transaction.
 */
export class MongoIdSequence {
  private _last: bigint;

  constructor(last: bigint) {
    if (typeof last != 'bigint') {
      throw new Error(`BigInt required, got ${last} ${typeof last}`);
    }
    this._last = last;
  }

  next() {
    return ++this._last;
  }

  last() {
    return this._last;
  }
}
