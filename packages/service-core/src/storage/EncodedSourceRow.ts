import { SqliteRow, ToastableSqliteRow } from '@powersync/service-sync-rules';

/**
 * Encodings of {@link EncodedSourceRow}s.
 *
 *  - `bson`: A BSON document, as received from MongoDB.
 */
export type SourceRowFormat = 'bson';

/**
 * A source row in its source encoding, passed to storage instead of a decoded {@link SqliteRow}.
 *
 * Storage implementations that can evaluate rows in their encoded form (MongoDB storage with the Rust addon) use
 * {@link bytes} directly. Everything else calls {@link toSqliteRow}, which decodes the row once and caches the result.
 *
 * Encoded rows are always complete: They never contain TOAST values.
 */
export class EncodedSourceRow {
  #decode: (() => SqliteRow) | null;
  #decoded: SqliteRow | null = null;

  constructor(
    readonly format: SourceRowFormat,
    /**
     * The encoded row. This may be a view into a larger buffer (e.g. a response from the source database), and must
     * not be modified.
     */
    readonly bytes: Uint8Array,
    /**
     * Decodes {@link bytes} into a row, with the same semantics as the source's regular row conversion.
     */
    decode: () => SqliteRow
  ) {
    this.#decode = decode;
  }

  /**
   * Returns the decoded row, decoding it on first use.
   */
  toSqliteRow(): SqliteRow {
    if (this.#decoded == null) {
      this.#decoded = this.#decode!();
      // Allow the closure (and anything it captures) to be garbage-collected.
      this.#decode = null;
    }
    return this.#decoded;
  }
}

/**
 * A row passed to storage: Either decoded, or in its source encoding.
 */
export type SourceRow<R extends ToastableSqliteRow = SqliteRow> = R | EncodedSourceRow;

/**
 * Returns the decoded values of a source row, decoding it if necessary.
 */
export function decodeSourceRow<R extends ToastableSqliteRow>(row: SourceRow<R>): R | SqliteRow;
export function decodeSourceRow<R extends ToastableSqliteRow>(row: SourceRow<R> | undefined): R | SqliteRow | undefined;
export function decodeSourceRow<R extends ToastableSqliteRow>(
  row: SourceRow<R> | undefined
): R | SqliteRow | undefined {
  return row instanceof EncodedSourceRow ? row.toSqliteRow() : row;
}

/**
 * Describes the `id` of a source row in log messages, without decoding encoded rows for that.
 */
export function sourceRowLogId(row: SourceRow<ToastableSqliteRow> | undefined, replicaId: unknown): unknown {
  return row instanceof EncodedSourceRow ? replicaId : row?.id;
}
