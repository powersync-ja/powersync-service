import { HashMap, listEquality, StableHasher } from '../../compiler/equality.js';
import { SqliteParameterValue } from '../../types.js';

/**
 * A mutable result set of parameter results.
 *
 * This is used to represent parameter results when resolving buckets: Each expanding lookup is joined onto a pending
 * result set until all lookups have been applied. Once all result sets have been added, bucket parameters can be read
 * by reading columns in each row.
 */
export class ResultSet {
  #containsLookup: boolean[];
  #rows: ResultSetRow[];

  /**
   * @param totalLookups - The total amount of lookups that will be joined to this result set.
   */
  constructor(totalLookups: number) {
    this.#containsLookup = new Array(totalLookups).fill(false);

    const initialRow = new Array(totalLookups);
    initialRow.fill(undefined);
    this.#rows = [initialRow];
  }

  get length(): number {
    return this.#rows.length;
  }

  clone(): ResultSet {
    const rs = new ResultSet(this.#containsLookup.length);
    rs.#containsLookup = this.#containsLookup.slice();
    rs.#rows.splice(0, 1); // Remove the initial unit row

    for (const row of this.#rows) {
      // We can shallow-clone rows, inner items are frozen once added into the result set.
      rs.#rows.push(row.slice());
    }

    return rs;
  }

  /**
   * Extracts unique values by looking values for each column in this result set.
   */
  *projectUnique(columns: ResultSetColumn[]): Iterable<SqliteParameterValue[]> {
    for (const { group, first } of this.#groupBy(columns, (values) => values)) {
      if (first) {
        yield group;
      }
    }
  }

  /**
   * Adds a new result set by forming the cartesian product with the given values.
   *
   * With {@link JoinOptions.keys}, only pairs of existing and added rows with matching keys are combined.
   */
  multiply(resultSetIndex: number, rows: SqliteParameterValue[][], options?: JoinOptions) {
    if (rows.length === 0) {
      this.#rows = [];
      return;
    }

    using add = this.#prepareAddingResultSet(resultSetIndex);
    const originalLength = this.#rows.length;
    const matching = indexRows(rows, options?.keys);

    for (let i = 0; i < originalLength; i++) {
      if (!this.#addAtRow(resultSetIndex, i, matching(this.#rows[i]), options)) {
        add.deletedRows.push(i);
      }
    }
  }

  /**
   * @param keys - Join keys that are already present in the result set.
   * @param resultSetIndex - The index of the resl set being joined.
   * @param performLookup - Adds resolved rows to each unique instantiation of join keys.
   * @param options - Additional constraints between existing rows and rows found by {@link performLookup}.
   */
  async joinAsync(
    keys: ResultSetColumn[],
    resultSetIndex: number,
    performLookup: (lookups: AsyncJoinLookup[]) => Promise<void>,
    options?: JoinOptions
  ) {
    const lookupsByRow: AsyncJoinLookup[] = [];
    const uniqueLookups: AsyncJoinLookup[] = [];

    for (const { group, first } of this.#groupBy(keys, (values) => ({ inputs: values, foundRows: [] }))) {
      if (first) uniqueLookups.push(group);
      lookupsByRow.push(group);
    }

    await performLookup(uniqueLookups);

    using add = this.#prepareAddingResultSet(resultSetIndex);

    // Rows found for a lookup are indexed once, and then re-used for all existing rows sharing that lookup.
    const indexedLookups = new Map<AsyncJoinLookup, MatchingRows>();
    const originalLength = this.#rows.length;
    for (let i = 0; i < originalLength; i++) {
      const lookup = lookupsByRow[i];
      let matching = indexedLookups.get(lookup);
      if (matching == null) {
        matching = indexRows(lookup.foundRows, options?.keys);
        indexedLookups.set(lookup, matching);
      }

      if (!this.#addAtRow(resultSetIndex, i, matching(this.#rows[i]), options)) {
        // The row has no matching join partner, so remove it. We can't split it immediately because #multiplyAtRow is
        // still iterating through rows.
        add.deletedRows.push(i);
      }
    }
  }

  #prepareAddingResultSet(addedResultSetIndex: number) {
    if (this.#containsLookup[addedResultSetIndex]) {
      throw new Error(`Already added results for ${addedResultSetIndex}`);
    }

    const deletedRows: number[] = [];

    return {
      deletedRows,
      [Symbol.dispose]: () => {
        if (deletedRows.length > 0) {
          // This is a set, so the order of rows doesn't matter. Delete indices by moving them to the end, then
          // truncating. Note that deletedRows are sorted, this processes them from the end.
          let end = this.#rows.length;
          for (let i = deletedRows.length - 1; i >= 0; i--) {
            const deleted = deletedRows[i];
            end--;
            if (deleted !== end) {
              this.#rows[deleted] = this.#rows[end];
            }
          }

          this.#rows.length = end;
        }

        this.#containsLookup[addedResultSetIndex] = true;
      }
    };
  }

  /**
   * Adds matching rows of a new result set to an existing row.
   *
   * Returns false if the original row needs to be removed because there are no matching rows.
   */
  #addAtRow(
    resultSetIndex: number,
    rowIndex: number,
    rows: readonly SqliteParameterValue[][],
    options: JoinOptions | undefined
  ): boolean {
    if (options?.semiJoin) {
      // Values of the added result set are never read, so we only need to know whether a match exists and don't
      // actually fill in values.
      return rows.length > 0;
    }

    return this.#multiplyAtRow(resultSetIndex, rowIndex, rows);
  }

  /**
   * Adds the cartesian product of an existing row and a new result set.
   *
   * Returns false if the original row needs to be removed because no row was added.
   */
  #multiplyAtRow(resultSetIndex: number, rowIndex: number, rows: readonly SqliteParameterValue[][]): boolean {
    const originalRow = this.#rows[rowIndex];

    let isFirst = true;
    for (const row of rows) {
      if (isFirst) {
        isFirst = false;

        // Add first element of product to existing row, remaining as new rows.
        originalRow[resultSetIndex] = Object.freeze(row);
      } else {
        const copy = originalRow.slice();
        copy[resultSetIndex] = Object.freeze(row);
        this.#rows.push(copy);
      }
    }

    return !isFirst;
  }

  *#groupBy<T>(columns: ResultSetColumn[], generateGroup: (values: SqliteParameterValue[]) => T) {
    const originalLength = this.#rows.length;

    if (columns.length === 1) {
      // Fast path, we can use native sets.
      const [column] = columns;
      const foundValues = new Map<SqliteParameterValue, T>();

      for (let i = 0; i < originalLength; i++) {
        const row = this.#rows[i];
        const value = lookupInRow(row, column);
        const existingGroup = foundValues.get(value);

        if (existingGroup != null) {
          yield { group: existingGroup, first: false };
        } else {
          const group = generateGroup([value]);
          foundValues.set(value, group);
          yield { group, first: true };
        }
      }
    } else {
      const foundValues = new HashMap<SqliteParameterValue[], T>(parameterArrayEquality);

      for (let i = 0; i < originalLength; i++) {
        const row = this.#rows[i];
        const values = columns.map((c) => lookupInRow(row, c));

        let isFirst = false;
        const group = foundValues.putIfAbsent(values, () => {
          isFirst = true;
          return generateGroup(values);
        });

        yield { group, first: isFirst };
      }
    }
  }
}

export interface ResultSetElement {
  resultSetIndex: number;
}

export interface ResultSetColumn {
  lookup: ResultSetElement;
  outputIndex: number;
}

/**
 * Requires a value already present in the result set to match an output of rows being added.
 */
export interface JoinKey {
  existing: ResultSetColumn;
  addedIndex: number;
}

export interface JoinOptions {
  /**
   * Constraints between existing and added rows. Rows are only combined if all keys match.
   *
   * Matching rows are found with an index on added rows, so this is more efficient than forming the cartesian product
   * and filtering afterwards.
   */
  keys: JoinKey[];
  /**
   * Whether values of the added result set are never read.
   *
   * In that case, the added result set only filters existing rows (keeping those with at least one matching row) and
   * doesn't multiply them.
   */
  semiJoin: boolean;
}

export interface AsyncJoinLookup {
  inputs: SqliteParameterValue[];
  foundRows: SqliteParameterValue[][];
}

/**
 * A row in a result set.
 *
 * While this is semantically a list of columns, that representation would require a lot of copying on each join.
 * So, we represent each lookup result as an array of values (that we can re-use when we create new rows for joins).
 *
 * Result sets that have not yet been processed, or result sets that are only needed for semi-joins, are represented as
 * undefined.
 */
type ResultSetRow = (ReadonlyArray<SqliteParameterValue> | undefined)[];

function lookupInRow(row: ResultSetRow, column: ResultSetColumn): SqliteParameterValue {
  const valuesForResultSet = row[column.lookup.resultSetIndex];
  if (valuesForResultSet === undefined) {
    throw new Error('Tried to lookup values set before it was joined to result set');
  }

  return valuesForResultSet[column.outputIndex];
}

/**
 * Returns rows to add for an existing row.
 */
type MatchingRows = (existing: ResultSetRow) => readonly SqliteParameterValue[][];

function indexRows(rows: SqliteParameterValue[][], keys: JoinKey[] | undefined): MatchingRows {
  if (keys == null || keys.length === 0) {
    return () => rows;
  }

  if (keys.length === 1) {
    // Fast path, we can use native maps.
    const [{ existing, addedIndex }] = keys;
    const index = new Map<SqliteParameterValue, SqliteParameterValue[][]>();
    for (const row of rows) {
      const key = row[addedIndex];
      const group = index.get(key);
      if (group != null) {
        group.push(row);
      } else {
        index.set(key, [row]);
      }
    }

    return (row) => index.get(lookupInRow(row, existing)) ?? [];
  } else {
    const index = new HashMap<SqliteParameterValue[], SqliteParameterValue[][]>(parameterArrayEquality);
    for (const row of rows) {
      index
        .putIfAbsent(
          keys.map((k) => row[k.addedIndex]),
          () => []
        )
        .push(row);
    }

    return (row) => index.get(keys.map((k) => lookupInRow(row, k.existing))) ?? [];
  }
}

const parameterArrayEquality = listEquality(StableHasher.parameterValueEquality);
