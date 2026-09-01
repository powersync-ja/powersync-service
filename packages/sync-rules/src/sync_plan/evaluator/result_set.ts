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
  #totalLookups: number;
  #rows: ResultSetRow[];

  constructor(totalLookups: number) {
    this.#totalLookups = totalLookups;
    const initialRow = new Array(totalLookups);
    initialRow.fill(undefined);
    this.#rows = [initialRow];
  }

  get length(): number {
    return this.#rows.length;
  }

  clone(): ResultSet {
    const rs = new ResultSet(this.#totalLookups);
    rs.#rows.splice(0, 1); // Remove the initial unit row

    for (const row of this.#rows) {
      rs.#rows.push([...row]);
    }
    return rs;
  }

  /**
   * Extracts unique values by looking values for each column in this result set.
   *
   * For each unique projection row, also returns all source rows the value was derived from.
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
   */
  multiply(resultSetIndex: number, rows: SqliteParameterValue[][]) {
    if (rows.length === 0) {
      this.#rows = [];
    }

    const originalLength = this.#rows.length;
    for (let i = 0; i < originalLength; i++) {
      this.#multiplyAtRow(resultSetIndex, i, rows);
    }
  }

  /**
   * Removes rows where the given columns have different values.
   *
   * If a fixed value is passed, this also removes rows where any of the given columns has a different value.
   */
  formIntersection(columns: ResultSetColumn[], fixedValue?: SqliteParameterValue) {
    row: for (let i = 0; i < this.#rows.length; i++) {
      const row = this.#rows[i];
      let requiredValue = fixedValue;

      for (const column of columns) {
        const evaluated = lookupInRow(row, column);
        if (requiredValue !== undefined && evaluated != requiredValue) {
          // This row needs to be removed!
          this.#rows.splice(i, 1);
          i--;
          continue row;
        }

        requiredValue = evaluated;
      }
    }
  }

  async joinAsync(
    keys: ResultSetColumn[],
    resultSetIndex: number,
    performLookup: (lookups: AsyncJoinLookup[]) => Promise<void>
  ) {
    const lookupsByRow: AsyncJoinLookup[] = [];
    const uniqueLookups: AsyncJoinLookup[] = [];

    for (const { group, first } of this.#groupBy(keys, (values) => ({ inputs: values, foundRows: [] }))) {
      if (first) uniqueLookups.push(group);
      lookupsByRow.push(group);
    }

    await performLookup(uniqueLookups);

    const deletedRows: number[] = [];
    const originalLength = this.#rows.length;
    for (let i = 0; i < originalLength; i++) {
      const lookup = lookupsByRow[i];
      if (lookup.foundRows.length > 0) {
        this.#multiplyAtRow(resultSetIndex, i, lookup.foundRows);
      } else {
        // The row has no matching join partner, so remove it. We can't split it immediately because #multiplyAtRow is
        // still iterating through rows.
        deletedRows.push(i);
      }
    }

    let offset = 0;
    for (const toDelete of deletedRows) {
      this.#rows.splice(toDelete - offset, 1);
      offset++;
    }
  }

  #multiplyAtRow(resultSetIndex: number, rowIndex: number, rows: SqliteParameterValue[][]) {
    // Add first element of product to existing row, remaining as new rows.
    const row = this.#rows[rowIndex];
    row[resultSetIndex] = rows[0];

    for (let j = 1; j < rows.length; j++) {
      const copy = Array.from(row);
      copy[resultSetIndex] = rows[j];
      this.#rows.push(copy);
    }
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
          yield { index: i, group: existingGroup, first: false };
        } else {
          const group = generateGroup([value]);
          foundValues.set(value, group);
          yield { index: i, group, first: true };
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

        yield { index: i, group, first: isFirst };
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

export interface AsyncJoinLookup {
  inputs: SqliteParameterValue[];
  foundRows: SqliteParameterValue[][];
}

/**
 * A row in a result set.
 *
 * While this is semantically a list of columns, that representation would require a lot of copying on each join.
 * So, we represent each lookup result as an array of values (that we can re-use when we create new rows for joins).
 * Result sets that have not yet been processed are represented as undefined.
 */
type ResultSetRow = (SqliteParameterValue[] | undefined)[];

function lookupInRow(row: ResultSetRow, column: ResultSetColumn): SqliteParameterValue {
  return row[column.lookup.resultSetIndex]![column.outputIndex];
}

const parameterArrayEquality = listEquality(StableHasher.parameterValueEquality);
