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
   * Intersection constraints to respect when adding new rows, keyed by result sets affected by them.
   *
   * Invariant: For all lookups that have already been added, no row contradicts any intersection constraint. In other
   * words, we only have to check _one_ existing column of the intersection when processing new lookups to add.
   */
  #intersections: (IntersectionConstraint[] | undefined)[];

  /**
   * @param totalLookups - The total amount of lookups that will be joined to this result set.
   */
  constructor(totalLookups: number) {
    this.#containsLookup = new Array(totalLookups).fill(false);
    this.#intersections = new Array(totalLookups).fill(undefined);

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
    rs.#intersections = this.#intersections.slice();
    rs.#rows.splice(0, 1); // Remove the initial unit row

    for (const row of this.#rows) {
      // We can shallow-clone rows, inner items are frozen once added into the result set.
      rs.#rows.push(row.slice());
    }

    return rs;
  }

  addIntersectionConstraints(constraints: Iterable<IntersectionConstraint>) {
    // To make it easy to uphold the invariant that all rows must satisfy the constraint, we only allow adding
    // intersection constraints to the initial result set.
    if (this.length != 1 || this.#rows[0].some((s) => s !== undefined)) {
      throw new Error('Can only add intersection constraints to unit result set');
    }

    for (const constraint of constraints) {
      for (const { lookup } of constraint.columns) {
        const tracked = this.#intersections[lookup.resultSetIndex];
        if (tracked != null && !tracked.includes(constraint)) {
          tracked.push(constraint);
        } else {
          this.#intersections[lookup.resultSetIndex] = [constraint];
        }
      }
    }
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
   */
  multiply(resultSetIndex: number, rows: SqliteParameterValue[][]) {
    if (rows.length === 0) {
      this.#rows = [];
      return;
    }

    using add = this.#prepareAddingResultSet(resultSetIndex);
    const originalLength = this.#rows.length;

    for (let i = 0; i < originalLength; i++) {
      if (!this.#multiplyAtRow(resultSetIndex, add.filter, i, rows)) {
        add.deletedRows.push(i);
      }
    }
  }

  /**
   * @param keys - Join keys that are already present in the result set.
   * @param resultSetIndex - The index of the resl set being joined.
   * @param performLookup - Adds resolved rows to each unique instantiation of join keys.
   */
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

    using add = this.#prepareAddingResultSet(resultSetIndex);

    const originalLength = this.#rows.length;
    for (let i = 0; i < originalLength; i++) {
      const lookup = lookupsByRow[i];
      if (lookup.foundRows.length === 0 || !this.#multiplyAtRow(resultSetIndex, add.filter, i, lookup.foundRows)) {
        // The row has no matching join partner, so remove it. We can't split it immediately because #multiplyAtRow is
        // still iterating through rows.
        add.deletedRows.push(i);
      }
    }
  }

  #intersectionFilter(intersection: IntersectionConstraint, addedResultSetIndex: number): IntersectionFilter {
    const affectedColumnsInAddedResultSet = intersection.columns.filter(
      ({ lookup }) => lookup.resultSetIndex === addedResultSetIndex
    );

    if (intersection.fixedValue !== undefined) {
      // All rows already in the result set satisfy the intersection and must match the fixed value in relevant columns.
      // So when checking a new row, we just need to check columns there.
      return function (_existingRow: ResultSetRow, added: SqliteParameterValue[]): boolean {
        for (const { outputIndex } of affectedColumnsInAddedResultSet) {
          if (added[outputIndex] !== intersection.fixedValue) return false;
        }

        return true;
      };
    } else {
      const anyExistingColumn = intersection.columns.find(({ lookup }) => this.#containsLookup[lookup.resultSetIndex]);

      return function (existingRow: ResultSetRow, added: SqliteParameterValue[]): boolean {
        let referenceValue: SqliteParameterValue | undefined;

        if (anyExistingColumn != null) {
          referenceValue = lookupInRow(existingRow, anyExistingColumn);
        }

        for (const { outputIndex } of affectedColumnsInAddedResultSet) {
          const value = added[outputIndex];

          if (referenceValue !== undefined && value !== referenceValue) return false;
          referenceValue = value;
        }

        return true;
      };
    }
  }

  #prepareAddingResultSet(addedResultSetIndex: number) {
    if (this.#containsLookup[addedResultSetIndex]) {
      throw new Error(`Already added results for ${addedResultSetIndex}`);
    }

    const filters = this.#intersections[addedResultSetIndex]?.map((intersection) =>
      this.#intersectionFilter(intersection, addedResultSetIndex)
    );

    const deletedRows: number[] = [];
    const filter = (existingRow: ResultSetRow, added: SqliteParameterValue[]): boolean => {
      if (filters == null) return true;

      return filters.every((f) => f(existingRow, added));
    };

    return {
      deletedRows,
      filter,
      [Symbol.dispose]: () => {
        let offset = 0;
        for (const toDelete of deletedRows) {
          this.#rows.splice(toDelete - offset, 1);
          offset++;
        }

        this.#containsLookup[addedResultSetIndex] = true;
      }
    };
  }

  /**
   * Adds the cartesian product of an existing row and a new result set.
   *
   * Returns false if the original row needs to be removed because no row was added (e.g. because an intersection filter
   * doesn't match).
   */
  #multiplyAtRow(
    resultSetIndex: number,
    filter: IntersectionFilter,
    rowIndex: number,
    rows: SqliteParameterValue[][]
  ): boolean {
    const originalRow = this.#rows[rowIndex];

    let isFirst = true;
    for (const row of rows) {
      if (!filter(originalRow, row)) {
        continue;
      }

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

export interface AsyncJoinLookup {
  inputs: SqliteParameterValue[];
  foundRows: SqliteParameterValue[][];
}

export interface IntersectionConstraint {
  readonly columns: ResultSetColumn[];
  readonly fixedValue?: SqliteParameterValue;
}

type IntersectionFilter = (existingRow: ResultSetRow, added: SqliteParameterValue[]) => boolean;

/**
 * A row in a result set.
 *
 * While this is semantically a list of columns, that representation would require a lot of copying on each join.
 * So, we represent each lookup result as an array of values (that we can re-use when we create new rows for joins).
 * Result sets that have not yet been processed are represented as undefined.
 */
type ResultSetRow = (ReadonlyArray<SqliteParameterValue> | undefined)[];

function lookupInRow(row: ResultSetRow, column: ResultSetColumn): SqliteParameterValue {
  const valuesForResultSet = row[column.lookup.resultSetIndex];
  if (valuesForResultSet === undefined) {
    throw new Error('Tried to lookup values set before it was joined to result set');
  }

  return valuesForResultSet[column.outputIndex];
}

const parameterArrayEquality = listEquality(StableHasher.parameterValueEquality);
