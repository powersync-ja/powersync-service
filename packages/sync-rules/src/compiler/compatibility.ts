import { Equality, listEquality, StableHasher } from './equality.js';

/**
 * Interface for structures that can be compared for equality in a way that ignores referenced result set.
 *
 * This is primarily used to compare expressions across streams. If we have two streams defined as
 * `SELECT * FROM users WHERE id = ...` , the `id` column would not be equal between those since it references a the
 * logical `users` table added to each individual statement. But if we are in a context where we know the physical table
 * is the same, this allows comparing expressions for equality.
 */
export interface EqualsIgnoringResultSet {
  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean;
  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void;
}

export const equalsIgnoringResultSet: Equality<EqualsIgnoringResultSet> = {
  equals: function (a: EqualsIgnoringResultSet, b: EqualsIgnoringResultSet): boolean {
    return a.equalsAssumingSameResultSet(b);
  },
  hash: function (hasher: StableHasher, value: EqualsIgnoringResultSet): void {
    return value.assumingSameResultSetEqualityHashCode(hasher);
  }
};

export const equalsIgnoringResultSetList = listEquality(equalsIgnoringResultSet);
export const equalsIgnoringResultSetUnordered = listEquality(equalsIgnoringResultSet);
