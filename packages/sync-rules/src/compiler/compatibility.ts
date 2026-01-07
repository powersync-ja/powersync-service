import { Equality, listEquality, StableHasher } from './equality.js';

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
