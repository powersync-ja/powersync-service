import { Equality, listEquality, StableHasher, unorderedEquality } from './equality.js';
import { PhysicalSourceResultSet, SourceResultSet, TableValuedResultSet } from './table.js';

/**
 * Interface for structures that can be compared for equality in a way that ignores referenced result sets.
 *
 * This is primarily used to compare expressions across streams. If we have two streams defined as
 * `SELECT * FROM users WHERE id = ...` , the `id` column would not be equal between those since it references a the
 * syntactical `users` table added to each individual statement. But if we are in a context where we know the resolved
 * physical table is the same, this allows comparing expressions for equality.
 */
export interface EqualsIgnoringPrimaryResultSet {
  equalsAssumingSamePrimaryResultSet(other: EqualsIgnoringPrimaryResultSet, identities: TableValuedIdentities): boolean;

  assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void;
}

export class TableValuedHashCodes {
  readonly #tableValued: Map<TableValuedResultSet, number>;

  constructor(tableValued: Map<TableValuedResultSet, number>) {
    this.#tableValued = tableValued;
  }

  hashTableValued(resultSet: SourceResultSet, hasher: StableHasher) {
    // There's only one primary result set which we ignore by design.
    if (resultSet instanceof PhysicalSourceResultSet) return;

    const hash = this.#tableValued.get(resultSet);
    if (hash == null) {
      throw new Error('Hashing unknown table-valued result set');
    }

    hasher.addHash(hash);
  }

  hashOrdered(inner: Iterable<EqualsIgnoringPrimaryResultSet>, hasher: StableHasher) {
    for (const element of inner) {
      element.assumingSamePrimaryResultSetEqualityHashCode(this, hasher);
    }
  }

  static readonly empty = new TableValuedHashCodes(new Map());
}

export class TableValuedIdentities {
  readonly #identities: Map<TableValuedResultSet, symbol>;

  constructor(identities: Map<TableValuedResultSet, symbol>) {
    this.#identities = identities;
  }

  identityOf(resultSet: SourceResultSet): symbol {
    if (resultSet instanceof PhysicalSourceResultSet) {
      return TableValuedIdentities.primarySymbol;
    }

    const symbol = this.#identities.get(resultSet);
    if (symbol == null) {
      throw new Error('Unknown table-valued result set for equals');
    }

    return symbol;
  }

  orderedEquals(a: Iterable<EqualsIgnoringPrimaryResultSet>, b: Iterable<EqualsIgnoringPrimaryResultSet>): boolean {
    // TODO: Reuse
    if (a === b) return true;

    const iteratorA = a[Symbol.iterator]();
    const iteratorB = b[Symbol.iterator]();

    while (true) {
      let nextA = iteratorA.next();
      let nextB = iteratorB.next();

      if (nextA.done != nextB.done) {
        return false; // Different lengths
      } else if (nextA.done) {
        return true; // Both done
      } else {
        const elementA = nextA.value;
        const elementB = nextB.value;
        if (!elementA.equalsAssumingSamePrimaryResultSet(elementB, this)) {
          return false;
        }
      }
    }
  }

  static readonly empty = new TableValuedIdentities(new Map());

  private static readonly primarySymbol = Symbol.for('primary');
}

export function resultSetIgnoringEquality(hashes: TableValuedHashCodes, identities: TableValuedIdentities) {
  const equality: Equality<EqualsIgnoringPrimaryResultSet> = {
    hash(hasher: StableHasher, value: EqualsIgnoringPrimaryResultSet): void {
      value.assumingSamePrimaryResultSetEqualityHashCode(hashes, hasher);
    },
    equals(a: EqualsIgnoringPrimaryResultSet, b: EqualsIgnoringPrimaryResultSet): boolean {
      return a.equalsAssumingSamePrimaryResultSet(b, identities);
    }
  };

  return {
    equality,
    listEquality: listEquality(equality),
    unorderedEquality: unorderedEquality(equality)
  };
}
