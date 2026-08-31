import { Equality, listEquality, orderedEquals, StableHasher, unorderedEquality } from './equality.js';
import { PhysicalSourceResultSet, SourceResultSet, TableValuedResultSet } from './table.js';

/**
 * A semantic equality operator for syntactic structures.
 *
 * This is primarily used to compare expressions across streams. If we have two streams defined as
 * `SELECT * FROM users WHERE id = ...` , the `id` column would not be equal between those since it references a the
 * syntactical `users` table added to each individual statement. But if we are in a context where we know the resolved
 * physical table is the same, this allows comparing expressions for equality.
 *
 * It is the responsibility of the caller to also check the primary result set (like `users` in this example) for
 * equality before invoking these methods. Additionally, streams are allowed to join table-valued functions derived from
 * the primary result set. Those must also be equal, which is why {@link TableValuedHashCodes} is used to generate
 * stable hash codes for them and {@link TableValuedIdentities} guarantees that available table-valued functions have a
 * 1:1 correspondence between compared structures.
 */
export interface EqualsIgnoringPrimaryResultSet {
  equalsAssumingSamePrimaryResultSet(other: EqualsIgnoringPrimaryResultSet, identities: TableValuedIdentities): boolean;

  assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void;
}

/**
 * Pre-computed hash codes for table-valued functions available to a {@link EqualsIgnoringPrimaryResultSet}.
 *
 * This is used to embed hashes for table-valued functions when their columns are referenced.
 */
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

/**
 * When comparing two bucket data or parameter processors that can both have table-valued functions on them, an identity
 * for those functions that is valid for both processors.
 *
 * This allows testing expressions referencing table-valued functions for equality across different queries: A caller
 * first verifies that added table-valued functions are equal on the given queries by constructing an identity mapping
 * so that `identityOf(tableValuedFunctionInA) == identityOf(tableValuedFunctionInB)` iff `tableValuedFunctionInA` and
 * `tableValuedFunctionInB` are equivalent.
 */
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
    return orderedEquals(a, b, (elementA, elementB) => elementA.equalsAssumingSamePrimaryResultSet(elementB, this));
  }

  static readonly empty = new TableValuedIdentities(new Map());

  private static readonly primarySymbol = Symbol.for('primary');
}

/**
 * An equals and hash code operator for {@link EqualsIgnoringPrimaryResultSet} by binding hash codes and identities for
 * table-valued functions.
 */
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
