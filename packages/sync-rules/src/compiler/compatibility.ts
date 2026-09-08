import { Equality, listEquality, StableHasher, unorderedEquality } from './equality.js';
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
  equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    tableValued: TableValuedFunctionEquality
  ): boolean;

  assumingSamePrimaryResultSetEqualityHashCode(tableValued: TableValuedFunctionEquality, hasher: StableHasher): void;
}

interface ReadOnlyMap<K, V> {
  get(key: K): V | undefined;
}

export class TableValuedFunctionEquality implements Equality<EqualsIgnoringPrimaryResultSet> {
  readonly #hashes: ReadOnlyMap<TableValuedResultSet, number>;
  readonly #identities: ReadOnlyMap<TableValuedResultSet, unknown>;

  #ordered: Equality<Iterable<EqualsIgnoringPrimaryResultSet>> | undefined;
  #unordered: Equality<Iterable<EqualsIgnoringPrimaryResultSet>> | undefined;

  private constructor(
    hashes: ReadOnlyMap<TableValuedResultSet, number>,
    identities: ReadOnlyMap<TableValuedResultSet, unknown>
  ) {
    this.#hashes = hashes;
    this.#identities = identities;
  }

  get listEquality() {
    return (this.#ordered ??= listEquality(this));
  }

  get unorderedEquality() {
    return (this.#unordered ??= unorderedEquality(this));
  }

  #lookupIdentity(resultSet: TableValuedResultSet) {
    const symbol = this.#identities.get(resultSet);
    if (symbol == null) {
      throw new Error('Unknown table-valued result set for equals');
    }

    return symbol;
  }

  equals(a: EqualsIgnoringPrimaryResultSet, b: EqualsIgnoringPrimaryResultSet): boolean {
    return a.equalsAssumingSamePrimaryResultSet(b, this);
  }

  resultSetEquals(a: SourceResultSet, b: SourceResultSet): boolean {
    // We don't need to compare primary result sets, those are assumed to be equal.
    if (a instanceof PhysicalSourceResultSet) {
      return b instanceof PhysicalSourceResultSet;
    } else if (b instanceof PhysicalSourceResultSet) {
      return a instanceof PhysicalSourceResultSet;
    }

    return this.#lookupIdentity(a) === this.#lookupIdentity(b);
  }

  hashResultSet(hasher: StableHasher, resultSet: SourceResultSet): void {
    // There's only one primary result set which we ignore by design.
    if (resultSet instanceof PhysicalSourceResultSet) return;

    const hash = this.#hashes.get(resultSet);
    if (hash == null) {
      throw new Error('Hashing unknown table-valued result set');
    }

    hasher.addHash(hash);
  }

  hash(hasher: StableHasher, value: EqualsIgnoringPrimaryResultSet): void {
    return value.assumingSamePrimaryResultSetEqualityHashCode(this, hasher);
  }

  /**
   * An equality operator for table-valued functions, suitable only for generating hash codes within a single structure
   * owning table-valued functions.
   *
   * This can't be used to compare functions across streams or to test for equality.
   */
  static forHashCode(innerHashes: ReadOnlyMap<TableValuedResultSet, number>) {
    return this.forSingleOwner(innerHashes, {
      // Compare by identity within the single owner.
      get: (tableValued) => tableValued
    });
  }

  static forSingleTableValuedFunction(resultSet: TableValuedResultSet) {
    const hashCodeMap = new Map<TableValuedResultSet, number>();
    // Hash codes are only used to ensure column references to different table-valued functions generate different
    // codes. Since there's just one, it can be an arbitrary value.
    hashCodeMap.set(resultSet, 0);

    return this.forHashCode(hashCodeMap);
  }

  static forSingleOwner(
    innerHashes: ReadOnlyMap<TableValuedResultSet, number>,
    equality: ReadOnlyMap<TableValuedResultSet, unknown>
  ) {
    return new TableValuedFunctionEquality(innerHashes, equality);
  }

  static mergeForEquality(
    localHashes: TableValuedFunctionEquality,
    otherHashes: TableValuedFunctionEquality,
    equality: ReadOnlyMap<TableValuedResultSet, symbol>
  ) {
    return new TableValuedFunctionEquality(
      {
        get(key) {
          return localHashes.#hashes.get(key) ?? otherHashes.#hashes.get(key);
        }
      },
      equality
    );
  }

  static readonly empty = new TableValuedFunctionEquality(new Map(), new Map());
}
