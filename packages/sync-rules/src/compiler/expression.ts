import { Expr } from 'pgsql-ast-parser';
import { SourceResultSet } from './table.js';
import { EqualsIgnoringResultSet, equalsIgnoringResultSetList } from './compatibility.js';
import { StableHasher } from './equality.js';

/**
 * An analyzed SQL expression tracking dependencies on non-static data (i.e. rows or connection sources).
 *
 * Consider the sync stream `SELECT * FROM issues WHERE is_public OR auth.param('is_admin')`. To be able to explicitly
 * track dependencies referenced in expressions, we transform them into a {@link SyncExpression}. For the `WHERE` clause
 * in that example, the {@link sqlExpression} would be `?1 OR (?2 ->> 'is_admin')`, where `?1` is a {@link ColumnInRow}
 * and `?2` is a {@link ConnectionParameter}.
 *
 * Once in this form, it's easy to reason about dependencies in expressions (used to later generate parameter match
 * clauses) and to evaluate expressions at runtime (by preparing them as a statement and binding external values).
 */
export class SyncExpression implements EqualsIgnoringResultSet {
  constructor(
    /**
     * The original expression, where references to row or connection parameters have been replaced with SQL variables
     * that are tracked through {@link instantiation}.
     */
    readonly sql: string,
    /**
     * The AST node backing {@link expression}.
     *
     * We use this to be able to compose expressions, e.g. to possibly merge them.
     */
    readonly node: Expr,
    /**
     * The values to instantiate parameters in {@link sqlExpression} with to retain original semantics of the
     * expression.
     */
    readonly instantiation: ExpressionInput[]
  ) {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return (
      other instanceof SyncExpression &&
      other.sql == this.sql &&
      equalsIgnoringResultSetList.equals(other.instantiation, this.instantiation)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(this.sql);
    equalsIgnoringResultSetList.hash(hasher, this.instantiation);
  }
}

export type ExpressionInput = ColumnInRow | ConnectionParameter;

export class ColumnInRow implements EqualsIgnoringResultSet {
  constructor(
    readonly syntacticOrigin: Expr,
    readonly resultSet: SourceResultSet,
    readonly column: string
  ) {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof ColumnInRow && other.column == this.column;
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(this.column);
  }
}

export type ConnectionParameterSource = 'auth' | 'subscription' | 'connection';

export class ConnectionParameter implements EqualsIgnoringResultSet {
  constructor(
    readonly syntacticOrigin: Expr,
    readonly source: ConnectionParameterSource
  ) {}

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof ConnectionParameter && other.source == this.source;
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(this.source);
  }
}
