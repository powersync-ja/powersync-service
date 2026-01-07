import { assignChanged, astMapper, Expr, NodeLocation, toSql } from 'pgsql-ast-parser';
import { SourceResultSet } from './table.js';
import { EqualsIgnoringResultSet, equalsIgnoringResultSetList } from './compatibility.js';
import { StableHasher } from './equality.js';
import { expandNodeLocations } from '../errors.js';

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
  private cachedFormattedSqlExpression?: string;
  readonly originalLocation: NodeLocation | undefined;

  constructor(
    /**
     * The original expression, where references to row or connection parameters have been replcated with SQL variables
     * that are tracked through {@link instantiation}.
     */
    readonly sqlExpression: Expr,
    /**
     * The values to instantiate parameters in {@link sqlExpression} with to retain original semantics of the
     * expression.
     */
    readonly instantiation: ExpressionInput[]
  ) {
    this.originalLocation = sqlExpression._location;
  }

  get formattedSql(): string {
    return (this.cachedFormattedSqlExpression ??= toSql.expr(this.sqlExpression));
  }

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return (
      other instanceof SyncExpression &&
      other.formattedSql == this.formattedSql &&
      equalsIgnoringResultSetList.equals(other.instantiation, this.instantiation)
    );
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(this.formattedSql);
    equalsIgnoringResultSetList.hash(hasher, this.instantiation);
  }

  /**
   * Merges two sync expressions, automatically patching variable references to ensure they match the combined
   * expression.
   */
  static compose(a: SyncExpression, b: SyncExpression, combine: (a: Expr, b: Expr) => Expr): SyncExpression {
    const bExpr = astMapper((m) => ({
      parameter: (st) => {
        // All parameters are named ?<idx>, rename those in b to avoid collisions with a
        const originalIndex = Number(st.name.substring(1));
        const newIndex = a.instantiation.length + originalIndex;

        return assignChanged(st, { name: `?${newIndex}` });
      }
    })).expr(b.sqlExpression)!;

    const combined = combine(a.sqlExpression, bExpr);
    combined._location = expandNodeLocations([a.sqlExpression, bExpr])!;

    return new SyncExpression(combined, [...a.instantiation, ...b.instantiation]);
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
