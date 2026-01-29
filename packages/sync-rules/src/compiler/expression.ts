import { Expr, NodeLocation, PGNode } from 'pgsql-ast-parser';
import { SourceResultSet } from './table.js';
import { EqualsIgnoringResultSet, equalsIgnoringResultSetList } from './compatibility.js';
import { StableHasher } from './equality.js';
import { ConnectionParameterSource } from '../sync_plan/plan.js';
import { ExternalData, SqlExpression } from '../sync_plan/expression.js';
import { ExpressionToSqlite } from '../sync_plan/expression_to_sql.js';
import { RecursiveExpressionVisitor } from '../sync_plan/expression_visitor.js';
import { getLocation } from '../errors.js';

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
  #sql?: string;
  #instantiation?: readonly ExpressionInput[];

  /**
   * The original expression, where references to row or connection parameters have been replaced with SQL variables
   * that are tracked through {@link instantiation}.
   *
   * This is only used to compute hash codes and to check instances for equality. {@link node} is the canonical
   * representation of this expression.
   */
  get sql(): string {
    return (this.#sql ??= ExpressionToSqlite.toSqlite(this.node));
  }

  /**
   * The values to instantiate parameters in {@link sqlExpression} with to retain original semantics of the
   * expression.
   */
  get instantiation(): readonly ExpressionInput[] {
    if (this.#instantiation != null) {
      return this.#instantiation;
    }

    const instantiation: ExpressionInput[] = [];
    FindExternalData.instance.visit(this.node, instantiation);
    return (this.#instantiation = instantiation);
  }

  get location(): NodeLocation {
    return this.locations.locationFor(this.node);
  }

  constructor(
    /**
     * The AST node backing {@link sql}.
     *
     * We use this to be able to compose expressions, e.g. to possibly merge them.
     */
    readonly node: SqlExpression<ExpressionInput>,
    readonly locations: NodeLocations
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

class FindExternalData extends RecursiveExpressionVisitor<ExpressionInput, void, ExpressionInput[]> {
  defaultExpression(expr: SqlExpression<ExpressionInput>, arg: ExpressionInput[]): void {
    this.visitChildren(expr, arg);
  }

  visitExternalData(expr: ExternalData<ExpressionInput>, arg: ExpressionInput[]): void {
    arg.push(expr.source);
  }

  static readonly instance: FindExternalData = new FindExternalData();
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

/**
 * Tracks the original source location for translated {@link SqlExpression} nodes.
 *
 * We want to serialize translated expressions for sync plan, so embedding source offsets in them expands the size of
 * sync plans and is tedious. We only need access to node locations while compiling sync streams, which we store in this
 * in-memory map.
 */
export class NodeLocations {
  readonly sourceForNode = new Map<SqlExpression<ExpressionInput>, PGNode | NodeLocation>();

  locationFor(source: SqlExpression<ExpressionInput>): NodeLocation {
    const location = getLocation(this.sourceForNode.get(source));
    if (location == null) {
      throw new Error('Missing location');
    }

    return location;
  }
}
