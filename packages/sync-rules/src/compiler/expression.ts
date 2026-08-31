import { Expr, NodeLocation, PGNode } from 'pgsql-ast-parser';
import { getLocation } from '../errors.js';
import { ExternalData, SqlExpression } from '../sync_plan/expression.js';
import { ExpressionToSqlite } from '../sync_plan/expression_to_sql.js';
import { RecursiveExpressionVisitor } from '../sync_plan/expression_visitor.js';
import { ConnectionParameterSource } from '../sync_plan/plan.js';
import { EqualsIgnoringPrimaryResultSet, TableValuedHashCodes, TableValuedIdentities } from './compatibility.js';
import { ParsingErrorListener } from './compiler.js';
import { Equatable, StableHasher } from './equality.js';
import { SourceResultSet } from './table.js';

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
export class SyncExpression implements EqualsIgnoringPrimaryResultSet {
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

  get location(): SourceLocation {
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

  equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    identities: TableValuedIdentities
  ): boolean {
    return (
      other instanceof SyncExpression &&
      other.sql == this.sql &&
      identities.orderedEquals(other.instantiation, this.instantiation)
    );
  }

  assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void {
    hasher.addString(this.sql);
    codes.hashOrdered(this.instantiation, hasher);
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

export type ExpressionInput = ColumnInRow | RowMetadata | ConnectionParameter;

/**
 * An expression input resolved against a row of a result set: either a column value or metadata about the row's
 * source table.
 */
export abstract class RowReference implements EqualsIgnoringPrimaryResultSet {
  constructor(
    readonly syntacticOrigin: Expr,
    readonly resultSet: SourceResultSet
  ) {}

  abstract equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    identities: TableValuedIdentities
  ): boolean;
  abstract assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void;
}

export class ColumnInRow extends RowReference {
  constructor(
    syntacticOrigin: Expr,
    resultSet: SourceResultSet,
    readonly column: string
  ) {
    super(syntacticOrigin, resultSet);
  }

  override equalsAssumingSamePrimaryResultSet(
    other: EqualsIgnoringPrimaryResultSet,
    identities: TableValuedIdentities
  ): boolean {
    return (
      other instanceof ColumnInRow &&
      other.column == this.column &&
      identities.identityOf(other.resultSet) === identities.identityOf(this.resultSet)
    );
  }

  override assumingSamePrimaryResultSetEqualityHashCode(codes: TableValuedHashCodes, hasher: StableHasher): void {
    hasher.addString(this.column);
    codes.hashTableValued(this.resultSet, hasher);
  }
}

export type RowMetadataKind = 'schema' | 'table_name' | 'table_suffix';

/**
 * A reference to metadata of the row's source table (`users.schema()`, `users.table_name()` or
 * `users.table_suffix()`) instead of an actual column, resolved against the concrete table a row was
 * replicated from.
 */
export class RowMetadata extends RowReference {
  constructor(
    syntacticOrigin: Expr,
    resultSet: SourceResultSet,
    readonly kind: RowMetadataKind
  ) {
    super(syntacticOrigin, resultSet);
  }

  override equalsAssumingSamePrimaryResultSet(other: EqualsIgnoringPrimaryResultSet): boolean {
    // Row metadata is always on the primary result set, so no need to hash the result set here.
    return other instanceof RowMetadata && other.kind == this.kind;
  }

  override assumingSamePrimaryResultSetEqualityHashCode(_codes: TableValuedHashCodes, hasher: StableHasher): void {
    hasher.addString(`table.${this.kind}`);
  }
}

export class ConnectionParameter implements EqualsIgnoringPrimaryResultSet, Equatable {
  constructor(
    readonly syntacticOrigin: Expr,
    readonly source: ConnectionParameterSource
  ) {}

  equals(other: unknown): boolean {
    return other instanceof ConnectionParameter && other.source == this.source;
  }

  buildHash(hasher: StableHasher): void {
    hasher.addString(this.source);
  }

  equalsAssumingSamePrimaryResultSet(other: EqualsIgnoringPrimaryResultSet): boolean {
    return this.equals(other);
  }

  assumingSamePrimaryResultSetEqualityHashCode(_codes: TableValuedHashCodes, hasher: StableHasher): void {
    return this.buildHash(hasher);
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
  readonly sourceForNode = new Map<SqlExpression<unknown>, SourceLocation>();

  locationFor(source: SqlExpression<unknown>): SourceLocation {
    const resolved = this.sourceForNode.get(source);
    const location = getLocation(resolved?.location);
    if (location == null) {
      throw new Error('Missing location');
    }

    return { location, errors: resolved!.errors };
  }
}

export interface SourceLocation {
  location: PGNode | NodeLocation;
  /**
   * An error reporter that can understand the given {@link location}.
   *
   * Because sync streams might be composed of multiple source statements (like common table expressions) that can
   * ultimately only be fully analyzed together, this is necessary to ensure we can report errors on the correct source
   * everywhere.
   */
  errors: ParsingErrorListener;
}
