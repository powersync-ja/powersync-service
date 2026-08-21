import { Expr, NodeLocation, PGNode } from 'pgsql-ast-parser';
import { getLocation } from '../errors.js';
import { ExternalData, SqlExpression } from '../sync_plan/expression.js';
import { ExpressionToSqlite } from '../sync_plan/expression_to_sql.js';
import { RecursiveExpressionVisitor } from '../sync_plan/expression_visitor.js';
import { ConnectionParameterSource } from '../sync_plan/plan.js';
import { EqualsIgnoringResultSet, equalsIgnoringResultSetList } from './compatibility.js';
import { ParsingErrorListener } from './compiler.js';
import { StableHasher } from './equality.js';
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

/**
 * A stable identity for expression behavior across syntactic operand ordering changes.
 *
 * Boolean conjunction/disjunction and equality are commutative. Comparisons are normalized to one direction, so
 * `a > b` and `b < a` also match. Operators and constructs where order affects behavior retain their original order.
 */
export function expressionBehaviorIdentity(expression: SqlExpression<ExpressionInput>): string {
  switch (expression.type) {
    case 'data':
      return JSON.stringify(['data', expressionInputIdentity(expression.source)]);
    case 'unary':
      return JSON.stringify(['unary', expression.operator, expressionBehaviorIdentity(expression.operand)]);
    case 'binary': {
      if (expression.operator == 'and' || expression.operator == 'or') {
        const operands: SqlExpression<ExpressionInput>[] = [];
        collectAssociativeOperands(expression, expression.operator, operands);
        return JSON.stringify(['binary', expression.operator, operands.map(expressionBehaviorIdentity).sort()]);
      }

      let operator = expression.operator;
      let left = expression.left;
      let right = expression.right;
      if (operator == '>' || operator == '>=') {
        operator = operator == '>' ? '<' : '<=';
        [left, right] = [right, left];
      }

      const operands = [expressionBehaviorIdentity(left), expressionBehaviorIdentity(right)];
      if (operator == '=' || operator == 'is') {
        operands.sort();
      }
      return JSON.stringify(['binary', operator, operands]);
    }
    case 'between':
      return JSON.stringify([
        'between',
        expressionBehaviorIdentity(expression.value),
        expressionBehaviorIdentity(expression.low),
        expressionBehaviorIdentity(expression.high)
      ]);
    case 'scalar_in':
      return JSON.stringify([
        'scalar_in',
        expressionBehaviorIdentity(expression.target),
        expression.in.map(expressionBehaviorIdentity)
      ]);
    case 'case_when':
      return JSON.stringify([
        'case_when',
        expression.operand == null ? null : expressionBehaviorIdentity(expression.operand),
        expression.whens.map((branch) => [
          expressionBehaviorIdentity(branch.when),
          expressionBehaviorIdentity(branch.then)
        ]),
        expression.else == null ? null : expressionBehaviorIdentity(expression.else)
      ]);
    case 'cast':
      return JSON.stringify(['cast', expression.cast_as, expressionBehaviorIdentity(expression.operand)]);
    case 'function':
      return JSON.stringify(['function', expression.function, expression.parameters.map(expressionBehaviorIdentity)]);
    case 'lit_null':
      return JSON.stringify(['lit_null']);
    case 'lit_double':
      return JSON.stringify(['lit_double', expression.value]);
    case 'lit_int':
      return JSON.stringify(['lit_int', expression.base10]);
    case 'lit_string':
      return JSON.stringify(['lit_string', expression.value]);
  }
}

function collectAssociativeOperands(
  expression: SqlExpression<ExpressionInput>,
  operator: 'and' | 'or',
  output: SqlExpression<ExpressionInput>[]
): void {
  if (expression.type == 'binary' && expression.operator == operator) {
    collectAssociativeOperands(expression.left, operator, output);
    collectAssociativeOperands(expression.right, operator, output);
  } else {
    output.push(expression);
  }
}

function expressionInputIdentity(input: ExpressionInput): readonly string[] {
  if (input instanceof ColumnInRow) {
    return ['column', input.column];
  } else if (input instanceof RowMetadata) {
    return ['row_metadata', input.kind];
  } else {
    return ['connection_parameter', input.source];
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
export abstract class RowReference implements EqualsIgnoringResultSet {
  constructor(
    readonly syntacticOrigin: Expr,
    readonly resultSet: SourceResultSet
  ) {}

  abstract equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean;
  abstract assumingSameResultSetEqualityHashCode(hasher: StableHasher): void;
}

export class ColumnInRow extends RowReference {
  constructor(
    syntacticOrigin: Expr,
    resultSet: SourceResultSet,
    readonly column: string
  ) {
    super(syntacticOrigin, resultSet);
  }

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof ColumnInRow && other.column == this.column;
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(this.column);
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

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof RowMetadata && other.kind == this.kind;
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    hasher.addString(`table.${this.kind}`);
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
