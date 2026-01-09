import { assignChanged, astMapper, BinaryOperator, Expr, NodeLocation } from 'pgsql-ast-parser';
import { ColumnInRow, ExpressionInput, SyncExpression } from './expression.js';
import { SourceResultSet } from './table.js';
import { expandNodeLocations } from '../errors.js';
import { EqualsIgnoringResultSet } from './compatibility.js';
import { StableHasher } from './equality.js';

export interface Or {
  terms: And[];
}

export interface And {
  terms: BaseTerm[];
}

/**
 * A boolean expression that doesn't have boolean operators with subexpressions referencing different tables in it.
 *
 * These basic terms form a filter by being composed into a distributive normal form. For analysis, we just need to know
 * how terms from different tables relate to each other. So `users.id = auth.user_id() AND issues.is_public` need to be
 * two terms in an {@link And}, but `users.is_deleted OR users.is_admin` can be represented as a single base term.
 */
export type BaseTerm = SingleDependencyExpression | EqualsClause;

export function isBaseTerm(value: unknown): value is BaseTerm {
  return value instanceof SingleDependencyExpression || value instanceof EqualsClause;
}

/**
 * A {@link SyncExpression} that only depends on a single result set or connection data.
 */
export class SingleDependencyExpression implements EqualsIgnoringResultSet {
  readonly expression: SyncExpression;
  /**
   * The single result set on which the expression depends on.
   *
   * If null, the expression is allowed to depend on subscription parameters instead.
   */
  readonly resultSet: SourceResultSet | null;

  /**
   * Whether this expression depends on data derived from the connection or subscription.
   */
  readonly dependsOnConnection: boolean;

  constructor(expression: SyncExpression | SingleDependencyExpression) {
    if (expression instanceof SyncExpression) {
      const checked = SingleDependencyExpression.extractSingleDependency(expression.instantiation);
      if (checked == null) {
        throw new InvalidExpressionError('Expression with multiple dependencies passed to SingleDependencyExpression');
      }

      this.expression = expression;
      this.resultSet = checked[0];
      this.dependsOnConnection = checked[1];
    } else {
      this.expression = expression.expression;
      this.resultSet = expression.resultSet;
      this.dependsOnConnection = expression.dependsOnConnection;
    }
  }

  equalsAssumingSameResultSet(other: EqualsIgnoringResultSet): boolean {
    return other instanceof SingleDependencyExpression && other.expression.equalsAssumingSameResultSet(this.expression);
  }

  assumingSameResultSetEqualityHashCode(hasher: StableHasher): void {
    this.expression.assumingSameResultSetEqualityHashCode(hasher);
  }

  /**
   * If all inputs have a single dependency, returns either the result set they all depend on or `null` and whether they
   * depend on connection data.
   *
   * If the inputs have more than a single distinct dependency, returns null.
   */
  static extractSingleDependency(inputs: Iterable<ExpressionInput>): [SourceResultSet | null, boolean] | null {
    let resultSet: SourceResultSet | null = null;
    let hasSubscriptionDependency = false;

    for (const dependency of inputs) {
      if (dependency instanceof ColumnInRow) {
        if ((resultSet != null && resultSet !== dependency.resultSet) || hasSubscriptionDependency) {
          return null;
        }
        resultSet = dependency.resultSet;
      } else {
        if (resultSet != null) {
          return null;
        }

        hasSubscriptionDependency = true;
      }
    }

    return [resultSet, hasSubscriptionDependency];
  }
}

/**
 * A {@link SyncExpression} that only depends on data from a single row.
 */
export class RowExpression extends SingleDependencyExpression {
  declare readonly resultSet: SourceResultSet;

  constructor(expression: SyncExpression | SingleDependencyExpression) {
    super(expression);
    if (this.resultSet == null) {
      throw new InvalidExpressionError('Does not depend on a single result set');
    }
  }
}

/**
 * A {@link SyncExpression} that only depends on connection data.
 */
export class RequestExpression extends SingleDependencyExpression {
  constructor(expression: SyncExpression | SingleDependencyExpression) {
    super(expression);
    if (this.resultSet != null) {
      throw new InvalidExpressionError('Does not depend on connection data');
    }
  }
}

/**
 * An expression of the form `foo = bar`, where `foo` and `bar` are expressions allowed to depend on different sources.
 */
export class EqualsClause {
  constructor(
    readonly left: SingleDependencyExpression,
    readonly right: SingleDependencyExpression
  ) {}

  get location(): NodeLocation | undefined {
    return expandNodeLocations([this.left.expression.node, this.right.expression.node]);
  }
}

export class InvalidExpressionError extends Error {
  constructor(message: string) {
    super(message);
  }
}
