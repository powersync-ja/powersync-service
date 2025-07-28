import _ from 'lodash';
import { isParameterValueClause, isRowValueClause, SQLITE_TRUE, sqliteBool } from '../sql_support.js';
import { TablePattern } from '../TablePattern.js';
import { ParameterMatchClause, ParameterValueClause, RowValueClause, SqliteJsonValue } from '../types.js';
import { isJsonValue, normalizeParameterValue } from '../utils.js';
import { SqlTools } from '../sql_filters.js';
import { OPERATOR_NOT } from '../sql_functions.js';
import { ParameterLookup } from '../BucketParameterQuerier.js';

import { StreamVariant } from './variant.js';
import { SubqueryEvaluator } from './parameter.js';

export abstract class FilterOperator {
  abstract compile(context: BucketCompilationContext): void;

  compileVariants(streamName: string): StreamVariant[] {
    const initialVariant = new StreamVariant(0);
    const allVariants: StreamVariant[] = [initialVariant];

    this.compile({ streamName, currentVariant: initialVariant, allVariants, queryCounter: 0 });
    return allVariants;
  }
}

interface BucketCompilationContext {
  streamName: string;
  allVariants: StreamVariant[];
  currentVariant: StreamVariant;
  queryCounter: number;
}

/**
 * A `NOT` operator that inverts an inner operator.
 *
 * This is only used temporarily when compiling the filter: It is illegal to negate {@link InOperator} and
 * {@link CompareRowValueWithStreamParameter} clauses, as those are implemented with special lookups based on bucket
 * parameters.
 * For {@link EvaluateSimpleCondition}, a wrapping {@link Not} clause is unecessary, because we can simply push the
 * negation into the inner evaluator.
 */
export class Not extends FilterOperator {
  private operand: FilterOperator;

  constructor(operand: FilterOperator) {
    super();
    this.operand = operand;
  }

  compile(): void {
    throw Error('Not operator should have been desugared before compilation.');
  }
}

export class And extends FilterOperator {
  private left: FilterOperator;
  private right: FilterOperator;

  constructor(left: FilterOperator, right: FilterOperator) {
    super();
    this.left = left;
    this.right = right;
  }

  compile(context: BucketCompilationContext) {
    // Within a variant, added conditions and parameters form a conjunction.
    this.left.compile(context);
    this.right.compile(context);
  }
}

export class Or extends FilterOperator {
  private left: FilterOperator;
  private right: FilterOperator;

  constructor(left: FilterOperator, right: FilterOperator) {
    super();
    this.left = left;
    this.right = right;
  }

  compile(context: BucketCompilationContext): void {
    throw 'todo: compile or operators';
  }
}

export class Subquery {
  private table: TablePattern;
  readonly column: RowValueClause;
  private filter: FilterOperator;

  constructor(
    table: TablePattern,
    column: RowValueClause,
    filter: CompareRowValueWithStreamParameter | EvaluateSimpleCondition
  ) {
    this.table = table;
    this.column = column;
    this.filter = filter;
  }

  addFilter(filter: CompareRowValueWithStreamParameter | EvaluateSimpleCondition) {
    this.filter = new And(this.filter, filter);
  }

  compileEvaluator(context: BucketCompilationContext): SubqueryEvaluator {
    const innerVariants = this.filter.compileVariants(context.streamName).map((variant) => {
      const id = context.queryCounter.toString();
      context.queryCounter++;

      return [variant, id] satisfies [StreamVariant, string];
    });

    const column = this.column;

    const evaluator: SubqueryEvaluator = {
      parameterTable: this.table,
      lookupsForParameterRow(sourceTable, row) {
        const value = column.evaluate({ [sourceTable.table]: row });
        if (!isJsonValue(value)) {
          return null;
        }

        const lookups: ParameterLookup[] = [];
        for (const [variant, id] of innerVariants) {
          for (const instantiation of variant.instantiationsForRow({ sourceTable, record: row })) {
            lookups.push(ParameterLookup.normalized(context.streamName, id, instantiation));
          }
        }
        return { value, lookups };
      },
      lookupsForRequest(parameters) {
        const lookups: ParameterLookup[] = [];

        for (const [variant, id] of innerVariants) {
          const instantiation = variant.findStaticInstantiation(parameters);
          if (instantiation == null) {
            continue;
          }

          lookups.push(ParameterLookup.normalized(context.streamName, id, instantiation));
        }

        return lookups;
      }
    };

    context.currentVariant.subqueries.push(evaluator);
    return evaluator;
  }
}

export type ScalarExpression = RowValueClause | ParameterValueClause;

/**
 * A filter that matches when _something_ is contained in a subquery.
 *
 * Examples:
 *
 *  - `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
 *
 *  An `IN` operator can also be handled without requiring a subquery, those don't create {@link InOperator}s:
 *
 *  - `SELECT * FROM comments WHERE id IN request.params('id')` ({@link CompareRowValueWithStreamParameter})
 *  - `SELECT * FROM comments WHERE request.user() IN comments.tagged_users` ({@link CompareRowValueWithStreamParameter})
 */
export class InOperator extends FilterOperator {
  private left: RowValueClause;
  private right: Subquery;

  constructor(left: RowValueClause, right: Subquery) {
    super();
    this.left = left;
    this.right = right;
  }

  compile(context: BucketCompilationContext): void {
    const subqueryEvaluator = this.right.compileEvaluator(context);

    const filter = this.left;
    // Something like `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
    // This groups rows into buckets identified by comments.issue_id, which happens in filterRow.
    // When a user connects, we need to resolve all the issue ids they own. This happens with an indirection:
    //  1. In the subquery evaluator, we create an index from owner_id to issue ids.
    //  2. When we have users, we use that index to find issue ids dynamically, with which we can build the buckets
    //     to sync.
    context.currentVariant.parameters.push({
      lookup: {
        type: 'in',
        subquery: subqueryEvaluator
      },
      filterRow(options) {
        const tables = { [options.sourceTable.table]: options.record };
        const value = filter.evaluate(tables);
        if (isJsonValue(value)) {
          return [normalizeParameterValue(value)];
        } else {
          return [];
        }
      }
    });

    if (isRowValueClause(this.left)) {
    } else if (isParameterValueClause(this.left)) {
      // Something like `SELECT * FROM comments WHERE request.user_id() IN (SELECT id FROM users WHERE is_admin)`.
      // This doesn't introduce a bucket parameter, but we still need to create a parameter lookup to determine whether
      // a given user should have access to the bucket or not. Because lookups can only be exact (we can't evaluate
      // `SELECT id FROM users WHERE is_admin` into a single ParameterLookup), we need to push the filter down into
      // the subquery, e.g. `WHERE EXISTS(SELECT id FROM users WHERE is_admin AND id = request.user_id())`.
      const left = this.left;
      const subqueryEvaluator = this.right.compileEvaluator(context);

      context.currentVariant.requestFilters.push({
        type: 'dynamic',
        subquery: subqueryEvaluator,
        matches(_, results) {
          return results.length > 0;
        }
      });
    } else {
      const _: never = this.left; // Exhaustive check
    }
  }
}

/**
 * A filter that matches when _something_ exists in a subquery.
 *
 * This is exclusively used to compile `IN` operators where the left operand does not depend on row values, e.g.
 *
 *  - `SELECT * FROM comments WHERE request.user_id() IN (SELECT id FROM users WHERE is_admin)`
 *
 * These queries are desugared to something that is semantically equivalent to
 * `WHERE EXISTS (SELECT _ FROM users WHERE is_admin AND id = request.user_id())`.
 */
export class ExistsOperator extends FilterOperator {
  private subquery: Subquery;

  constructor(subquery: Subquery) {
    super();
    this.subquery = subquery;
  }

  compile(context: BucketCompilationContext): void {
    const subqueryEvaluator = this.subquery.compileEvaluator(context);

    context.currentVariant.requestFilters.push({
      type: 'dynamic',
      subquery: subqueryEvaluator,
      matches(_, results) {
        return results.length > 0;
      }
    });
  }
}

/**
 * A filter that matches if a column of the input table matches some condition that depends on the request parameters.
 *
 * E.g. `SELECT * FROM issue_id WHERE owner_id = request.user_id()`. This will add the referenced column as a bucket
 * parameter, but doesn't require a dynamic lookup.
 *
 * We only allow a few operators that are efficient to compute here (effectively just an equality operator).
 */
export class CompareRowValueWithStreamParameter extends FilterOperator {
  private match: ParameterMatchClause;

  constructor(match: ParameterMatchClause) {
    super();
    this.match = match;
  }

  compile(context: BucketCompilationContext): void {
    const match = this.match;

    for (const filters of match.inputParameters) {
      context.currentVariant.parameters.push({
        lookup: {
          type: 'static',
          fromRequest(parameters) {
            return filters.parametersToLookupValue(parameters);
          }
        },
        filterRow(options) {
          const tables = { [options.sourceTable.table]: options.record };
          const matchingValues: SqliteJsonValue[] = [];

          for (const matchingParameters of match.filterRow(tables)) {
            const matchingValue = matchingParameters[filters.key];
            if (matchingValue != null) {
              matchingValues.push(normalizeParameterValue(matchingValue));
            }
          }

          return matchingValues;
        }
      });
    }
  }
}

/**
 * A simple condition that is either static, only depends on the current row being matched, or only depends on the input
 * parameters.
 */
export class EvaluateSimpleCondition extends FilterOperator {
  expression: ScalarExpression;

  constructor(expression: ScalarExpression) {
    super();
    this.expression = expression;
  }

  compile(context: BucketCompilationContext): void {
    if (isRowValueClause(this.expression)) {
      const filter = this.expression;

      context.currentVariant.additionalRowFilters.push((row) => {
        return sqliteBool(filter.evaluate({ [row.sourceTable.table]: row.record })) == SQLITE_TRUE;
      });
    } else if (isParameterValueClause(this.expression)) {
      const filter = this.expression;

      context.currentVariant.requestFilters.push({
        type: 'static',
        matches(params) {
          return sqliteBool(filter.lookupParameterValue(params)) == SQLITE_TRUE;
        }
      });
    } else {
      const _: never = this.expression; // Exhaustive check
    }
  }

  negate(tools: SqlTools): EvaluateSimpleCondition {
    return new EvaluateSimpleCondition(tools.composeFunction(OPERATOR_NOT, [this.expression], []) as ScalarExpression);
  }
}
