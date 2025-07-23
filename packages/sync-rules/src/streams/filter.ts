import _ from 'lodash';
import { SQLITE_TRUE, sqliteBool } from '../sql_support.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluateRowOptions,
  EvaluationError,
  ParameterMatchClause,
  ParameterValueClause,
  RequestParameters,
  RowValueClause,
  SqliteJsonValue
} from '../types.js';
import { JSONBucketNameSerialize } from '../utils.js';

abstract class FilterOperator {
  compile(context: BucketCompilationContext) {
    throw 'TODO';
  }
}

interface BucketParameter {
  filterRow(options: EvaluateRowOptions): SqliteJsonValue[];
}

/**
 * A variant of a stream.
 *
 * Variants are introduced on {@link Or} filters, since different sub-filters (with potentially different) bucket
 * parameters can both cause a row to be matched.
 *
 * Consider the query `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user()) OR request.is_admin()`.
 * Here, the filter is an or clause matching rows where:
 *
 *   - An {@link InOperator} associatates comments in issues owned by the requesting user. This gets implemented with a
 *     parameter lookup index mapping `issue.owner_id => issue.id`. `comments.issue_id` is a bucket parameter resolved
 *     dynamically.
 *   - Or, the user is an admin, in which case all comments are matched. There are no bucket parameters for this
 *     variant.
 *
 * The introduction of stream variants allows the `evaluateParameterRow` and `queriersForSubscription` implementations
 * to operate independently.
 */
export class StreamVariant {
  id: number;
  parameters: BucketParameter[];

  /**
   * Additional filters that don't affect bucket parameters, but can exclude rows.
   */
  additionalRowFilters: ((options: EvaluateRowOptions) => boolean)[];

  /**
   * Additional filters that only affect the queriers without introducing or relying on bucket parameters.
   */
  additionalParameterFilters: ((options: RequestParameters) => boolean)[];

  constructor(id: number) {
    this.id = id;
    this.parameters = [];
    this.additionalRowFilters = [];
    this.additionalParameterFilters = [];
  }

  /**
   * Given a row in the table this stream selects from, returns all ids of buckets to which that row belongs to.
   */
  bucketIdsForRow(streamName: string, options: EvaluateRowOptions): string[] {
    for (const additional of this.additionalRowFilters) {
      if (!additional(options)) {
        return [];
      }
    }

    const parameterInstantiations: SqliteJsonValue[][] = [];
    for (const parameter of this.parameters) {
      const matching = parameter.filterRow(options);
      if (matching.length == 0) {
        // The final list of bucket ids is the cartesian product of all matching parameters. So if there's no parameter
        // satisfying this value, we know the final list will be empty.
        return [];
      }

      parameterInstantiations.push(matching);
    }

    // Combine the map of values like {param_1: [foo, bar], param_2: [baz]} into parameter arrays:
    // [foo, baz], [bar, baz].
    const bucketIds: string[] = [];
    const totalLength = _.reduce(parameterInstantiations, (a, b) => a * b.length, 1);
    for (let i = 0; i < totalLength; i++) {
      const instantiation: SqliteJsonValue[] = [];
      let indexInSet = i;

      for (const parameterSet in parameterInstantiations) {
        instantiation.push(parameterSet[Math.floor(indexInSet % parameterSet.length)]);
        indexInSet = Math.floor(indexInSet / parameterSet.length);
      }

      bucketIds.push(this.buildBucketId(streamName, instantiation));
    }

    return [];
  }

  private buildBucketId(streamName: string, instantiation: SqliteJsonValue[]) {
    if (instantiation.length != this.parameters.length) {
      throw Error('Internal error, instantiation length mismatch');
    }

    return `${streamName}|${this.id}${JSONBucketNameSerialize.stringify(instantiation)}`;
  }
}

interface BucketCompilationContext {
  allVariants: StreamVariant[];
  currentVariant: StreamVariant;
}

class And extends FilterOperator {
  private left: FilterOperator;
  private right: FilterOperator;

  constructor(left: FilterOperator, right: FilterOperator) {
    super();
    this.left = left;
    this.right = right;
  }
}

class Or extends FilterOperator {
  private left: FilterOperator;
  private right: FilterOperator;

  constructor(left: FilterOperator, right: FilterOperator) {
    super();
    this.left = left;
    this.right = right;
  }
}

class Subquery {
  private table: TablePattern;
  private column: string;
  private filter: ParameterMatchClause;

  constructor(table: TablePattern, column: string, filter: ParameterMatchClause) {
    this.table = table;
    this.column = column;
    this.filter = filter;
  }
}

type ScalarExpression = { row: RowValueClause } | { parameter: ParameterValueClause };

/**
 * A filter that matches when _something_ is contained in a subquery or a JSON array.
 *
 * Examples:
 *
 *  - `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
 *  - `SELECT * FROM secrets WHERE request.user_id() IN (SELECT user_id FROM users WHERE is_admin)`
 *  - `SELECT * FROM comments WHERE id IN request.params('id')`
 *  - `SELECT * FROM comments WHERE request.user() IN comments.tagged_users`
 */
class InOperator extends FilterOperator {
  private left: ScalarExpression;
  private right: ScalarExpression | Subquery;

  constructor(left: ScalarExpression, right: ScalarExpression | Subquery) {
    super();
    this.left = left;
    this.right = right;
  }
}

/**
 * A filter that matches if a column of the input table matches some condition that depends on the request parameters.
 *
 * E.g. `SELECT * FROM issue_id WHERE owner_id = request.user_id()`. This will add the referenced column as a bucket
 * parameter, but doesn't require a dynamic lookup.
 *
 * We only allow a few operators that are efficient to compute here.
 */
class CompareRowValueWithStreamParameter extends FilterOperator {
  private match: ParameterMatchClause;

  constructor(match: ParameterMatchClause) {
    super();
    this.match = match;
  }

  compile(context: BucketCompilationContext): void {
    const match = this.match;

    for (const filters of match.inputParameters) {
      context.currentVariant.parameters.push({
        filterRow(options) {
          const tables = { [options.sourceTable.table]: options.record };
          const matchingValues: SqliteJsonValue[] = [];

          for (const matchingParameters of match.filterRow(tables)) {
            const matchingValue = matchingParameters[filters.key];
            if (matchingValue != null) {
              matchingValues.push(matchingParameters[filters.key]);
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
class EvaluateSimpleCondition extends FilterOperator {
  private expression: ScalarExpression;

  constructor(expression: ScalarExpression) {
    super();
    this.expression = expression;
  }

  compile(context: BucketCompilationContext): void {
    if ('row' in this.expression) {
      const filter = this.expression.row;

      context.currentVariant.additionalRowFilters.push((row) => {
        return sqliteBool(filter.evaluate({ [row.sourceTable.table]: row.record })) == SQLITE_TRUE;
      });
    } else if ('parameter' in this.expression) {
      const filter = this.expression.parameter;

      context.currentVariant.additionalParameterFilters.push((request) => {
        return sqliteBool(filter.lookupParameterValue(request)) == SQLITE_TRUE;
      });
    } else {
      const _: never = this.expression; // Exhaustive check
    }
  }
}

/**
 * A filter that matches all rows.
 */
class AllRows extends FilterOperator {}
