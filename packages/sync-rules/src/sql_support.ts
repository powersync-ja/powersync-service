import {
  ClauseError,
  CompiledClause,
  FilterParameters,
  InputParameter,
  ParameterMatchClause,
  ParameterValueClause,
  QueryParameters,
  SqliteJsonRow,
  SqliteValue,
  StaticRowValueClause,
  StaticValueClause,
  TrueIfParametersMatch
} from './types.js';
import { MATCH_CONST_FALSE, MATCH_CONST_TRUE } from './sql_filters.js';
import { SqlFunction, evaluateOperator, getOperatorReturnType } from './sql_functions.js';
import { SelectFromStatement } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { ExpressionType } from './ExpressionType.js';

export function isParameterMatchClause(clause: CompiledClause): clause is ParameterMatchClause {
  return Array.isArray((clause as ParameterMatchClause).bucketParameters);
}

export function isStaticRowValueClause(clause: CompiledClause): clause is StaticRowValueClause {
  return typeof (clause as StaticRowValueClause).evaluate == 'function';
}

export function isStaticValueClause(clause: CompiledClause): clause is StaticValueClause {
  return isStaticRowValueClause(clause) && typeof (clause as StaticValueClause).value != 'undefined';
}

export function isParameterValueClause(clause: CompiledClause): clause is ParameterValueClause {
  // noinspection SuspiciousTypeOfGuard
  return typeof (clause as ParameterValueClause).bucketParameter == 'string';
}

export function isClauseError(clause: CompiledClause): clause is ClauseError {
  return (clause as ClauseError).error === true;
}

export const SQLITE_TRUE = 1n;
export const SQLITE_FALSE = 0n;

export function sqliteBool(value: SqliteValue | boolean): 1n | 0n {
  if (value == null) {
    return SQLITE_FALSE;
  } else if (typeof value == 'boolean' || typeof value == 'number') {
    return value ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (typeof value == 'bigint') {
    return value != 0n ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (typeof value == 'string') {
    return parseInt(value) ? SQLITE_TRUE : SQLITE_FALSE;
  } else {
    return SQLITE_FALSE;
  }
}

export function sqliteNot(value: SqliteValue | boolean) {
  return sqliteBool(!sqliteBool(value));
}

export function compileStaticOperator(
  op: string,
  left: StaticRowValueClause,
  right: StaticRowValueClause
): StaticRowValueClause {
  return {
    evaluate: (tables) => {
      const leftValue = left.evaluate(tables);
      const rightValue = right.evaluate(tables);
      return evaluateOperator(op, leftValue, rightValue);
    },
    getType(schema) {
      const typeLeft = left.getType(schema);
      const typeRight = right.getType(schema);
      return getOperatorReturnType(op, typeLeft, typeRight);
    }
  };
}

export function getOperatorFunction(op: string): SqlFunction {
  return {
    debugName: `operator${op}`,
    call(...args: SqliteValue[]) {
      return evaluateOperator(op, args[0], args[1]);
    },
    getReturnType(args) {
      return getOperatorReturnType(op, args[0], args[1]);
    },
    parameters: [
      { name: 'left', type: ExpressionType.ANY, optional: false },
      { name: 'right', type: ExpressionType.ANY, optional: false }
    ]
  };
}

export function andFilters(a: CompiledClause, b: CompiledClause): CompiledClause {
  if (isStaticRowValueClause(a) && isStaticRowValueClause(b)) {
    // Optimization
    return {
      evaluate(tables: QueryParameters): SqliteValue {
        const aValue = sqliteBool(a.evaluate(tables));
        const bValue = sqliteBool(b.evaluate(tables));
        return sqliteBool(aValue && bValue);
      },
      getType() {
        return ExpressionType.INTEGER;
      }
    };
  }

  const aFilter = toBooleanParameterSetClause(a);
  const bFilter = toBooleanParameterSetClause(b);

  const aParams = aFilter.bucketParameters;
  const bParams = bFilter.bucketParameters;

  if (aFilter.unbounded && bFilter.unbounded) {
    // This could explode the number of buckets for the row
    throw new Error('Cannot have multiple IN expressions on bucket parameters');
  }

  const combinedMap = new Map([...aParams, ...bParams].map((p) => [p.key, p]));

  return {
    error: aFilter.error || bFilter.error,
    bucketParameters: [...combinedMap.values()],
    unbounded: aFilter.unbounded || bFilter.unbounded, // result count = a.count * b.count
    filterRow: (tables) => {
      const aResult = aFilter.filterRow(tables);
      const bResult = bFilter.filterRow(tables);

      let results: FilterParameters[] = [];
      for (let result1 of aResult) {
        for (let result2 of bResult) {
          let combined = { ...result1 };
          let valid = true;
          for (let key in result2) {
            if (key in combined && combined[key] != result2[key]) {
              valid = false;
              break;
            }
            combined[key] = result2[key];
          }

          results.push(combined);
        }
      }
      return results;
    }
  } satisfies ParameterMatchClause;
}

export function orFilters(a: CompiledClause, b: CompiledClause): CompiledClause {
  if (isStaticRowValueClause(a) && isStaticRowValueClause(b)) {
    // Optimization
    return {
      evaluate(tables: QueryParameters): SqliteValue {
        const aValue = sqliteBool(a.evaluate(tables));
        const bValue = sqliteBool(b.evaluate(tables));
        return sqliteBool(aValue || bValue);
      },
      getType() {
        return ExpressionType.INTEGER;
      }
    } satisfies StaticRowValueClause;
  }

  const aFilter = toBooleanParameterSetClause(a);
  const bFilter = toBooleanParameterSetClause(b);
  return orParameterSetClauses(aFilter, bFilter);
}

export function orParameterSetClauses(a: ParameterMatchClause, b: ParameterMatchClause): ParameterMatchClause {
  const aParams = a.bucketParameters;
  const bParams = b.bucketParameters;

  // This gives the guaranteed set of parameters matched against.
  const combinedMap = new Map([...aParams, ...bParams].map((p) => [p.key, p]));
  if (combinedMap.size != aParams.length || combinedMap.size != bParams.length) {
    throw new Error(
      `Left and right sides of OR must use the same parameters, or split into separate queries. ${JSON.stringify(
        aParams
      )} != ${JSON.stringify(bParams)}`
    );
  }

  const parameters = [...combinedMap.values()];

  // assets.region_id = bucket.region_id AND bucket.user_id IN assets.user_ids
  // OR bucket.region_id IN assets.region_ids AND bucket.user_id = assets.user_id

  const unbounded = a.unbounded || b.unbounded;
  return {
    error: a.error || b.error,
    bucketParameters: parameters,
    unbounded, // result count = a.count + b.count
    filterRow: (tables) => {
      const aResult = a.filterRow(tables);
      const bResult = b.filterRow(tables);

      let results: FilterParameters[] = [...aResult, ...bResult];
      return results;
    }
  } satisfies ParameterMatchClause;
}

/**
 * Given any CompiledClause, convert it into a ParameterMatchClause.
 *
 * @param clause
 */
export function toBooleanParameterSetClause(clause: CompiledClause): ParameterMatchClause {
  if (isParameterMatchClause(clause)) {
    return clause;
  } else if (isStaticRowValueClause(clause)) {
    return {
      error: false,
      bucketParameters: [],
      unbounded: false,
      filterRow(tables: QueryParameters): TrueIfParametersMatch {
        const value = sqliteBool(clause.evaluate(tables));
        return value ? MATCH_CONST_TRUE : MATCH_CONST_FALSE;
      }
    } satisfies ParameterMatchClause;
  } else if (isClauseError(clause)) {
    return {
      error: true,
      bucketParameters: [],
      unbounded: false,
      filterRow(tables: QueryParameters): TrueIfParametersMatch {
        throw new Error('invalid clause');
      }
    } satisfies ParameterMatchClause;
  } else {
    // Equivalent to `bucket.param = true`
    const paramName = clause.bucketParameter;
    const [table, column] = clause.bucketParameter.split('.');

    const inputParam: InputParameter = {
      key: paramName,
      expands: false,
      filteredRowToLookupValue: (filterParameters) => {
        return filterParameters[paramName];
      },
      parametersToLookupValue: (parameters) => {
        const pt: SqliteJsonRow | undefined = (parameters as any)[table];
        return pt?.[column] ?? null;
      }
    };

    return {
      error: false,
      bucketParameters: [inputParam],
      unbounded: false,
      filterRow(tables: QueryParameters): TrueIfParametersMatch {
        return [{ [paramName]: SQLITE_TRUE }];
      }
    } satisfies ParameterMatchClause;
  }
}

export function checkUnsupportedFeatures(sql: string, q: SelectFromStatement) {
  let errors: SqlRuleError[] = [];
  if (q.limit != null) {
    errors.push(new SqlRuleError('LIMIT is not supported', sql, q.limit._location));
  }

  if (q.orderBy != null) {
    errors.push(new SqlRuleError('ORDER BY is not supported', sql, q.orderBy[0]?._location));
  }

  if (q.skip != null) {
    errors.push(new SqlRuleError('SKIP is not supported', sql, q.skip._location));
  }

  if (q.having != null) {
    errors.push(new SqlRuleError('HAVING is not supported', sql, q.having._location));
  }

  if (q.groupBy != null) {
    errors.push(new SqlRuleError('GROUP BY is not supported', sql, q.groupBy[0]?._location));
  }

  if (q.distinct != null) {
    errors.push(new SqlRuleError('DISTINCT is not supported', sql));
  }

  if (q.for != null) {
    errors.push(new SqlRuleError('SELECT FOR is not supported', sql, q.for._location));
  }

  return errors;
}
