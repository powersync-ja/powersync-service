import { SelectFromStatement } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { ColumnDefinition, ExpressionType } from './ExpressionType.js';
import { MATCH_CONST_FALSE, MATCH_CONST_TRUE } from './sql_filters.js';
import { evaluateOperator, getOperatorReturnType } from './sql_functions.js';
import {
  ClauseError,
  CompiledClause,
  FilterParameters,
  InputParameter,
  ParameterMatchClause,
  ParameterValueClause,
  ParameterValueSet,
  QueryParameters,
  QuerySchema,
  RowValueClause,
  SqliteValue,
  StaticValueClause,
  TrueIfParametersMatch
} from './types.js';

export function isParameterMatchClause(clause: CompiledClause): clause is ParameterMatchClause {
  return Array.isArray((clause as ParameterMatchClause).inputParameters);
}

export function isRowValueClause(clause: CompiledClause): clause is RowValueClause {
  return typeof (clause as RowValueClause).evaluate == 'function';
}

export function isStaticValueClause(clause: CompiledClause): clause is StaticValueClause {
  return isRowValueClause(clause) && typeof (clause as StaticValueClause).value != 'undefined';
}

export function isParameterValueClause(clause: CompiledClause): clause is ParameterValueClause {
  // noinspection SuspiciousTypeOfGuard
  return typeof (clause as ParameterValueClause).key == 'string';
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

/**
 * Applies a combinator on row values that itself is also a row value.
 */
export function composeRowValues<T extends Record<string, RowValueClause>>(options: {
  values: T;
  compose: (values: { [K in keyof T]: SqliteValue }) => SqliteValue;
  getColumnDefinition: RowValueClause['getColumnDefinition'];
}): RowValueClause {
  return {
    evaluate: function (tables: QueryParameters): SqliteValue {
      const evaluated = Object.fromEntries(
        Object.entries(options.values).map((e) => {
          const [key, clause] = e;
          return [key, clause.evaluate(tables)];
        })
      ) as { [K in keyof T]: SqliteValue };

      return options.compose(evaluated);
    },
    getColumnDefinition: function (schema: QuerySchema): ColumnDefinition | undefined {
      return options.getColumnDefinition(schema);
    },
    staticFilter: { any: true }
  };
}

/**
 * Applies a combinator on parameter values that itself is also a parameter value.
 */
export function composeParameterValues<T extends Record<string, ParameterValueClause>>(options: {
  values: T;
  key: string;
  compose: (values: { [K in keyof T]: SqliteValue }) => SqliteValue;
}): ParameterValueClause {
  const entries = Object.entries(options.values);

  return {
    visitChildren: (visitor) => entries.forEach((e) => visitor(e[1])),
    key: `${options.key}${entries.map((e) => e[1].key).join(',')}`,
    lookupParameterValue: function (parameters: ParameterValueSet): SqliteValue {
      const evaluated = Object.fromEntries(
        Object.entries(options.values).map((e) => {
          const [key, clause] = e;
          return [key, clause.lookupParameterValue(parameters)];
        })
      ) as { [K in keyof T]: SqliteValue };

      return options.compose(evaluated);
    }
  };
}

export function compileStaticOperator(op: string, left: RowValueClause, right: RowValueClause): RowValueClause {
  return {
    evaluate: (tables) => {
      const leftValue = left.evaluate(tables);
      const rightValue = right.evaluate(tables);
      return evaluateOperator(op, leftValue, rightValue);
    },
    getColumnDefinition(schema) {
      const typeLeft = left.getColumnDefinition(schema)?.type ?? ExpressionType.NONE;
      const typeRight = right.getColumnDefinition(schema)?.type ?? ExpressionType.NONE;
      const type = getOperatorReturnType(op, typeLeft, typeRight);
      return {
        name: '?',
        type
      };
    },
    staticFilter: {
      operator: op as any,
      left: left.staticFilter,
      right: right.staticFilter
    }
  };
}

export function andFilters(a: CompiledClause, b: CompiledClause): CompiledClause {
  // Optimizations: If the two clauses both only depend on row or parameter data, we can merge them into a single
  // clause.
  if (isRowValueClause(a) && isRowValueClause(b)) {
    return {
      ...composeRowValues({
        values: { a, b },
        compose(values) {
          return sqliteBool(sqliteBool(values.a) && sqliteBool(values.b));
        },
        getColumnDefinition() {
          return { name: 'and', type: ExpressionType.INTEGER };
        }
      }),
      staticFilter: {
        and: [a.staticFilter, b.staticFilter]
      }
    };
  }
  if (isParameterValueClause(a) && isParameterValueClause(b)) {
    return composeParameterValues({
      values: { a, b },
      key: 'and',
      compose(values) {
        return sqliteBool(sqliteBool(values.a) && sqliteBool(values.b));
      }
    });
  }

  const aFilter = toBooleanParameterSetClause(a);
  const bFilter = toBooleanParameterSetClause(b);

  const aParams = aFilter.inputParameters;
  const bParams = bFilter.inputParameters;

  if (aFilter.unbounded && bFilter.unbounded) {
    // This could explode the number of buckets for the row
    throw new Error('Cannot have multiple IN expressions on bucket parameters');
  }

  const combinedMap = new Map([...aParams, ...bParams].map((p) => [p.key, p]));

  return {
    error: aFilter.error || bFilter.error,
    inputParameters: [...combinedMap.values()],
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
    },
    visitChildren: (visitor) => {
      visitor(aFilter);
      visitor(bFilter);
    },
    staticFilter: {
      and: [aFilter.staticFilter, bFilter.staticFilter]
    }
  } satisfies ParameterMatchClause;
}

export function orFilters(a: CompiledClause, b: CompiledClause): CompiledClause {
  // Optimizations: If the two clauses both only depend on row or parameter data, we can merge them into a single
  // clause.
  if (isRowValueClause(a) && isRowValueClause(b)) {
    return {
      ...composeRowValues({
        values: { a, b },
        compose(values) {
          return sqliteBool(sqliteBool(values.a) || sqliteBool(values.b));
        },
        getColumnDefinition() {
          return { name: 'or', type: ExpressionType.INTEGER };
        }
      }),
      staticFilter: {
        or: [a.staticFilter, b.staticFilter]
      }
    };
  }
  if (isParameterValueClause(a) && isParameterValueClause(b)) {
    return composeParameterValues({
      values: { a, b },
      key: 'or',
      compose(values) {
        return sqliteBool(sqliteBool(values.a) || sqliteBool(values.b));
      }
    });
  }

  const aFilter = toBooleanParameterSetClause(a);
  const bFilter = toBooleanParameterSetClause(b);
  return orParameterSetClauses(aFilter, bFilter);
}

export function orParameterSetClauses(a: ParameterMatchClause, b: ParameterMatchClause): ParameterMatchClause {
  const aParams = a.inputParameters;
  const bParams = b.inputParameters;

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
    inputParameters: parameters,
    unbounded, // result count = a.count + b.count
    filterRow: (tables) => {
      const aResult = a.filterRow(tables);
      const bResult = b.filterRow(tables);

      let results: FilterParameters[] = [...aResult, ...bResult];
      return results;
    },
    specialType: 'or',
    visitChildren: (v) => {
      v(a);
      v(b);
    },
    staticFilter: {
      or: [a.staticFilter, b.staticFilter]
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
  } else if (isRowValueClause(clause)) {
    return {
      error: false,
      inputParameters: [],
      unbounded: false,
      filterRow(tables: QueryParameters): TrueIfParametersMatch {
        const value = sqliteBool(clause.evaluate(tables));
        return value ? MATCH_CONST_TRUE : MATCH_CONST_FALSE;
      },
      visitChildren: (v) => v(clause),
      staticFilter: clause.staticFilter
    } satisfies ParameterMatchClause;
  } else if (isClauseError(clause)) {
    return {
      error: true,
      inputParameters: [],
      unbounded: false,
      filterRow(tables: QueryParameters): TrueIfParametersMatch {
        throw new Error('invalid clause');
      },
      visitChildren: (v) => v(clause),
      staticFilter: { any: true }
    } satisfies ParameterMatchClause;
  } else {
    // Equivalent to `bucket.param = true`
    const key = clause.key;

    const inputParam: InputParameter = {
      key: key,
      expands: false,
      filteredRowToLookupValue: (filterParameters) => {
        return filterParameters[key];
      },
      parametersToLookupValue: (parameters) => {
        const inner = clause.lookupParameterValue(parameters);
        return sqliteBool(inner);
      }
    };

    return {
      error: false,
      inputParameters: [inputParam],
      unbounded: false,
      filterRow(tables: QueryParameters): TrueIfParametersMatch {
        return [{ [key]: SQLITE_TRUE }];
      },
      visitChildren: (v) => v(clause),
      staticFilter: { any: true }
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
