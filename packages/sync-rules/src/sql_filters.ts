import { Expr, ExprRef, NodeLocation, SelectedColumn } from 'pgsql-ast-parser';
import { nil } from 'pgsql-ast-parser/src/utils.js';
import { ExpressionType, SqliteType, TYPE_NONE } from './ExpressionType.js';
import { SqlRuleError } from './errors.js';
import {
  BASIC_OPERATORS,
  CAST_TYPES,
  OPERATOR_IS_NOT_NULL,
  OPERATOR_IS_NULL,
  OPERATOR_JSON_EXTRACT_JSON,
  OPERATOR_JSON_EXTRACT_SQL,
  OPERATOR_NOT,
  SQL_FUNCTIONS,
  SqlFunction,
  cast,
  castOperator,
  sqliteTypeOf
} from './sql_functions.js';
import {
  SQLITE_FALSE,
  SQLITE_TRUE,
  andFilters,
  compileStaticOperator,
  getOperatorFunction,
  isClauseError,
  isParameterMatchClause,
  isParameterValueClause,
  isRowValueClause,
  isStaticValueClause,
  orFilters,
  sqliteNot,
  toBooleanParameterSetClause
} from './sql_support.js';
import {
  ClauseError,
  CompiledClause,
  InputParameter,
  ParameterMatchClause,
  ParameterValueClause,
  QueryParameters,
  QuerySchema,
  SqliteJsonRow,
  SqliteValue,
  RowValueClause,
  StaticValueClause,
  TrueIfParametersMatch
} from './types.js';
import { isJsonValue } from './utils.js';

export const MATCH_CONST_FALSE: TrueIfParametersMatch = [];
export const MATCH_CONST_TRUE: TrueIfParametersMatch = [{}];

Object.freeze(MATCH_CONST_TRUE);
Object.freeze(MATCH_CONST_FALSE);

export interface SqlToolsOptions {
  /**
   * Default table name, if any. I.e. SELECT FROM <table>.
   *
   * Used for to determine the table when using bare column names.
   */
  table?: string;

  /**
   * Set of tables used for FilterParameters.
   *
   * This is tables that can be used to filter the data on, e.g.:
   *   "bucket" (bucket parameters for data query)
   *   "token_parameters" (token parameters for parameter query)
   */
  parameter_tables?: string[];

  /**
   * Set of tables used in QueryParameters.
   *
   * If not specified, defaults to [table].
   */
  value_tables?: string[];

  /**
   * For debugging / error messages.
   */
  sql: string;

  /**
   * true if values in parameter tables can be expanded, i.e. `WHERE value IN parameters.something`.
   *
   * Only one parameter may be expanded.
   */
  supports_expanding_parameters?: boolean;

  /**
   * true if expressions on parameters are supported, e.g. upper(token_parameters.user_id)
   */
  supports_parameter_expressions?: boolean;

  schema?: QuerySchema;
}

export class SqlTools {
  default_table?: string;
  value_tables: string[];
  /**
   * ['bucket'] for data queries
   * ['token_parameters', 'user_parameters'] for parameter queries
   */
  parameter_tables: string[];
  sql: string;
  errors: SqlRuleError[] = [];

  supports_expanding_parameters: boolean;
  supports_parameter_expressions: boolean;

  schema?: QuerySchema;

  constructor(options: SqlToolsOptions) {
    this.default_table = options.table;
    this.schema = options.schema;

    if (options.value_tables) {
      this.value_tables = options.value_tables;
    } else if (this.default_table) {
      this.value_tables = [this.default_table];
    } else {
      this.value_tables = [];
    }
    this.parameter_tables = options.parameter_tables ?? [];
    this.sql = options.sql;
    this.supports_expanding_parameters = options.supports_expanding_parameters ?? false;
    this.supports_parameter_expressions = options.supports_parameter_expressions ?? false;
  }

  error(message: string, expr: NodeLocation | Expr | undefined): ClauseError {
    this.errors.push(new SqlRuleError(message, this.sql, expr));
    return { error: true };
  }

  warn(message: string, expr: NodeLocation | Expr | undefined) {
    const error = new SqlRuleError(message, this.sql, expr);
    error.type = 'warning';
    this.errors.push(error);
  }

  /**
   * Compile the where clause into a ParameterMatchClause.
   *
   * A ParameterMatchClause takes a data row, and returns filter values that
   * would make the expression true for the row.
   */
  compileWhereClause(where: Expr | nil): ParameterMatchClause {
    const base = this.compileClause(where);
    return toBooleanParameterSetClause(base);
  }

  compileStaticExtractor(expr: Expr | nil): RowValueClause | ClauseError {
    const clause = this.compileClause(expr);
    if (!isRowValueClause(clause) && !isClauseError(clause)) {
      throw new SqlRuleError('Bucket parameters are not allowed here', this.sql, expr ?? undefined);
    }
    return clause;
  }

  /**
   * Given an expression, return a compiled clause.
   */
  compileClause(expr: Expr | nil): CompiledClause {
    if (expr == null) {
      return staticValueClause(SQLITE_TRUE);
    } else if (isStatic(expr)) {
      const value = staticValue(expr);
      return staticValueClause(value);
    } else if (expr.type == 'ref') {
      const column = expr.name;
      if (column == '*') {
        return this.error('* not supported here', expr);
      }
      if (this.refHasSchema(expr)) {
        return this.error(`Schema is not supported in column references`, expr);
      }
      if (this.isParameterRef(expr)) {
        return this.getParameterRefClause(expr);
      } else if (this.isTableRef(expr)) {
        const table = this.getTableName(expr);
        this.checkRef(table, expr);
        return {
          evaluate(tables: QueryParameters): SqliteValue {
            return tables[table]?.[column];
          },
          getType(schema) {
            return schema.getType(table, column);
          }
        } satisfies RowValueClause;
      } else {
        const ref = [(expr as ExprRef).table?.schema, (expr as ExprRef).table?.name, (expr as ExprRef).name]
          .filter((e) => e != null)
          .join('.');
        return this.error(`Undefined reference: ${ref}`, expr);
      }
    } else if (expr.type == 'binary') {
      const { left, right, op } = expr;
      const leftFilter = this.compileClause(left);
      const rightFilter = this.compileClause(right);
      if (isClauseError(leftFilter) || isClauseError(rightFilter)) {
        return { error: true } satisfies ClauseError;
      }

      if (op == 'AND') {
        try {
          return andFilters(leftFilter, rightFilter);
        } catch (e) {
          return this.error(e.message, expr);
        }
      } else if (op == 'OR') {
        try {
          return orFilters(leftFilter, rightFilter);
        } catch (e) {
          return this.error(e.message, expr);
        }
      } else if (op == '=') {
        // Options:
        //  1. static, static
        //  2. static, parameterValue
        //  3. static true, parameterMatch - not supported yet

        let staticFilter1: RowValueClause;
        let otherFilter1: CompiledClause;

        if (!isRowValueClause(leftFilter) && !isRowValueClause(rightFilter)) {
          return this.error(`Cannot have bucket parameters on both sides of = operator`, expr);
        } else if (isRowValueClause(leftFilter)) {
          staticFilter1 = leftFilter;
          otherFilter1 = rightFilter;
        } else {
          staticFilter1 = rightFilter as RowValueClause;
          otherFilter1 = leftFilter;
        }
        const staticFilter = staticFilter1;
        const otherFilter = otherFilter1;

        if (isRowValueClause(otherFilter)) {
          // 1. static = static
          return compileStaticOperator(op, leftFilter as RowValueClause, rightFilter as RowValueClause);
        } else if (isParameterValueClause(otherFilter)) {
          // 2. static = parameterValue
          const inputParam = basicInputParameter(otherFilter);

          return {
            error: false,
            inputParameters: [inputParam],
            unbounded: false,
            filterRow(tables: QueryParameters): TrueIfParametersMatch {
              const value = staticFilter.evaluate(tables);
              if (value == null) {
                // null never matches on =
                // Should technically return null, but "false" is sufficient here
                return MATCH_CONST_FALSE;
              }
              if (!isJsonValue(value)) {
                // Cannot persist this, e.g. BLOB
                return MATCH_CONST_FALSE;
              }

              return [{ [inputParam.key]: value }];
            }
          } satisfies ParameterMatchClause;
        } else if (isParameterMatchClause(otherFilter)) {
          // 3. static, parameterMatch
          // (bucket.param = 'something') = staticValue
          // To implement this, we need to ensure the static value here can only be true.
          return this.error(`Parameter match clauses cannot be used here`, expr);
        } else {
          throw new Error('Unexpected');
        }
      } else if (op == 'IN') {
        // Options:
        //  static IN static
        //  parameterValue IN static

        if (isRowValueClause(leftFilter) && isRowValueClause(rightFilter)) {
          // static1 IN static2
          return compileStaticOperator(op, leftFilter, rightFilter);
        } else if (isParameterValueClause(leftFilter) && isRowValueClause(rightFilter)) {
          // token_parameters.value IN table.some_array
          // bucket.param IN table.some_array
          const inputParam = basicInputParameter(leftFilter);

          return {
            error: false,
            inputParameters: [inputParam],
            unbounded: true,
            filterRow(tables: QueryParameters): TrueIfParametersMatch {
              const aValue = rightFilter.evaluate(tables);
              if (aValue == null) {
                return MATCH_CONST_FALSE;
              }
              const values = JSON.parse(aValue as string);
              if (!Array.isArray(values)) {
                throw new Error('Not an array');
              }
              return values.map((value) => {
                return { [inputParam.key]: value };
              });
            }
          } satisfies ParameterMatchClause;
        } else if (
          this.supports_expanding_parameters &&
          isRowValueClause(leftFilter) &&
          isParameterValueClause(rightFilter)
        ) {
          // table.some_value IN token_parameters.some_array
          // This expands into "table_some_value = <value>" for each value of the array.
          // We only support one such filter per query
          const key = `${rightFilter.key}[*]`;

          const inputParam: InputParameter = {
            key: key,
            expands: true,
            filteredRowToLookupValue: (filterParameters) => {
              return filterParameters[key];
            },
            parametersToLookupValue: (parameters) => {
              return rightFilter.lookupParameterValue(parameters);
            }
          };

          return {
            error: false,
            inputParameters: [inputParam],
            unbounded: false,
            filterRow(tables: QueryParameters): TrueIfParametersMatch {
              const value = leftFilter.evaluate(tables);
              if (!isJsonValue(value)) {
                // Cannot persist, e.g. BLOB
                return MATCH_CONST_FALSE;
              }
              return [{ [inputParam.key]: value }];
            }
          } satisfies ParameterMatchClause;
        } else {
          return this.error(`Unsupported usage of IN operator`, expr);
        }
      } else if (BASIC_OPERATORS.has(op)) {
        const fnImpl = getOperatorFunction(op);
        return this.composeFunction(fnImpl, [leftFilter, rightFilter], [left, right]);
      } else {
        return this.error(`Operator not supported: ${op}`, expr);
      }
    } else if (expr.type == 'unary') {
      if (expr.op == 'NOT') {
        const clause = this.compileClause(expr.operand);
        return this.composeFunction(OPERATOR_NOT, [clause], [expr.operand]);
      } else if (expr.op == 'IS NULL') {
        const clause = this.compileClause(expr.operand);
        return this.composeFunction(OPERATOR_IS_NULL, [clause], [expr.operand]);
      } else if (expr.op == 'IS NOT NULL') {
        const clause = this.compileClause(expr.operand);
        return this.composeFunction(OPERATOR_IS_NOT_NULL, [clause], [expr.operand]);
      } else {
        return this.error(`Operator ${expr.op} is not supported`, expr);
      }
    } else if (expr.type == 'call' && expr.function?.name != null) {
      const fn = expr.function.name;
      const fnImpl = SQL_FUNCTIONS[fn];
      if (fnImpl == null) {
        return this.error(`Function '${fn}' is not defined`, expr);
      }

      const argClauses = expr.args.map((arg) => this.compileClause(arg));
      const composed = this.composeFunction(fnImpl, argClauses, expr.args);
      return composed;
    } else if (expr.type == 'member') {
      const operand = this.compileClause(expr.operand);

      if (!(typeof expr.member == 'string' && (expr.op == '->>' || expr.op == '->'))) {
        return this.error(`Unsupported member operation ${expr.op}`, expr);
      }

      const debugArgs: Expr[] = [expr.operand, expr];
      const args: CompiledClause[] = [operand, staticValueClause(expr.member)];
      if (expr.op == '->') {
        return this.composeFunction(OPERATOR_JSON_EXTRACT_JSON, args, debugArgs);
      } else {
        return this.composeFunction(OPERATOR_JSON_EXTRACT_SQL, args, debugArgs);
      }
    } else if (expr.type == 'cast') {
      const operand = this.compileClause(expr.operand);
      const to = (expr.to as any)?.name?.toLowerCase() as string | undefined;
      const castFn = castOperator(to);
      if (castFn == null) {
        return this.error(`CAST not supported for '${to}'`, expr);
      }
      return this.composeFunction(castFn, [operand], [expr.operand]);
    } else {
      return this.error(`${expr.type} not supported here`, expr);
    }
  }

  /**
   * "some_column" => "some_column"
   * "table.some_column" => "some_column".
   * "some_function() AS some_column" => "some_column"
   * "some_function() some_column" => "some_column"
   * "some_function()" => error
   */
  getOutputName(column: SelectedColumn) {
    let alias = column.alias?.name;
    if (alias) {
      return alias;
    }
    const expr = column.expr;
    if (expr.type == 'ref') {
      return expr.name;
    }
    throw new SqlRuleError(`alias is required`, this.sql, column.expr);
  }

  getSpecificOutputName(column: SelectedColumn) {
    const name = this.getOutputName(column);
    if (name == '*') {
      throw new SqlRuleError('* is not supported here - use explicit columns', this.sql, column.expr);
    }
    return name;
  }

  /**
   * Check if an expression is a parameter_table reference.
   */
  isParameterRef(expr: Expr): expr is ExprRef {
    if (expr.type != 'ref') {
      return false;
    }
    return this.parameter_tables.includes(expr.table?.name ?? '');
  }

  /**
   * Check if an expression is a value_tables reference.
   *
   * This means the expression can be evaluated directly on a value row.
   */
  isTableRef(expr: Expr): expr is ExprRef {
    if (expr.type != 'ref') {
      return false;
    }
    try {
      this.getTableName(expr);
      return true;
    } catch (e) {
      return false;
    }
  }

  private checkRef(table: string, ref: ExprRef) {
    if (this.schema) {
      const type = this.schema.getType(table, ref.name);
      if (type.typeFlags == TYPE_NONE) {
        this.warn(`Column not found: ${ref.name}`, ref);
      }
    }
  }

  getParameterRefClause(expr: ExprRef): ParameterValueClause {
    const table = expr.table!.name;
    const column = expr.name;
    return {
      key: `${table}.${column}`,
      lookupParameterValue: (parameters) => {
        const pt: SqliteJsonRow | undefined = (parameters as any)[table];
        return pt?.[column] ?? null;
      }
    } satisfies ParameterValueClause;
  }

  refHasSchema(ref: ExprRef) {
    return ref.table?.schema != null;
  }

  /**
   * Get the table name from an expression.
   *
   * Only "value" tables are supported here, not parameter values.
   */
  getTableName(ref: ExprRef) {
    if (this.refHasSchema(ref)) {
      throw new SqlRuleError(`Specifying schema in column references is not supported`, this.sql, ref);
    }
    if (ref.table?.name == null && this.default_table != null) {
      return this.default_table;
    } else if (this.value_tables.includes(ref.table?.name ?? '')) {
      return ref.table!.name;
    } else if (ref.table?.name == null) {
      throw new SqlRuleError(`Table name required`, this.sql, ref);
    } else {
      throw new SqlRuleError(`Undefined table ${ref.table?.name}`, this.sql, ref);
    }
  }

  /**
   * Given a function, compile a clause with the function over compiled arguments.
   *
   * For functions with multiple arguments, the following combinations are supported:
   * fn(StaticValueClause, StaticValueClause) => StaticValueClause
   * fn(ParameterValueClause, ParameterValueClause) => ParameterValueClause
   * fn(RowValueClause, RowValueClause) => RowValueClause
   * fn(ParameterValueClause, StaticValueClause) => ParameterValueClause
   * fn(RowValueClause, StaticValueClause) => RowValueClause
   *
   * This is not supported, and will likely never be supported:
   * fn(ParameterValueClause, RowValueClause) => error
   *
   * @param fnImpl The function or operator implementation
   * @param argClauses The compiled argument clauses
   * @param debugArgExpressions The original parsed expressions, for debug info only
   * @returns a compiled function clause
   */
  composeFunction(fnImpl: SqlFunction, argClauses: CompiledClause[], debugArgExpressions: Expr[]): CompiledClause {
    let argsType: 'static' | 'row' | 'param' = 'static';
    for (let i = 0; i < argClauses.length; i++) {
      const debugArg = debugArgExpressions[i];
      const clause = argClauses[i];
      if (isClauseError(clause)) {
        // Return immediately on error
        return clause;
      } else if (isStaticValueClause(clause)) {
        // argsType unchanged
      } else if (isParameterValueClause(clause)) {
        if (!this.supports_parameter_expressions) {
          return this.error(`Cannot use bucket parameters in expressions`, debugArg);
        }
        if (argsType == 'static' || argsType == 'param') {
          argsType = 'param';
        } else {
          return this.error(`Cannot use table values and parameters in the same clauses`, debugArg);
        }
      } else if (isRowValueClause(clause)) {
        if (argsType == 'static' || argsType == 'row') {
          argsType = 'row';
        } else {
          return this.error(`Cannot use table values and parameters in the same clauses`, debugArg);
        }
      } else {
        return this.error(`Parameter match clauses cannot be used here`, debugArg);
      }
    }

    if (argsType == 'row' || argsType == 'static') {
      return {
        evaluate: (tables) => {
          const args = argClauses.map((e) => (e as RowValueClause).evaluate(tables));
          return fnImpl.call(...args);
        },
        getType(schema) {
          const argTypes = argClauses.map((e) => (e as RowValueClause).getType(schema));
          return fnImpl.getReturnType(argTypes);
        }
      } satisfies RowValueClause;
    } else if (argsType == 'param') {
      const argStrings = argClauses.map((e) => {
        if (isParameterValueClause(e)) {
          return e.key;
        } else if (isStaticValueClause(e)) {
          return e.value;
        } else {
          throw new Error('unreachable condition');
        }
      });
      const name = `${fnImpl.debugName}(${argStrings.join(',')})`;
      return {
        key: name,
        lookupParameterValue: (parameters) => {
          const args = argClauses.map((e) => {
            if (isParameterValueClause(e)) {
              return e.lookupParameterValue(parameters);
            } else if (isStaticValueClause(e)) {
              return e.value;
            } else {
              throw new Error('unreachable condition');
            }
          });
          return fnImpl.call(...args);
        }
      } satisfies ParameterValueClause;
    } else {
      throw new Error('unreachable condition');
    }
  }
}

function isStatic(expr: Expr) {
  return ['integer', 'string', 'numeric', 'boolean', 'null'].includes(expr.type);
}

function staticValue(expr: Expr): SqliteValue {
  if (expr.type == 'boolean') {
    return expr.value ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (expr.type == 'integer') {
    return BigInt(expr.value);
  } else {
    return (expr as any).value;
  }
}

function staticValueClause(value: SqliteValue): StaticValueClause {
  return {
    value: value,
    evaluate: () => value,
    getType() {
      return ExpressionType.fromTypeText(sqliteTypeOf(value));
    }
  };
}

function basicInputParameter(clause: ParameterValueClause): InputParameter {
  return {
    key: clause.key,
    expands: false,
    filteredRowToLookupValue: (filterParameters) => {
      return filterParameters[clause.key];
    },
    parametersToLookupValue: (parameters) => {
      return clause.lookupParameterValue(parameters);
    }
  };
}
