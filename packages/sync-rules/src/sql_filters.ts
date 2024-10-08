import { Expr, ExprRef, Name, NodeLocation, QName, QNameAliased, SelectedColumn, parse } from 'pgsql-ast-parser';
import { nil } from 'pgsql-ast-parser/src/utils.js';
import { ExpressionType, TYPE_NONE } from './ExpressionType.js';
import { SqlRuleError } from './errors.js';
import {
  BASIC_OPERATORS,
  OPERATOR_IN,
  OPERATOR_IS_NOT_NULL,
  OPERATOR_IS_NULL,
  OPERATOR_JSON_EXTRACT_JSON,
  OPERATOR_JSON_EXTRACT_SQL,
  OPERATOR_NOT,
  SQL_FUNCTIONS,
  SqlFunction,
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
  RowValueClause,
  SqliteJsonRow,
  SqliteValue,
  StaticValueClause,
  TrueIfParametersMatch
} from './types.js';
import { isJsonValue } from './utils.js';
import { JSONBig } from '@powersync/service-jsonbig';
import { REQUEST_FUNCTIONS } from './request_functions.js';

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

  /**
   * Schema for validations.
   */
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

  compileRowValueExtractor(expr: Expr | nil): RowValueClause | ClauseError {
    const clause = this.compileClause(expr);
    if (!isRowValueClause(clause) && !isClauseError(clause)) {
      return this.error('Parameter match expression is not allowed here', expr ?? undefined);
    }
    return clause;
  }

  compileParameterValueExtractor(expr: Expr | nil): ParameterValueClause | StaticValueClause | ClauseError {
    const clause = this.compileClause(expr);

    if (isClauseError(clause) || isStaticValueClause(clause) || isParameterValueClause(clause)) {
      return clause;
    }

    return this.error('Parameter match expression is not allowed here', expr ?? undefined);
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
      this.checkRefCase(expr);
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
          getColumnDefinition(schema) {
            return schema.getColumn(table, column);
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
        //  1. row value, row value
        //  2. row value, parameter value
        //  3. static true, parameterMatch - not supported yet
        //  4. parameter value, parameter value

        let staticFilter1: RowValueClause;
        let otherFilter1: CompiledClause;

        if (
          this.supports_parameter_expressions &&
          isParameterValueClause(leftFilter) &&
          isParameterValueClause(rightFilter)
        ) {
          // 4. parameterValue, parameterValue
          // This includes (static value, parameter value)
          // Not applicable to data queries (composeFunction will error).
          // Some of those cases can still be handled with case (2),
          // so we filter for supports_parameter_expressions above.
          const fnImpl = getOperatorFunction('=');
          return this.composeFunction(fnImpl, [leftFilter, rightFilter], [left, right]);
        }

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
          // 1. row value = row value
          return compileStaticOperator(op, leftFilter as RowValueClause, rightFilter as RowValueClause);
        } else if (isParameterValueClause(otherFilter)) {
          // 2. row value = parameter value
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
            },
            usesAuthenticatedRequestParameters: otherFilter.usesAuthenticatedRequestParameters,
            usesUnauthenticatedRequestParameters: otherFilter.usesUnauthenticatedRequestParameters
          } satisfies ParameterMatchClause;
        } else if (isParameterMatchClause(otherFilter)) {
          // 3. row value = parameterMatch
          // (bucket.param = 'something') = staticValue
          // To implement this, we need to ensure the static value here can only be true.
          return this.error(`Parameter match clauses cannot be used here`, expr);
        } else {
          throw new Error('Unexpected');
        }
      } else if (op == 'IN') {
        // Special cases:
        //  parameterValue IN rowValue
        //  rowValue IN parameterValue
        // All others are handled by standard function composition

        const composeType = this.getComposeType(OPERATOR_IN, [leftFilter, rightFilter], [left, right]);
        if (composeType.errorClause != null) {
          return composeType.errorClause;
        } else if (composeType.argsType != null) {
          // This is a standard supported configuration, takes precedence over
          // the special cases below.
          return this.composeFunction(OPERATOR_IN, [leftFilter, rightFilter], [left, right]);
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
            },
            usesAuthenticatedRequestParameters: leftFilter.usesAuthenticatedRequestParameters,
            usesUnauthenticatedRequestParameters: leftFilter.usesUnauthenticatedRequestParameters
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
            },
            usesAuthenticatedRequestParameters: rightFilter.usesAuthenticatedRequestParameters,
            usesUnauthenticatedRequestParameters: rightFilter.usesUnauthenticatedRequestParameters
          } satisfies ParameterMatchClause;
        } else {
          // Not supported, return the error previously computed
          return this.error(composeType.error!, composeType.errorExpr);
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
      const schema = expr.function.schema; // schema.function()
      const fn = expr.function.name;
      if (schema == null) {
        // Just fn()
        const fnImpl = SQL_FUNCTIONS[fn];
        if (fnImpl == null) {
          return this.error(`Function '${fn}' is not defined`, expr);
        }

        const argClauses = expr.args.map((arg) => this.compileClause(arg));
        const composed = this.composeFunction(fnImpl, argClauses, expr.args);
        return composed;
      } else if (schema == 'request') {
        // Special function
        if (!this.supports_parameter_expressions) {
          return this.error(`${schema} schema is not available in data queries`, expr);
        }

        if (expr.args.length > 0) {
          return this.error(`Function '${schema}.${fn}' does not take arguments`, expr);
        }

        if (fn in REQUEST_FUNCTIONS) {
          const fnImpl = REQUEST_FUNCTIONS[fn];
          return {
            key: 'request.parameters()',
            lookupParameterValue(parameters) {
              return fnImpl.call(parameters);
            },
            usesAuthenticatedRequestParameters: fnImpl.usesAuthenticatedRequestParameters,
            usesUnauthenticatedRequestParameters: fnImpl.usesUnauthenticatedRequestParameters
          } satisfies ParameterValueClause;
        } else {
          return this.error(`Function '${schema}.${fn}' is not defined`, expr);
        }
      } else {
        // Unknown function with schema
        return this.error(`Function '${schema}.${fn}' is not defined`, expr);
      }
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

  public checkRefCase(ref: ExprRef) {
    if (ref.table != null) {
      this.checkSpecificNameCase(ref.table);
    }
    this.checkColumnNameCase(ref);
  }

  private checkColumnNameCase(expr: ExprRef) {
    if (expr.name.toLowerCase() != expr.name) {
      // name is not lower case, must be quoted
      return;
    }

    let location = expr._location;
    if (location == null) {
      return;
    }
    const tableLocation = expr.table?._location;
    if (tableLocation != null) {
      // exp._location contains the entire expression.
      // We use this to remove the "table" part.
      location = { start: tableLocation.end + 1, end: location.end };
    }
    const source = this.sql.substring(location.start, location.end);
    if (source.toLowerCase() != source) {
      // source is not lower case, while parsed is lower-case
      this.warn(`Unquoted identifiers are converted to lower-case. Use "${source}" instead.`, location);
    }
  }

  /**
   * Check the case of a table name or any alias.
   */
  public checkSpecificNameCase(expr: Name | QName | QNameAliased) {
    if ((expr as QNameAliased).alias != null || (expr as QName).schema != null) {
      // We cannot properly distinguish alias and schema from the name itself,
      // without building our own complete parser, so we ignore this for now.
      return;
    }
    if (expr.name.toLowerCase() != expr.name) {
      // name is not lower case, which means it is already quoted
      return;
    }

    const location = expr._location;
    if (location == null) {
      return;
    }
    const source = this.sql.substring(location.start, location.end);
    if (source.toLowerCase() != source) {
      // source is not lower case
      this.warn(`Unquoted identifiers are converted to lower-case. Use "${source}" instead.`, location);
    }
  }

  private checkRef(table: string, ref: ExprRef) {
    if (this.schema) {
      const type = this.schema.getColumn(table, ref.name);
      if (type == null) {
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
      },
      usesAuthenticatedRequestParameters: table == 'token_parameters',
      usesUnauthenticatedRequestParameters: table == 'user_parameters'
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
    const result = this.getComposeType(fnImpl, argClauses, debugArgExpressions);
    if (result.errorClause != null) {
      return result.errorClause;
    } else if (result.error != null) {
      return this.error(result.error, result.errorExpr);
    }
    const argsType = result.argsType!;

    if (argsType == 'static') {
      const args = argClauses.map((e) => (e as StaticValueClause).value);
      const evaluated = fnImpl.call(...args);
      return staticValueClause(evaluated);
    } else if (argsType == 'row') {
      return {
        evaluate: (tables) => {
          const args = argClauses.map((e) => (e as RowValueClause).evaluate(tables));
          return fnImpl.call(...args);
        },
        getColumnDefinition(schema) {
          const argTypes = argClauses.map(
            (e) => (e as RowValueClause).getColumnDefinition(schema)?.type ?? ExpressionType.NONE
          );
          return { name: `${fnImpl}()`, type: fnImpl.getReturnType(argTypes) };
        }
      } satisfies RowValueClause;
    } else if (argsType == 'param') {
      const argStrings = argClauses.map((e) => (e as ParameterValueClause).key);
      const name = `${fnImpl.debugName}(${argStrings.join(',')})`;
      const usesAuthenticatedRequestParameters =
        argClauses.find((clause) => isParameterValueClause(clause) && clause.usesAuthenticatedRequestParameters) !=
        null;
      const usesUnauthenticatedRequestParameters =
        argClauses.find((clause) => isParameterValueClause(clause) && clause.usesUnauthenticatedRequestParameters) !=
        null;
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
        },
        usesAuthenticatedRequestParameters,
        usesUnauthenticatedRequestParameters
      } satisfies ParameterValueClause;
    } else {
      throw new Error('unreachable condition');
    }
  }

  getComposeType(
    fnImpl: SqlFunction,
    argClauses: CompiledClause[],
    debugArgExpressions: Expr[]
  ): { argsType?: string; error?: string; errorExpr?: Expr; errorClause?: ClauseError } {
    let argsType: 'static' | 'row' | 'param' = 'static';
    for (let i = 0; i < argClauses.length; i++) {
      const debugArg = debugArgExpressions[i];
      const clause = argClauses[i];
      if (isClauseError(clause)) {
        // Return immediately on error
        return { errorClause: clause };
      } else if (isStaticValueClause(clause)) {
        // argsType unchanged
      } else if (isParameterValueClause(clause)) {
        if (!this.supports_parameter_expressions) {
          if (fnImpl.debugName == 'operatorIN') {
            // Special-case error message to be more descriptive
            return { error: `Cannot use bucket parameters on the right side of IN operators`, errorExpr: debugArg };
          }
          return { error: `Cannot use bucket parameters in expressions`, errorExpr: debugArg };
        }
        if (argsType == 'static' || argsType == 'param') {
          argsType = 'param';
        } else {
          return { error: `Cannot use table values and parameters in the same clauses`, errorExpr: debugArg };
        }
      } else if (isRowValueClause(clause)) {
        if (argsType == 'static' || argsType == 'row') {
          argsType = 'row';
        } else {
          return { error: `Cannot use table values and parameters in the same clauses`, errorExpr: debugArg };
        }
      } else {
        return { error: `Parameter match clauses cannot be used here`, errorExpr: debugArg };
      }
    }

    return {
      argsType
    };
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
    // RowValueClause compatibility
    evaluate: () => value,
    getColumnDefinition() {
      return {
        name: 'literal',
        type: ExpressionType.fromTypeText(sqliteTypeOf(value))
      };
    },
    // ParamterValueClause compatibility
    key: JSONBig.stringify(value),
    lookupParameterValue(_parameters) {
      return value;
    },
    usesAuthenticatedRequestParameters: false,
    usesUnauthenticatedRequestParameters: false
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
