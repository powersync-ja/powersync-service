import { Expr, ExprRef, NodeLocation, SelectedColumn } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { BASIC_OPERATORS, cast, CAST_TYPES, jsonExtract, SQL_FUNCTIONS, sqliteTypeOf } from './sql_functions.js';
import {
  ClauseError,
  CompiledClause,
  ParameterMatchClause,
  QueryParameters,
  QuerySchema,
  SqliteValue,
  StaticRowValueClause,
  TrueIfParametersMatch
} from './types.js';
import { nil } from 'pgsql-ast-parser/src/utils.js';
import { isJsonValue } from './utils.js';
import {
  andFilters,
  compileStaticOperator,
  isClauseError,
  isParameterMatchClause,
  isParameterValueClause,
  isStaticRowValueClause,
  orFilters,
  SQLITE_FALSE,
  SQLITE_TRUE,
  sqliteNot,
  toBooleanParameterSetClause
} from './sql_support.js';
import { ExpressionType, SqliteType, TYPE_NONE } from './ExpressionType.js';

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

  schema?: QuerySchema;
}

export class SqlTools {
  default_table?: string;
  value_tables: string[];
  parameter_tables: string[];
  sql: string;
  errors: SqlRuleError[] = [];

  supports_expanding_parameters: boolean;
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

  compileStaticExtractor(expr: Expr | nil): StaticRowValueClause | ClauseError {
    const clause = this.compileClause(expr);
    if (!isStaticRowValueClause(clause) && !isClauseError(clause)) {
      throw new SqlRuleError('Bucket parameters are not allowed here', this.sql, expr ?? undefined);
    }
    return clause;
  }

  /**
   * Given an expression, return a compiled clause.
   */
  compileClause(expr: Expr | nil): CompiledClause {
    if (expr == null) {
      return {
        evaluate: () => SQLITE_TRUE,
        getType() {
          return ExpressionType.INTEGER;
        }
      } satisfies StaticRowValueClause;
    } else if (isStatic(expr)) {
      const value = staticValue(expr);
      return {
        evaluate: () => value,
        getType() {
          return ExpressionType.fromTypeText(sqliteTypeOf(value));
        }
      } satisfies StaticRowValueClause;
    } else if (expr.type == 'ref') {
      const column = expr.name;
      if (column == '*') {
        return this.error('* not supported here', expr);
      }
      if (this.refHasSchema(expr)) {
        return this.error(`Schema is not supported in column references`, expr);
      }
      if (this.isParameterRef(expr)) {
        const param = this.getParameterRef(expr)!;
        return { bucketParameter: param };
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
        } satisfies StaticRowValueClause;
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
        return { error: true } as ClauseError;
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

        let staticFilter1: StaticRowValueClause;
        let otherFilter1: CompiledClause;

        if (!isStaticRowValueClause(leftFilter) && !isStaticRowValueClause(rightFilter)) {
          return this.error(`Cannot have bucket parameters on both sides of = operator`, expr);
        } else if (isStaticRowValueClause(leftFilter)) {
          staticFilter1 = leftFilter;
          otherFilter1 = rightFilter;
        } else {
          staticFilter1 = rightFilter as StaticRowValueClause;
          otherFilter1 = leftFilter;
        }
        const staticFilter = staticFilter1;
        const otherFilter = otherFilter1;

        if (isStaticRowValueClause(otherFilter)) {
          // 1. static, static
          return compileStaticOperator(op, leftFilter as StaticRowValueClause, rightFilter as StaticRowValueClause);
        } else if (isParameterValueClause(otherFilter)) {
          // 2. static, parameterValue
          return {
            error: false,
            bucketParameters: [otherFilter.bucketParameter],
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
              return [{ [otherFilter.bucketParameter]: value }];
            }
          } satisfies ParameterMatchClause;
        } else if (isParameterMatchClause(otherFilter)) {
          // 3. static, parameterMatch
          // (bucket.param = 'something') = staticValue
          // To implement this, we need to ensure the static value here can only be true.
          return this.error(
            `Bucket parameter clauses cannot currently be combined with other operators`,

            expr
          );
        } else {
          throw new Error('Unexpected');
        }
      } else if (op == 'IN') {
        // Options:
        //  static IN static
        //  parameterValue IN static

        if (isStaticRowValueClause(leftFilter) && isStaticRowValueClause(rightFilter)) {
          return compileStaticOperator(op, leftFilter, rightFilter);
        } else if (isParameterValueClause(leftFilter) && isStaticRowValueClause(rightFilter)) {
          const param = leftFilter.bucketParameter;
          return {
            error: false,
            bucketParameters: [param],
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
                return { [param]: value };
              });
            }
          } satisfies ParameterMatchClause;
        } else if (
          this.supports_expanding_parameters &&
          isStaticRowValueClause(leftFilter) &&
          isParameterValueClause(rightFilter)
        ) {
          const param = `${rightFilter.bucketParameter}[*]`;
          return {
            error: false,
            bucketParameters: [param],
            unbounded: false,
            filterRow(tables: QueryParameters): TrueIfParametersMatch {
              const value = leftFilter.evaluate(tables);
              if (!isJsonValue(value)) {
                // Cannot persist, e.g. BLOB
                return MATCH_CONST_FALSE;
              }
              return [{ [param]: value }];
            }
          } satisfies ParameterMatchClause;
        } else {
          return this.error(`Unsupported usage of IN operator`, expr);
        }
      } else if (BASIC_OPERATORS.has(op)) {
        if (!isStaticRowValueClause(leftFilter) || !isStaticRowValueClause(rightFilter)) {
          return this.error(`Operator ${op} is not supported on bucket parameters`, expr);
        }
        return compileStaticOperator(op, leftFilter, rightFilter);
      } else {
        return this.error(`Operator not supported: ${op}`, expr);
      }
    } else if (expr.type == 'unary') {
      if (expr.op == 'NOT') {
        const filter = this.compileClause(expr.operand);
        if (isClauseError(filter)) {
          return filter;
        } else if (!isStaticRowValueClause(filter)) {
          return this.error('Cannot use NOT on bucket parameter filters', expr);
        }

        return {
          evaluate: (tables) => {
            const value = filter.evaluate(tables);
            return sqliteNot(value);
          },
          getType() {
            return ExpressionType.INTEGER;
          }
        } satisfies StaticRowValueClause;
      } else if (expr.op == 'IS NULL') {
        const leftFilter = this.compileClause(expr.operand);
        if (isClauseError(leftFilter)) {
          return leftFilter;
        } else if (isStaticRowValueClause(leftFilter)) {
          //  1. static IS NULL
          const nullValue: StaticRowValueClause = {
            evaluate: () => null,
            getType() {
              return ExpressionType.INTEGER;
            }
          } satisfies StaticRowValueClause;
          return compileStaticOperator('IS', leftFilter, nullValue);
        } else if (isParameterValueClause(leftFilter)) {
          //  2. param IS NULL
          return {
            error: false,
            bucketParameters: [leftFilter.bucketParameter],
            unbounded: false,
            filterRow(tables: QueryParameters): TrueIfParametersMatch {
              return [{ [leftFilter.bucketParameter]: null }];
            }
          } satisfies ParameterMatchClause;
        } else {
          return this.error(`Cannot use IS NULL here`, expr);
        }
      } else if (expr.op == 'IS NOT NULL') {
        const leftFilter = this.compileClause(expr.operand);
        if (isClauseError(leftFilter)) {
          return leftFilter;
        } else if (isStaticRowValueClause(leftFilter)) {
          //  1. static IS NULL
          const nullValue: StaticRowValueClause = {
            evaluate: () => null,
            getType() {
              return ExpressionType.INTEGER;
            }
          } satisfies StaticRowValueClause;
          return compileStaticOperator('IS NOT', leftFilter, nullValue);
        } else {
          return this.error(`Cannot use IS NOT NULL here`, expr);
        }
      } else {
        return this.error(`Operator ${expr.op} is not supported`, expr);
      }
    } else if (expr.type == 'call' && expr.function?.name != null) {
      const fn = expr.function.name;
      const fnImpl = SQL_FUNCTIONS[fn];
      if (fnImpl == null) {
        return this.error(`Function '${fn}' is not defined`, expr);
      }

      let error = false;
      const argExtractors = expr.args.map((arg) => {
        const clause = this.compileClause(arg);

        if (isClauseError(clause)) {
          error = true;
        } else if (!isStaticRowValueClause(clause)) {
          error = true;
          return this.error(`Bucket parameters are not supported in function call arguments`, arg);
        }
        return clause;
      }) as StaticRowValueClause[];

      if (error) {
        return { error: true };
      }

      return {
        evaluate: (tables) => {
          const args = argExtractors.map((e) => e.evaluate(tables));
          return fnImpl.call(...args);
        },
        getType(schema) {
          const argTypes = argExtractors.map((e) => e.getType(schema));
          return fnImpl.getReturnType(argTypes);
        }
      } satisfies StaticRowValueClause;
    } else if (expr.type == 'member') {
      const operand = this.compileClause(expr.operand);
      if (isClauseError(operand)) {
        return operand;
      } else if (!isStaticRowValueClause(operand)) {
        return this.error(`Bucket parameters are not supported in member lookups`, expr.operand);
      }

      if (typeof expr.member == 'string' && (expr.op == '->>' || expr.op == '->')) {
        return {
          evaluate: (tables) => {
            const containerString = operand.evaluate(tables);
            return jsonExtract(containerString, expr.member, expr.op);
          },
          getType() {
            return ExpressionType.ANY_JSON;
          }
        } satisfies StaticRowValueClause;
      } else {
        return this.error(`Unsupported member operation ${expr.op}`, expr);
      }
    } else if (expr.type == 'cast') {
      const operand = this.compileClause(expr.operand);
      if (isClauseError(operand)) {
        return operand;
      } else if (!isStaticRowValueClause(operand)) {
        return this.error(`Bucket parameters are not supported in cast expressions`, expr.operand);
      }
      const to = (expr.to as any)?.name?.toLowerCase() as string | undefined;
      if (CAST_TYPES.has(to!)) {
        return {
          evaluate: (tables) => {
            const value = operand.evaluate(tables);
            if (value == null) {
              return null;
            }
            return cast(value, to!);
          },
          getType() {
            return ExpressionType.fromTypeText(to as SqliteType);
          }
        } satisfies StaticRowValueClause;
      } else {
        return this.error(`CAST not supported for '${to}'`, expr);
      }
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

  getParameterRef(expr: Expr) {
    if (this.isParameterRef(expr)) {
      return `${expr.table!.name}.${expr.name}`;
    }
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
