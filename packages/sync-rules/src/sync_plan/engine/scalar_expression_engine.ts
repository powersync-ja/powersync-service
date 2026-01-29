import { SqliteValue } from '../../types.js';
import { ExternalData, SqlExpression } from '../expression.js';
import { ExpressionToSqlite, Precedence } from '../expression_to_sql.js';
import { MapSourceVisitor, visitExpr } from '../expression_visitor.js';
import { ColumnSqlParameterValue, RequestSqlParameterValue, SqlParameterValue } from '../plan.js';

/**
 * Description of a scalar SQL statement (without external dependencies on tables).
 *
 * This corresponds to the SQL statement `SELECT $outputs FROM $tableValuedFunctions WHERE $filters`.
 *
 * Each output and filter expression can reference:
 *
 *   1. An external parameter, passed in {@link ScalarExpressionEvaluator.evaluate}. The number corresponds to the index
 *      of the parameter to use.
 *   2. An output column of a table-valued function added to the statement.
 */
export interface ScalarStatement {
  outputs?: SqlExpression<number | TableValuedFunctionOutput>[];
  filters?: SqlExpression<number | TableValuedFunctionOutput>[];
  tableValuedFunctions?: TableValuedFunction[];
}

export interface TableValuedFunction {
  name: string;
  inputs: SqlExpression<number>[];
}

export interface TableValuedFunctionOutput {
  function: TableValuedFunction;
  column: string;
}

export interface ScalarExpressionEngine {
  prepareEvaluator(statement: ScalarStatement): ScalarExpressionEvaluator;

  /**
   * Disposes all evaluators and closes this engine.
   */
  close(): void;
}

export interface ScalarExpressionEvaluator {
  evaluate(inputs: SqliteValue[]): SqliteValue[][];
}

export function scalarStatementToSql({ filters = [], outputs = [], tableValuedFunctions = [] }: ScalarStatement) {
  const tableValuedFunctionNames = new Map<TableValuedFunction, string>();
  for (const fn of tableValuedFunctions) {
    tableValuedFunctionNames.set(fn, `tbl_${tableValuedFunctionNames.size}`);
  }

  const toSqlite = new StatementToSqlite(tableValuedFunctionNames);
  toSqlite.addLexeme('SELECT');

  if (outputs.length === 0) {
    // We need to add a bogus expression to avoid a syntax error (`SELECT WHERE ...` alone is invalid).
    toSqlite.addLexeme('1');
  } else {
    outputs.forEach((expr, i) => {
      if (i != 0) toSqlite.comma();
      visitExpr(toSqlite, expr, null);
    });
  }

  if (tableValuedFunctionNames.size != 0) {
    toSqlite.addLexeme('FROM');
    let first = true;

    tableValuedFunctionNames.forEach((name, fn) => {
      if (!first) {
        toSqlite.comma();
      }

      visitExpr(toSqlite, { type: 'function', function: fn.name, parameters: fn.inputs }, null);
      toSqlite.addLexeme('AS');
      toSqlite.identifier(name);

      first = false;
    });
  }

  if (filters.length != 0) {
    toSqlite.addLexeme('WHERE');
    filters.forEach((expr, i) => {
      if (i != 0) toSqlite.addLexeme('AND');

      visitExpr(toSqlite, expr, Precedence.and);
    });
  }

  return toSqlite.sql;
}

class StatementToSqlite extends ExpressionToSqlite<number | TableValuedFunctionOutput> {
  constructor(private readonly tableValuedFunctionNames: Map<TableValuedFunction, string>) {
    super();
  }

  comma() {
    this.addLexeme(',', { spaceLeft: false });
  }

  visitExternalData(expr: ExternalData<number | TableValuedFunctionOutput>): void {
    if (typeof expr.source === 'number') {
      this.addLexeme(`?${expr.source}`);
    } else {
      const fn = this.tableValuedFunctionNames.get(expr.source.function)!;
      this.identifier(fn);
      this.addLexeme('.', { spaceLeft: false, spaceRight: false });
      this.identifier(expr.source.column);
    }
  }
}

/**
 * Utility to transform multiple expressions embedding parameter values into expressions that reference a parameter
 * index representing that value.
 *
 * Parameter values used multiple times are de-duplicated into the same SQL parameter.
 */
export function mapExternalDataToInstantiation<T extends SqlParameterValue>() {
  const instantiation: T[] = [];
  const columnsToIndex = new Map<ColumnSqlParameterValue, number>();
  const requestToIndex = new Map<RequestSqlParameterValue, number>();

  const visitor = new MapSourceVisitor<T, number>((data) => {
    const indexIfAdded = instantiation.length + 1;
    if ('column' in data) {
      if (columnsToIndex.has(data)) {
        return columnsToIndex.get(data)!;
      }

      columnsToIndex.set(data, indexIfAdded);
    } else {
      if (requestToIndex.has(data)) {
        return requestToIndex.get(data)!;
      }

      requestToIndex.set(data, indexIfAdded);
    }

    instantiation.push(data);
    return indexIfAdded;
  });

  return {
    instantiation,
    transform(expr: SqlExpression<SqlParameterValue>) {
      return visitExpr(visitor, expr, null);
    }
  };
}
