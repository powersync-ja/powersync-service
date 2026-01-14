import { SqliteRow, SqliteValue } from '../types.js';
import {
  ColumnSqlParameterValue,
  isColumnSqlParameterValue,
  PartitionKey,
  SqlExpression,
  SqlParameterValue
} from './plan.js';

export interface SqlEngine {
  prepare(stmt: string): PreparedStatement;
}

export interface PreparedStatement {
  evaluateScalar(params: SqliteValue[]): SqliteValue[] | undefined;
}

export function nodeSqlEngine(module: typeof import('node:sqlite')): SqlEngine {
  // Imported dynamically so that we can keep running sync-rules in browser contexts.
  const db = new module.DatabaseSync(':memory:', { readOnly: true, readBigInts: true, returnArrays: true } as any);

  return {
    prepare(stmt): PreparedStatement {
      const prepared = db.prepare(stmt);

      return {
        evaluateScalar(params) {
          const rows = prepared.get(...params) as unknown as SqliteValue[][];
          return rows[0];
        }
      };
    }
  };
}

export class SqlBuilder {
  sql: string;
  readonly values: SqlParameterValue[] = [];

  constructor(sql: string) {
    this.sql = sql;
  }

  addExpression(expression: SqlExpression<SqlParameterValue>) {
    // TODO: De-duplicate values across multiple expressions added to this statement.
    this.sql += expression.sql;
    this.values.push(...expression.values);
  }

  addExpressions(expressions: SqlExpression<SqlParameterValue>[]) {
    expressions.forEach((expr, i) => {
      if (i != 0) {
        this.sql += ', ';
      }

      this.addExpression(expr);
    });
  }
}

export function evaluateParameterValueOnRow(row: SqliteRow, value: SqlParameterValue) {
  if (isColumnSqlParameterValue(value)) {
    return row[value.column];
  } else {
    throw new Error('Not a column parameter value');
  }
}

export function prepareRowEvaluator(
  engine: SqlEngine,
  outputs: SqlExpression<ColumnSqlParameterValue>[],
  filters: SqlExpression<ColumnSqlParameterValue>[],
  parameters: PartitionKey[]
): RowEvaluator {
  const builder = new SqlBuilder('SELECT ');
  builder.addExpressions(outputs);
  builder.addExpressions(parameters.map((e) => e.expr));

  if (filters.length) {
    builder.sql += ' WHERE ';
    builder.addExpressions(filters);
  }

  const stmt = engine.prepare(builder.sql);
  return {
    evaluate(input: SqliteRow): TableProcessorResult[] {
      const mappedInputs: SqliteValue[] = [];
      for (const param of builder.values) {
        mappedInputs.push(evaluateParameterValueOnRow(input, param));
      }

      const columnValues = stmt.evaluateScalar(mappedInputs);
      if (columnValues == null) {
        return [];
      }

      const outputValues = columnValues.splice(0, outputs.length);
      return [
        {
          outputs: outputValues,
          partitionValues: columnValues
        }
      ];
    }
  };
}

export interface RowEvaluator {
  evaluate(input: SqliteRow): TableProcessorResult[];
}

export interface TableProcessorResult {
  partitionValues: SqliteValue[];
  outputs: SqliteValue[];
}
