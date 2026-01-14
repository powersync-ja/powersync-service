import { BucketDataSource } from '../BucketSource.js';
import { ColumnDefinition } from '../ExpressionType.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { SqlSyncRules } from '../SqlSyncRules.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluateRowOptions,
  idFromData,
  SourceSchema,
  SqliteJsonRow,
  UnscopedEvaluatedRow,
  UnscopedEvaluationResult
} from '../types.js';
import { isJsonValue, JSONBucketNameSerialize } from '../utils.js';
import * as plan from './plan.js';
import { prepareRowEvaluator, RowEvaluator, SqlEngine } from './sql_engine.js';

export interface StreamEvaluationContext {
  engine: SqlEngine;
}

export function addPrecompiledSyncPlanToRules(
  plan: plan.SyncPlan,
  rules: SqlSyncRules,
  context: StreamEvaluationContext
) {}

class PreparedStreamDataSource {
  private readonly outputs: ('star' | { index: number; alias: string })[] = [];
  private readonly evaluator: RowEvaluator;

  constructor(evaluator: plan.StreamDataSource, { engine }: StreamEvaluationContext) {
    const outputExpressions: plan.SqlExpression<plan.ColumnSqlParameterValue>[] = [];
    for (const column of evaluator.columns) {
      if (column === 'star') {
        this.outputs.push('star');
      } else {
        const expressionIndex = outputExpressions.length;
        outputExpressions.push(column.expr);
        this.outputs.push({ index: expressionIndex, alias: column.alias ?? column.expr.sql });
      }
    }

    this.evaluator = prepareRowEvaluator(engine, [], evaluator.filters, evaluator.parameters);
  }

  evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[] {
    try {
      const outputs: UnscopedEvaluationResult[] = [];
      row: for (const source of this.evaluator.evaluate(options.record)) {
        const record: SqliteJsonRow = {};
        for (const output of this.outputs) {
          if (output === 'star') {
            Object.assign(record, options.record);
          } else {
            const value = source.outputs[output.index];
            if (isJsonValue(value)) {
              record[output.alias] = value;
            }
          }
        }
        const id = idFromData(record);

        for (const bucketParameter of source.partitionValues) {
          if (!isJsonValue(bucketParameter)) {
            outputs.push({
              error: `While evaluating ${options.sourceTable.name}, id ${id}. Parameter is not JSON serializable.`
            });
            continue row;
          }
        }

        outputs.push({
          id,
          data: record,
          // TODO: Properly infer table name
          table: options.sourceTable.name,
          serializedBucketParameters: JSONBucketNameSerialize.stringify(source.partitionValues)
        } satisfies UnscopedEvaluatedRow);
      }

      return outputs;
    } catch (e) {
      return [{ error: e.message }];
    }
  }
}

class StreamBucketDataSource implements BucketDataSource {
  private readonly sourceTables = new Set<TablePattern>();

  constructor(readonly source: plan.StreamBucketDataSource) {}

  get uniqueName(): string {
    return this.source.uniqueName;
  }

  get bucketParameters(): string[] {
    // We can pick an arbitrary evaluator within the source, since they're all guaranteed to have the same parameters.
    const evaluator = this.source.sources[0];

    // It doesn't matter what we return here because it's for debugging purposes only. We at least want to get the
    // amount of array elements right.
    return evaluator.parameters.map((p) => p.expr.sql);
  }

  getSourceTables(): Set<TablePattern> {
    throw new Error('Method not implemented.');
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    throw new Error('Method not implemented.');
  }

  evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[] {
    throw new Error('Method not implemented.');
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    throw new Error('Method not implemented.');
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    throw new Error('Method not implemented.');
  }
}
