import { BucketDataSource } from '../../BucketSource.js';
import { ColumnDefinition } from '../../ExpressionType.js';
import { SourceTableInterface } from '../../SourceTableInterface.js';
import { TablePattern } from '../../TablePattern.js';
import {
  EvaluateRowOptions,
  SourceSchema,
  SqliteJsonRow,
  UnscopedEvaluatedRow,
  UnscopedEvaluationResult
} from '../../types.js';
import { filterJsonRow, idFromData, isJsonValue, isValidParameterValue, JSONBucketNameSerialize } from '../../utils.js';
import { SqlExpression } from '../expression.js';
import { ExpressionToSqlite } from '../expression_to_sql.js';
import * as plan from '../plan.js';
import { StreamEvaluationContext } from './index.js';
import {
  mapExternalDataToInstantiation,
  ScalarExpressionEvaluator,
  scalarStatementToSql
} from '../engine/scalar_expression_engine.js';

export class PreparedStreamBucketDataSource implements BucketDataSource {
  private readonly sourceTables = new Set<TablePattern>();
  private readonly sources: PreparedStreamDataSource[] = [];

  constructor(
    readonly source: plan.StreamBucketDataSource,
    context: StreamEvaluationContext
  ) {
    for (const data of source.sources) {
      const prepared = new PreparedStreamDataSource(data, context);

      this.sources.push(prepared);
      this.sourceTables.add(prepared.tablePattern);
    }
  }

  get uniqueName(): string {
    return this.source.uniqueName;
  }

  get bucketParameters(): string[] {
    // We can pick an arbitrary evaluator within the source, since they're all guaranteed to have the same parameters.
    const evaluator = this.source.sources[0];

    // It doesn't matter what we return here because it's for debugging purposes only.
    return evaluator.parameters.map((p) => ExpressionToSqlite.toSqlite(p.expr));
  }

  getSourceTables(): Set<TablePattern> {
    return this.sourceTables;
  }

  private *sourcesForTable(table: SourceTableInterface) {
    for (const source of this.sources) {
      if (source.tablePattern.matches(table)) {
        yield source;
      }
    }
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return !this.sourcesForTable(table).next().done;
  }

  evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[] {
    const results: UnscopedEvaluationResult[] = [];
    for (const source of this.sourcesForTable(options.sourceTable)) {
      source.evaluateRow(options, results);
    }

    return results;
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    throw new Error('resolveResultSets not implemented.');
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    for (const source of this.sources) {
      const table = source.fixedOutputTableName;
      if (table != null) {
        const queries = (result[table] ??= []);
        queries.push({ query: source.debugSql });
      }
    }
  }
}

class PreparedStreamDataSource {
  readonly tablePattern: TablePattern;
  private readonly outputs: ('star' | { index: number; alias: string })[] = [];
  private readonly numberOfOutputExpressions: number;
  private readonly numberOfParameters: number;
  private readonly evaluator: ScalarExpressionEvaluator;
  private readonly evaluatorInputs: plan.ColumnSqlParameterValue[];
  readonly fixedOutputTableName?: string;
  readonly debugSql: string;

  constructor(evaluator: plan.StreamDataSource, { engine }: StreamEvaluationContext) {
    const mapExpressions = mapExternalDataToInstantiation<plan.ColumnSqlParameterValue>();
    const outputExpressions: SqlExpression<number>[] = [];
    for (const column of evaluator.columns) {
      if (column === 'star') {
        this.outputs.push('star');
      } else {
        const expressionIndex = outputExpressions.length;
        outputExpressions.push(mapExpressions.transform(column.expr));
        this.outputs.push({ index: expressionIndex, alias: column.alias });
      }
    }

    this.numberOfOutputExpressions = outputExpressions.length;
    for (const parameter of evaluator.parameters) {
      outputExpressions.push(mapExpressions.transform(parameter.expr));
    }
    this.numberOfParameters = evaluator.parameters.length;

    const evaluatorOptions = {
      outputs: outputExpressions,
      filters: evaluator.filters.map((f) => mapExpressions.transform(f))
    };
    this.debugSql = scalarStatementToSql(evaluatorOptions);
    this.evaluator = engine.prepareEvaluator(evaluatorOptions);
    this.fixedOutputTableName = evaluator.outputTableName;
    this.tablePattern = evaluator.sourceTable;
    this.evaluatorInputs = mapExpressions.instantiation;
  }

  evaluateRow(options: EvaluateRowOptions, results: UnscopedEvaluationResult[]) {
    try {
      const inputInstantiation = this.evaluatorInputs.map((input) => options.record[input.column]);
      row: for (const source of this.evaluator.evaluate(inputInstantiation)) {
        const record: SqliteJsonRow = {};
        for (const output of this.outputs) {
          if (output === 'star') {
            Object.assign(record, filterJsonRow(options.record));
          } else {
            const value = source[output.index];
            if (isJsonValue(value)) {
              record[output.alias] = value;
            }
          }
        }
        const id = idFromData(record);
        // source is [...outputs, ...partitionValues]
        const partitionValues = source.splice(this.numberOfOutputExpressions, this.numberOfParameters);

        for (const bucketParameter of partitionValues) {
          if (!isValidParameterValue(bucketParameter)) {
            continue row;
          }
        }

        results.push({
          id,
          data: record,
          table: this.fixedOutputTableName ?? options.sourceTable.name,
          serializedBucketParameters: JSONBucketNameSerialize.stringify(partitionValues)
        } satisfies UnscopedEvaluatedRow);
      }

      return results;
    } catch (e) {
      return results.push({ error: e.message });
    }
  }
}
