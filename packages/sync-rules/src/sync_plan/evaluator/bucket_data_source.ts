import { BucketDataEvaluator, BucketDataSource, HydrationInput } from '../../BucketSource.js';
import { idFromData } from '../../cast.js';
import { ColumnDefinition } from '../../ExpressionType.js';
import { SourceTableRef } from '../../SourceTableRef.js';
import { TablePattern } from '../../TablePattern.js';
import {
  EvaluateRowOptions,
  SourceSchema,
  SqliteJsonRow,
  UnscopedEvaluatedRow,
  UnscopedEvaluationResult
} from '../../types.js';
import { filterJsonRow, isJsonValue, isValidParameterValue, JSONBucketNameSerialize } from '../../utils.js';
import {
  ScalarExpressionEngine,
  ScalarStatement,
  scalarStatementToSql,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';
import { SqlExpression } from '../expression.js';
import { ExpressionToSqlite } from '../expression_to_sql.js';
import * as plan from '../plan.js';
import { SyncPlanSchemaAnalyzer } from '../schema_inference.js';
import { StreamEvaluationContext } from './index.js';
import { TableProcessorToSqlHelper } from './table_processor_to_sql.js';

export class PreparedStreamBucketDataSource implements BucketDataSource {
  private readonly sourceTables = new Set<TablePattern>();
  private readonly pendingSources: PendingStreamDataSource[] = [];
  private readonly defaultSchema: string;

  constructor(
    readonly source: plan.StreamBucketDataSource,
    context: StreamEvaluationContext
  ) {
    this.defaultSchema = context.defaultSchema;

    for (const data of source.sources) {
      const pending = new PendingStreamDataSource(data, context.defaultSchema);

      this.pendingSources.push(pending);
      this.sourceTables.add(pending.tablePattern);
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

  tableSyncsData(table: SourceTableRef): boolean {
    for (const source of this.sourceTables) {
      if (source.matches(table)) return true;
    }

    return false;
  }

  createEvaluator(context: HydrationInput): BucketDataEvaluator {
    const sources = this.pendingSources.map((s) => ({
      pattern: s.tablePattern,
      evaluate: s.instantiate(context.scalarExpressions)
    }));

    return {
      evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[] {
        const results: UnscopedEvaluationResult[] = [];
        for (const { pattern, evaluate } of sources) {
          if (pattern.matches(options.sourceTable)) {
            evaluate(options, results);
          }
        }

        return results;
      }
    };
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    const analyzer = new SyncPlanSchemaAnalyzer(this.defaultSchema, schema);
    for (const source of this.source.sources) {
      analyzer.resolveResultSets(source, tables);
    }
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    for (const source of this.pendingSources) {
      const table = source.fixedOutputTableName;
      if (table != null) {
        const queries = (result[table] ??= []);
        queries.push({ query: source.debugSql });
      }
    }
  }
}

class PendingStreamDataSource {
  readonly tablePattern: TablePattern;
  private readonly outputs: ('star' | { index: number; alias: string })[] = [];
  private readonly numberOfOutputExpressions: number;
  private readonly numberOfParameters: number;
  private readonly evaluatorInputs: plan.ColumnSqlParameterValue[];
  private readonly statement: ScalarStatement;
  readonly fixedOutputTableName?: string;

  constructor(evaluator: plan.StreamDataSource, defaultSchema: string) {
    const translationHelper = new TableProcessorToSqlHelper(evaluator);
    const outputExpressions: SqlExpression<number | TableValuedFunctionOutput>[] = [];

    for (const column of evaluator.columns) {
      if (column === 'star') {
        this.outputs.push('star');
      } else {
        const expressionIndex = outputExpressions.length;
        outputExpressions.push(translationHelper.mapper.transform(column.expr));
        this.outputs.push({ index: expressionIndex, alias: column.alias });
      }
    }

    this.numberOfOutputExpressions = outputExpressions.length;
    for (const parameter of evaluator.parameters) {
      outputExpressions.push(translationHelper.mapper.transform(parameter.expr));
    }
    this.numberOfParameters = evaluator.parameters.length;

    this.statement = {
      outputs: outputExpressions,
      filters: translationHelper.filterExpressions,
      tableValuedFunctions: translationHelper.tableValuedFunctions
    };
    this.fixedOutputTableName = evaluator.outputTableName;
    this.tablePattern = evaluator.sourceTable.toTablePattern(defaultSchema);
    this.evaluatorInputs = translationHelper.mapper.instantiation;
  }

  get debugSql(): string {
    return scalarStatementToSql(this.statement);
  }

  instantiate(engine: ScalarExpressionEngine) {
    const evaluator = engine.prepareEvaluator(this.statement);

    return (options: EvaluateRowOptions, results: UnscopedEvaluationResult[]) => {
      try {
        const inputInstantiation = this.evaluatorInputs.map((input) => options.record[input.column]);
        row: for (const source of evaluator.evaluate(inputInstantiation)) {
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
    };
  }
}
