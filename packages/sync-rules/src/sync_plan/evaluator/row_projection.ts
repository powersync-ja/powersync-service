import { TablePattern } from '../../TablePattern.js';
import { EvaluateRowOptions, SqliteJsonRow, SqliteValue } from '../../types.js';
import { filterJsonRow, isJsonValue } from '../../utils.js';
import {
  ScalarExpressionEngine,
  ScalarStatement,
  scalarStatementToSql,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';
import { SqlExpression } from '../expression.js';
import * as plan from '../plan.js';
import { resolveRowMetadata, TableProcessorToSqlHelper } from './table_processor_to_sql.js';

export interface EvaluatedRowProjection {
  data: SqliteJsonRow;
  partitionValues: SqliteValue[];
}

/**
 * Prepares the shared projection/filter portion of bucket and event row evaluators.
 */
export class PendingRowProjection {
  readonly tablePattern: TablePattern;
  private readonly outputs: ('star' | { index: number; alias: string })[] = [];
  private readonly numberOfOutputExpressions: number;
  private readonly numberOfParameters: number;
  private readonly evaluatorInputs: (plan.ColumnSqlParameterValue | plan.RowMetadataSqlValue)[];
  private readonly statement: ScalarStatement;

  constructor(evaluator: plan.RowProjection, defaultSchema: string) {
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
    this.tablePattern = evaluator.sourceTable.toTablePattern(defaultSchema);
    this.evaluatorInputs = translationHelper.mapper.instantiation;
  }

  get debugSql(): string {
    return scalarStatementToSql(this.statement);
  }

  instantiate(engine: ScalarExpressionEngine): (options: EvaluateRowOptions) => EvaluatedRowProjection[] {
    const evaluator = engine.prepareEvaluator(this.statement);
    const pattern = this.tablePattern;

    return (options) => {
      const inputInstantiation = this.evaluatorInputs.map((input) =>
        'column' in input ? options.record[input.column] : resolveRowMetadata(input, pattern, options.sourceTable)
      );
      const results: EvaluatedRowProjection[] = [];

      for (const source of evaluator.evaluate(inputInstantiation)) {
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

        results.push({
          data: record,
          partitionValues: source.slice(
            this.numberOfOutputExpressions,
            this.numberOfOutputExpressions + this.numberOfParameters
          )
        });
      }

      return results;
    };
  }
}
