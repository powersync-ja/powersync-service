import { ParameterIndexLookupCreator } from '../../BucketSource.js';
import { ParameterLookupScope } from '../../HydrationState.js';
import { mapExternalDataToInstantiation, ScalarExpressionEvaluator } from './scalar_expression_evaluator.js';
import * as plan from '../plan.js';
import { StreamEvaluationContext } from './index.js';
import { TablePattern } from '../../TablePattern.js';
import { SourceTableInterface } from '../../SourceTableInterface.js';
import { SqliteJsonValue, SqliteRow, UnscopedEvaluatedParametersResult } from '../../types.js';
import { isValidParameterValueRow } from './parameter_evaluator.js';
import { UnscopedParameterLookup } from '../../BucketParameterQuerier.js';

export class PreparedParameterIndexLookupCreator implements ParameterIndexLookupCreator {
  readonly defaultLookupScope: ParameterLookupScope;
  private readonly evaluator: ScalarExpressionEvaluator;
  private readonly evaluatorInputs: plan.ColumnSqlParameterValue[];
  private readonly numberOfOutputs: number;
  private readonly numberOfParameters: number;

  constructor(
    private readonly source: plan.StreamParameterIndexLookupCreator,
    { engine }: StreamEvaluationContext
  ) {
    this.defaultLookupScope = source.defaultLookupScope;
    const mapExpressions = mapExternalDataToInstantiation<plan.ColumnSqlParameterValue>();
    const expressions = source.outputs.map((o) => mapExpressions.transform(o));

    this.numberOfOutputs = expressions.length;
    for (const parameter of source.parameters) {
      expressions.push(mapExpressions.transform(parameter.expr));
    }
    this.numberOfParameters = source.parameters.length;

    this.evaluator = engine.prepareEvaluator({
      outputs: expressions,
      filters: source.filters.map((f) => mapExpressions.transform(f))
    });
    this.evaluatorInputs = mapExpressions.instantiation;
  }

  getSourceTables(): Set<TablePattern> {
    const set = new Set<TablePattern>();
    set.add(this.source.sourceTable);
    return set;
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): UnscopedEvaluatedParametersResult[] {
    const results: UnscopedEvaluatedParametersResult[] = [];
    if (!this.source.sourceTable.matches(sourceTable)) {
      return results;
    }

    try {
      const inputInstantiation = this.evaluatorInputs.map((input) => row[input.column]);

      for (const outputRow of this.evaluator.evaluate(inputInstantiation)) {
        if (!isValidParameterValueRow(outputRow)) {
          continue;
        }

        const outputs: Record<string, SqliteJsonValue> = {};
        for (let i = 0; i < this.numberOfOutputs; i++) {
          outputs[i.toString()] = outputRow[i];
        }

        // source is [...outputs, ...partitionValues]
        const partitionValues = outputRow.splice(this.numberOfOutputs, this.numberOfParameters);
        const lookup = UnscopedParameterLookup.normalized(partitionValues);
        results.push({ lookup, bucketParameters: [outputs] });
      }
    } catch (e) {
      results.push({ error: e.message });
    }

    return results;
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    return this.source.sourceTable.matches(table);
  }
}
