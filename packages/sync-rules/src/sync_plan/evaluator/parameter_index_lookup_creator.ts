import { UnscopedParameterLookup } from '../../BucketParameterQuerier.js';
import { HydrationInput, ParameterIndexLookupCreator, ParameterIndexLookupEvaluator } from '../../BucketSource.js';
import { ParameterLookupDefinitionId } from '../../HydrationState.js';
import { SourceTableRef } from '../../SourceTableRef.js';
import { TablePattern } from '../../TablePattern.js';
import { SqliteJsonValue, SqliteParameterValue, SqliteRow, UnscopedEvaluatedParametersResult } from '../../types.js';
import { ScalarStatement } from '../engine/scalar_expression_engine.js';
import * as plan from '../plan.js';
import { StreamEvaluationContext } from './index.js';
import { isValidParameterValueRow } from './parameter_evaluator.js';
import { resolveRowMetadata, TableProcessorToSqlHelper } from './table_processor_to_sql.js';

export class PreparedParameterIndexLookupCreator implements ParameterIndexLookupCreator {
  readonly sourceId: ParameterLookupDefinitionId;
  readonly sourceTable: TablePattern;
  private readonly evaluatorInputs: (plan.ColumnSqlParameterValue | plan.RowMetadataSqlValue)[];
  private readonly statement: ScalarStatement;
  private readonly numberOfOutputs: number;
  private readonly numberOfParameters: number;

  constructor(source: plan.StreamParameterIndexLookupCreator, { defaultSchema }: StreamEvaluationContext) {
    this.sourceId = {
      ...source.defaultLookupScope
    };
    const translationHelper = new TableProcessorToSqlHelper(source);
    const expressions = source.outputs.map((o) => translationHelper.mapper.transform(o));

    this.numberOfOutputs = expressions.length;
    for (const parameter of source.parameters) {
      expressions.push(translationHelper.mapper.transform(parameter.expr));
    }
    this.numberOfParameters = source.parameters.length;

    this.statement = {
      outputs: expressions,
      filters: translationHelper.filterExpressions,
      tableValuedFunctions: translationHelper.tableValuedFunctions
    };
    this.sourceTable = source.sourceTable.toTablePattern(defaultSchema);
    this.evaluatorInputs = translationHelper.mapper.instantiation;
  }

  getSourceTables(): Set<TablePattern> {
    const set = new Set<TablePattern>();
    set.add(this.sourceTable);
    return set;
  }

  createEvaluator({ scalarExpressions }: HydrationInput): ParameterIndexLookupEvaluator {
    const evaluator = scalarExpressions.prepareEvaluator(this.statement);

    return {
      evaluateParameterRow: (sourceTable: SourceTableRef, row: SqliteRow): UnscopedEvaluatedParametersResult[] => {
        const results: UnscopedEvaluatedParametersResult[] = [];
        if (!this.sourceTable.matches(sourceTable)) {
          return results;
        }

        interface PendingLookup {
          partitionValues: SqliteParameterValue[];
          bucketParameters: Record<string, SqliteJsonValue>[];
        }

        function parametersEqual(a: SqliteParameterValue[], b: SqliteParameterValue[]) {
          if (a.length != b.length) return false;

          return a.every((val, idx) => val === b[idx]);
        }

        try {
          const inputInstantiation = this.evaluatorInputs.map((input) =>
            'column' in input ? row[input.column] : resolveRowMetadata(input, this.sourceTable, sourceTable)
          );

          // In almost all cases, lookups generate at most one output with exactly one bucket parameter. This is the case
          // for all lookups without a table-valued function, e.g. `SELECT foo.* FROM foo, bar ON foo.x = bar.x AND
          // bar.y = auth.user_id()` which generates a lookup from `bar.y` to `bar.x`. However, it's also possible for:
          //
          //  - a single row with one partition value to generate multiple outputs, which would yield a lookup with multiple
          //    entries in bucketParameters.
          //  - a single row to generate multiple partition values, which would yield multiple lookup entries.
          //  - a combination of those two, if multiple table-valued functions are used in the parameter result set.
          const lookups: PendingLookup[] = [];

          function resolveLookup(entries: SqliteParameterValue[]) {
            // The lookups array could be a hash map, but since we don't expect rows to generate many lookups, a hash map
            // would likely be more expensive than this linear search.
            for (const existing of lookups) {
              if (parametersEqual(existing.partitionValues, entries)) {
                return existing;
              }
            }

            const created: PendingLookup = { partitionValues: entries, bucketParameters: [] };
            lookups.push(created);
            return created;
          }

          for (const outputRow of evaluator.evaluate(inputInstantiation)) {
            if (!isValidParameterValueRow(outputRow)) {
              continue;
            }

            const outputs: Record<string, SqliteJsonValue> = {};
            for (let i = 0; i < this.numberOfOutputs; i++) {
              outputs[i.toString()] = outputRow[i];
            }

            // source is [...outputs, ...partitionValues]
            const partitionValues = outputRow.splice(this.numberOfOutputs, this.numberOfParameters);
            const pendingLookup = resolveLookup(partitionValues);
            pendingLookup.bucketParameters.push(outputs);
          }

          for (const { partitionValues, bucketParameters } of lookups) {
            results.push({ lookup: UnscopedParameterLookup.normalized(partitionValues), bucketParameters });
          }
        } catch (e) {
          results.push({ error: e.message });
        }

        return results;
      }
    };
  }

  tableSyncsParameters(table: SourceTableRef): boolean {
    return this.sourceTable.matches(table);
  }
}
