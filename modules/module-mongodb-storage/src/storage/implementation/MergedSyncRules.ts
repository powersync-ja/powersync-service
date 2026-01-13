import {
  buildBucketInfo,
  CompatibilityContext,
  EvaluatedParameters,
  EvaluatedRow,
  EvaluateRowOptions,
  EvaluationError,
  EvaluationResult,
  HydrationState,
  isEvaluatedRow,
  isEvaluationError,
  mergeDataSources,
  RowProcessor,
  SOURCE,
  SourceTableInterface,
  SqlEventDescriptor,
  SqliteInputValue,
  SqliteRow,
  SqliteValue,
  TablePattern
} from '@powersync/service-sync-rules';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';

type EvaluateRowFn = (options: EvaluateRowOptions) => EvaluationResult[];

export class MergedSyncRules implements RowProcessor {
  static merge(sources: MongoPersistedSyncRules[]): MergedSyncRules {
    let evaluators = new Map<number, EvaluateRowFn>();

    for (let source of sources) {
      const syncRules = source.sync_rules;
      const mapping = source.mapping;
      const hydrationState = source.hydrationState;
      const dataSources = syncRules.bucketDataSources;
      for (let source of dataSources) {
        const scope = hydrationState.getBucketSourceScope(source);
        const id = mapping.bucketSourceId(source);
        if (evaluators.has(id)) {
          continue;
        }

        const evaluate: EvaluateRowFn = (options: EvaluateRowOptions): EvaluationResult[] => {
          return source.evaluateRow(options).map((result) => {
            if (isEvaluationError(result)) {
              return result;
            }
            const info = buildBucketInfo(scope, result.serializedBucketParameters);
            return {
              bucket: info.bucket,
              id: result.id,
              table: result.table,
              data: result.data,
              source: info[SOURCE]
            } satisfies EvaluatedRow;
          });
        };
        evaluators.set(id, evaluate);
      }
    }

    return new MergedSyncRules(Array.from(evaluators.values()));
  }

  constructor(private evaluators: EvaluateRowFn[]) {}
  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  getSourceTables(): TablePattern[] {
    throw new Error('Method not implemented.');
  }
  tableTriggersEvent(table: SourceTableInterface): boolean {
    throw new Error('Method not implemented.');
  }
  tableSyncsData(table: SourceTableInterface): boolean {
    throw new Error('Method not implemented.');
  }
  tableSyncsParameters(table: SourceTableInterface): boolean {
    throw new Error('Method not implemented.');
  }
  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    throw new Error('Method not implemented.');
  }

  /**
   * Throws errors.
   */
  evaluateRow(options: EvaluateRowOptions): EvaluatedRow[] {
    const { results, errors } = this.evaluateRowWithErrors(options);
    if (errors.length > 0) {
      throw new Error(errors[0].error);
    }
    return results;
  }

  evaluateRowWithErrors(options: EvaluateRowOptions): { results: EvaluatedRow[]; errors: EvaluationError[] } {
    const rawResults: EvaluationResult[] = this.evaluators.flatMap((evaluator) => evaluator(options));
    const results = rawResults.filter(isEvaluatedRow) as EvaluatedRow[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];

    return { results, errors };
  }

  evaluateParameterRow(table: SourceTableInterface, row: SqliteRow): EvaluatedParameters[] {
    throw new Error('Method not implemented.');
  }
  evaluateParameterRowWithErrors(
    table: SourceTableInterface,
    row: SqliteRow
  ): { results: EvaluatedParameters[]; errors: EvaluationError[] } {
    throw new Error('Method not implemented.');
  }
}
