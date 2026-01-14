import {
  BucketDataSource,
  buildBucketInfo,
  CompatibilityContext,
  EvaluatedParameters,
  EvaluatedRow,
  EvaluateRowOptions,
  EvaluationError,
  EvaluationResult,
  isEvaluatedRow,
  isEvaluationError,
  ParameterIndexLookupCreator,
  RowProcessor,
  SOURCE,
  SourceTableInterface,
  SqlEventDescriptor,
  SqliteInputValue,
  SqliteRow,
  SqliteValue,
  SqlSyncRules,
  TablePattern
} from '@powersync/service-sync-rules';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';
import { SourceTable } from '@powersync/service-core';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';

type EvaluateRowFn = (options: EvaluateRowOptions) => EvaluationResult[];

interface ResolvedDataSource {
  source: BucketDataSource;
  evaluate: EvaluateRowFn;
  id: number;
}
export class MergedSyncRules implements RowProcessor {
  static merge(sources: MongoPersistedSyncRules[]): MergedSyncRules {
    return new MergedSyncRules(sources);
  }

  private resolvedDataSources: Map<number, ResolvedDataSource>;
  private sourcePatterns: TablePattern[];
  private allSyncRules: SqlSyncRules[];

  constructor(sources: MongoPersistedSyncRules[]) {
    let resolvedDataSources = new Map<number, ResolvedDataSource>();
    let sourcePatternMap = new Map<string, TablePattern>();

    this.allSyncRules = [];
    for (let source of sources) {
      const syncRules = source.sync_rules;
      const mapping = source.mapping;
      const hydrationState = source.hydrationState;
      const dataSources = syncRules.bucketDataSources;
      this.allSyncRules.push(syncRules);
      for (let source of dataSources) {
        const scope = hydrationState.getBucketSourceScope(source);
        const id = mapping.bucketSourceId(source);
        if (resolvedDataSources.has(id)) {
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
        resolvedDataSources.set(id, { source, evaluate, id });
      }

      for (let pattern of syncRules.getSourceTables()) {
        const key = pattern.key;
        if (!sourcePatternMap.has(key)) {
          sourcePatternMap.set(key, pattern);
        }
      }
    }
    this.resolvedDataSources = resolvedDataSources;
    this.sourcePatterns = Array.from(sourcePatternMap.values());
  }

  getMatchingSources(table: SourceTableInterface): {
    bucketDataSources: BucketDataSource[];
    parameterIndexLookupCreators: ParameterIndexLookupCreator[];
  } {
    const bucketDataSources = [...this.resolvedDataSources.values()]
      .map((dataSource) => dataSource.source)
      .filter((ds) => ds.tableSyncsData(table));
    return {
      bucketDataSources: bucketDataSources,
      parameterIndexLookupCreators: [
        // FIXME: implement
      ]
    };
  }

  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  getSourceTables(): TablePattern[] {
    return this.sourcePatterns;
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
    // FIXME: This may be different per sync rules - need to handle that
    return this.allSyncRules[this.allSyncRules.length - 1].applyRowContext(source);
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
    // Important: We only get matching sources here, not all sources. This can help for two things:
    // 1. For performance: Skip any not-matching sources.
    // 2. For re-replication: We may take a snapshot when adding a new source, with a new SourceTable.
    //    In that case, we don't want to re-evaluate all existing sources, only the new one.

    // FIXME: Fix performance - don't scan all sources
    // And maybe re-use getMatchingSources?
    const table = options.sourceTable;
    if (!(table instanceof SourceTable)) {
      throw new ReplicationAssertionError(`Expected SourceTable instance`);
    }
    const bucketDataSources = [...this.resolvedDataSources.values()].filter((ds) =>
      table.bucketDataSourceIds.includes(ds.id)
    );

    const rawResults: EvaluationResult[] = bucketDataSources.flatMap((dataSource) => dataSource.evaluate(options));
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
