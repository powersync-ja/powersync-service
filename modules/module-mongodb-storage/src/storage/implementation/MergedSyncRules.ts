import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { SourceTable } from '@powersync/service-core';
import {
  BucketDataSource,
  CompatibilityContext,
  EvaluatedParameters,
  EvaluatedParametersResult,
  EvaluatedRow,
  EvaluateRowOptions,
  EvaluationError,
  EvaluationResult,
  hydrateEvaluateParameterRow,
  hydrateEvaluateRow,
  isEvaluatedParameters,
  isEvaluatedRow,
  isEvaluationError,
  ParameterIndexLookupCreator,
  RowProcessor,
  SourceTableInterface,
  SqlEventDescriptor,
  SqliteInputValue,
  SqliteRow,
  SqliteValue,
  SqlSyncRules,
  TablePattern
} from '@powersync/service-sync-rules';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';

type EvaluateRowFn = (options: EvaluateRowOptions) => EvaluationResult[];
type EvaluateParameterRowFn = (sourceTable: SourceTableInterface, row: SqliteRow) => EvaluatedParametersResult[];

interface ResolvedDataSource {
  source: BucketDataSource;
  evaluate: EvaluateRowFn;
  id: number;
}
interface ResolvedParameterLookupSource {
  source: ParameterIndexLookupCreator;
  id: number;
  evaluate: EvaluateParameterRowFn;
}

/**
 * This is like HydratedSyncRules, but merges multiple sources together, and only implements the methods
 * required for replication.
 *
 * This should be moved to a re-usable location, possibly merged with HydratedSyncRules logic.
 */
export class MergedSyncRules implements RowProcessor {
  static merge(sources: MongoPersistedSyncRules[]): MergedSyncRules {
    return new MergedSyncRules(sources);
  }

  private resolvedDataSources: Map<number, ResolvedDataSource>;
  private resolvedParameterLookupSources: Map<number, ResolvedParameterLookupSource>;
  private sourcePatterns: TablePattern[];
  private allSyncRules: SqlSyncRules[];

  constructor(sources: MongoPersistedSyncRules[]) {
    let resolvedDataSources = new Map<number, ResolvedDataSource>();
    let resolvedParameterLookupSources = new Map<number, ResolvedParameterLookupSource>();
    let sourcePatternMap = new Map<string, TablePattern>();

    this.allSyncRules = [];
    for (let source of sources) {
      const syncRules = source.sync_rules;
      const mapping = source.mapping;
      const hydrationState = source.hydrationState;
      const dataSources = syncRules.bucketDataSources;
      const bucketParameterLookupSources = syncRules.bucketParameterLookupSources;
      this.allSyncRules.push(syncRules);
      for (let source of dataSources) {
        const id = mapping.bucketSourceId(source);
        if (resolvedDataSources.has(id)) {
          continue;
        }
        const evaluate = hydrateEvaluateRow(hydrationState, source);
        resolvedDataSources.set(id, { source, evaluate, id });
      }

      for (let pattern of syncRules.getSourceTables()) {
        const key = pattern.key;
        if (!sourcePatternMap.has(key)) {
          sourcePatternMap.set(key, pattern);
        }
      }

      for (let source of bucketParameterLookupSources) {
        const id = mapping.parameterLookupId(source);
        if (resolvedParameterLookupSources.has(id)) {
          continue;
        }

        const withScope = hydrateEvaluateParameterRow(hydrationState, source);
        resolvedParameterLookupSources.set(id, { source, id, evaluate: withScope });
      }
    }
    this.resolvedDataSources = resolvedDataSources;
    this.resolvedParameterLookupSources = resolvedParameterLookupSources;
    this.sourcePatterns = Array.from(sourcePatternMap.values());
  }

  /**
   *
   * @param pattern The source database table definition, _not_ the individually derived SourceTables.
   * @returns
   */
  getMatchingSources(pattern: TablePattern): {
    bucketDataSources: BucketDataSource[];
    parameterIndexLookupCreators: ParameterIndexLookupCreator[];
  } {
    // FIXME: Fix performance - don't scan all sources
    const bucketDataSources = [...this.resolvedDataSources.values()]
      .map((dataSource) => dataSource.source)
      .filter((ds) => ds.getSourceTables().some((table) => table.equals(pattern)));

    const parameterIndexLookupCreators: ParameterIndexLookupCreator[] = [
      ...this.resolvedParameterLookupSources.values()
    ]
      .map((dataSource) => dataSource.source)
      .filter((ds) => ds.getSourceTables().some((table) => table.equals(pattern)));
    return {
      bucketDataSources,
      parameterIndexLookupCreators
    };
  }

  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  getSourceTables(): TablePattern[] {
    return this.sourcePatterns;
  }

  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    // FIXME: This may be different per sync rules - need to handle that
    return this.allSyncRules[this.allSyncRules.length - 1].applyRowContext(source);
  }

  evaluateRowWithErrors(options: EvaluateRowOptions): { results: EvaluatedRow[]; errors: EvaluationError[] } {
    // Important: We only get matching sources here, not all sources. This can help for two things:
    // 1. For performance: Skip any not-matching sources.
    // 2. For re-replication: We may take a snapshot when adding a new source, with a new SourceTable.
    //    In that case, we don't want to re-evaluate all existing sources, only the new one.

    // FIXME: Fix performance - don't scan all sources
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

  evaluateParameterRowWithErrors(
    table: SourceTableInterface,
    row: SqliteRow
  ): { results: EvaluatedParameters[]; errors: EvaluationError[] } {
    // FIXME: Fix performance - don't scan all sources

    if (!(table instanceof SourceTable)) {
      throw new ReplicationAssertionError(`Expected SourceTable instance`);
    }
    const parameterIndexLookupCreators = [...this.resolvedParameterLookupSources.values()].filter((ds) =>
      table.parameterLookupSourceIds.includes(ds.id)
    );
    const rawResults: EvaluatedParametersResult[] = parameterIndexLookupCreators.flatMap((creator) =>
      creator.evaluate(table, row)
    );
    const results = rawResults.filter(isEvaluatedParameters) as EvaluatedParameters[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];
    return { results, errors };
  }
}
