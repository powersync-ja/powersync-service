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
  TableDataSources,
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

  // keyed by TablePattern.key
  private tableDataSources: Map<string, TableDataSources> = new Map();

  private allSyncRules: SqlSyncRules[];

  // all table patterns
  private sourcePatterns: TablePattern[];
  // sourcePatterns, non-wildcard, keyed by patternKey()
  private indexedPatterns: Map<string, TablePattern[]> = new Map();
  // all wildcard patterns
  private wildcardPatterns: TablePattern[] = [];

  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

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

        for (let pattern of source.getSourceTables()) {
          if (!this.tableDataSources.has(pattern.key)) {
            this.tableDataSources.set(pattern.key, { bucketDataSources: [], parameterIndexLookupCreators: [] });
          }
          this.tableDataSources.get(pattern.key)!.bucketDataSources.push(source);
        }
      }

      for (let source of bucketParameterLookupSources) {
        const id = mapping.parameterLookupId(source);
        if (resolvedParameterLookupSources.has(id)) {
          continue;
        }

        const withScope = hydrateEvaluateParameterRow(hydrationState, source);
        resolvedParameterLookupSources.set(id, { source, id, evaluate: withScope });

        for (let pattern of source.getSourceTables()) {
          if (!this.tableDataSources.has(pattern.key)) {
            this.tableDataSources.set(pattern.key, { bucketDataSources: [], parameterIndexLookupCreators: [] });
          }
          this.tableDataSources.get(pattern.key)!.parameterIndexLookupCreators.push(source);
        }
      }

      for (let pattern of syncRules.getSourceTables()) {
        const key = pattern.key;
        if (!sourcePatternMap.has(key)) {
          sourcePatternMap.set(key, pattern);
        }
      }
    }
    this.resolvedDataSources = resolvedDataSources;
    this.resolvedParameterLookupSources = resolvedParameterLookupSources;
    this.sourcePatterns = Array.from(sourcePatternMap.values());

    for (let pattern of this.sourcePatterns) {
      if (pattern.isWildcard) {
        this.wildcardPatterns.push(pattern);
      } else {
        const key = patternKey(pattern);
        if (!this.indexedPatterns.has(key)) {
          this.indexedPatterns.set(key, []);
        }
        this.indexedPatterns.get(key)!.push(pattern);
      }
    }
  }

  /**
   *
   * @param pattern The source database table definition, _not_ the individually derived SourceTables.
   * @returns
   */
  getMatchingSources(pattern: TablePattern): TableDataSources {
    return this.tableDataSources.get(pattern.key) ?? { bucketDataSources: [], parameterIndexLookupCreators: [] };
  }

  getSourceTables(): TablePattern[] {
    return this.sourcePatterns;
  }

  getMatchingTablePatterns(table: SourceTableInterface): TablePattern[] {
    // Equivalent to:
    // return this.sourcePatterns.filter((pattern) => pattern.matches(table));
    const tables = this.indexedPatterns.get(patternKey(table)) ?? [];
    if (this.wildcardPatterns.length === 0) {
      // Fast path - no wildcards
      return tables;
    } else {
      const matchedPatterns = this.wildcardPatterns.filter((pattern) => pattern.matches(table));
      return [...tables, ...matchedPatterns];
    }
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

    const table = options.sourceTable;
    // FIXME: Fix API to not require this type assertion
    if (!(table instanceof SourceTable)) {
      throw new ReplicationAssertionError(`Expected SourceTable instance`);
    }
    const bucketDataSources: ResolvedDataSource[] = [];
    for (let sourceId of table.bucketDataSourceIds) {
      const ds = this.resolvedDataSources.get(sourceId);
      if (ds) {
        bucketDataSources.push(ds);
      }
    }

    const rawResults: EvaluationResult[] = bucketDataSources.flatMap((dataSource) => dataSource.evaluate(options));
    const results = rawResults.filter(isEvaluatedRow) as EvaluatedRow[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];

    return { results, errors };
  }

  evaluateParameterRowWithErrors(
    table: SourceTableInterface,
    row: SqliteRow
  ): { results: EvaluatedParameters[]; errors: EvaluationError[] } {
    // FIXME: Fix API to not require this type assertion
    if (!(table instanceof SourceTable)) {
      throw new ReplicationAssertionError(`Expected SourceTable instance`);
    }
    let parameterIndexLookupCreators: ResolvedParameterLookupSource[] = [];
    for (let sourceId of table.parameterLookupSourceIds) {
      const ds = this.resolvedParameterLookupSources.get(sourceId);
      if (ds) {
        parameterIndexLookupCreators.push(ds);
      }
    }
    const rawResults: EvaluatedParametersResult[] = parameterIndexLookupCreators.flatMap((creator) =>
      creator.evaluate(table, row)
    );
    const results = rawResults.filter(isEvaluatedParameters) as EvaluatedParameters[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];
    return { results, errors };
  }
}

/**
 * Key for a pattern or source table.
 *
 * Does not support wildcard patterns.
 */
function patternKey(pattern: TablePattern | SourceTableInterface): string {
  return JSON.stringify([pattern.connectionTag, pattern.schema, pattern.name]);
}
