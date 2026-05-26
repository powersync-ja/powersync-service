import { BucketDataSource, CreateSourceParams, HydratedBucketSource } from './BucketSource.js';
import {
  BucketParameterQuerier,
  CompatibilityContext,
  EvaluatedParameters,
  EvaluatedRow,
  EvaluationError,
  GetBucketParameterQuerierResult,
  GetQuerierOptions,
  HydrationState,
  isEvaluatedParameters,
  isEvaluatedRow,
  isEvaluationError,
  mergeBucketParameterQueriers,
  mergeDataSources,
  mergeParameterIndexLookupCreators,
  ParameterIndexLookupCreator,
  QuerierError,
  ScopedEvaluateParameterRow,
  ScopedEvaluateRow,
  SqlEventDescriptor,
  SqliteInputValue,
  SqliteValue,
  SyncConfig,
  TablePattern
} from './index.js';
import { SourceTableRef, sourceTableRefKey } from './SourceTableRef.js';
import { EvaluatedParametersResult, EvaluateRowOptions, EvaluationResult, SqliteRow } from './types.js';
import { applyRowContext } from './utils.js';

export interface MatchingSources {
  bucketDataSources: BucketDataSource[];
  parameterLookupSources: ParameterIndexLookupCreator[];
}

/**
 * Hydrated sync config is sync config definitions along with persisted state. Currently, the persisted state
 * specifically affects bucket names.
 */
export class HydratedSyncRules {
  bucketSources: HydratedBucketSource[] = [];
  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  readonly definition: SyncConfig;
  readonly definitions: SyncConfig[];

  private readonly innerEvaluateRow: ScopedEvaluateRow;
  private readonly innerEvaluateParameterRow: ScopedEvaluateParameterRow;
  private readonly hydrationState: HydrationState;
  private readonly matchingSourcesCache = new Map<string, MatchingSources>();
  private mergedEvaluatorCache = new WeakMap<BucketDataSource[], { evaluateRow: ScopedEvaluateRow }>();
  private mergedParameterIndexCreatorCache = new WeakMap<
    ParameterIndexLookupCreator[],
    { evaluateParameterRow: ScopedEvaluateParameterRow }
  >();

  constructor(params: {
    definition?: SyncConfig;
    definitions?: SyncConfig[];
    createParams: CreateSourceParams;
    bucketDataSources?: BucketDataSource[];
    bucketParameterIndexLookupCreators?: ParameterIndexLookupCreator[];
    eventDescriptors?: SqlEventDescriptor[];
    compatibility?: CompatibilityContext;
  }) {
    const hydrationState = params.createParams.hydrationState;
    this.hydrationState = hydrationState;

    const definitions = params.definitions ?? (params.definition == null ? [] : [params.definition]);
    if (definitions.length == 0) {
      throw new Error('HydratedSyncRules requires at least one SyncConfig definition');
    }

    this.definitions = [...definitions];
    this.definition = this.definitions[0];
    this.compatibility = assertSharedCompatibility(this.definitions, params.compatibility);

    const bucketDataSources =
      params.bucketDataSources ?? definitions.flatMap((definition) => definition.bucketDataSources);
    const bucketParameterIndexLookupCreators =
      params.bucketParameterIndexLookupCreators ??
      definitions.flatMap((definition) => definition.bucketParameterLookupSources);

    this.innerEvaluateRow = mergeDataSources(hydrationState, bucketDataSources).evaluateRow;
    this.innerEvaluateParameterRow = mergeParameterIndexLookupCreators(
      hydrationState,
      bucketParameterIndexLookupCreators
    ).evaluateParameterRow;

    if (params.eventDescriptors) {
      this.eventDescriptors = params.eventDescriptors;
    } else {
      this.eventDescriptors = definitions.flatMap((definition) => definition.eventDescriptors);
    }

    this.bucketSources = definitions.flatMap((definition) =>
      definition.bucketSources.map((source) => source.hydrate(params.createParams))
    );
  }

  // These methods do not depend on hydration, so we can multiplex them across definitions.

  getSourceTables() {
    const sourceTables = new Map<string, TablePattern>();
    for (const definition of this.definitions) {
      definition.writeSourceTables(sourceTables);
    }
    return [...sourceTables.values()];
  }

  tableTriggersEvent(table: SourceTableRef): boolean {
    return this.definitions.some((definition) => definition.tableTriggersEvent(table));
  }

  tableSyncsData(table: SourceTableRef): boolean {
    return this.definitions.some((definition) => definition.tableSyncsData(table));
  }

  tableSyncsParameters(table: SourceTableRef): boolean {
    return this.definitions.some((definition) => definition.tableSyncsParameters(table));
  }

  getMatchingSources(source: SourceTableRef): MatchingSources {
    const table: SourceTableRef = {
      connectionTag: source.connectionTag,
      schema: source.schema,
      name: source.name
    };
    const key = sourceTableRefKey(table);
    const cached = this.matchingSourcesCache.get(key);
    if (cached != null) {
      return cached;
    }

    const matchingSources = {
      bucketDataSources: this.definitions.flatMap((definition) =>
        definition.bucketDataSources.filter((source) => source.tableSyncsData(table))
      ),
      parameterLookupSources: this.definitions.flatMap((definition) =>
        definition.bucketParameterLookupSources.filter((source) => source.tableSyncsParameters(table))
      )
    };
    this.matchingSourcesCache.set(key, matchingSources);
    return matchingSources;
  }

  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    return applyRowContext(source, this.compatibility);
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
    let rawResults: EvaluationResult[];
    if (options.bucketDataSources != null) {
      // This array is generally expected to be stable, so makes for a good cache key.
      // It is not a strict requirement to use stable arrays, but it can help for performance.
      let merged = this.mergedEvaluatorCache.get(options.bucketDataSources);
      if (merged == null) {
        merged = mergeDataSources(this.hydrationState, options.bucketDataSources);
        this.mergedEvaluatorCache.set(options.bucketDataSources, merged);
      }
      rawResults = merged.evaluateRow(options);
    } else {
      rawResults = this.innerEvaluateRow(options);
    }
    const results = rawResults.filter(isEvaluatedRow) as EvaluatedRow[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];

    return { results, errors };
  }

  /**
   * Throws errors.
   */
  evaluateParameterRow(
    table: SourceTableRef,
    row: SqliteRow,
    options?: { parameterLookupSources?: ParameterIndexLookupCreator[] }
  ): EvaluatedParameters[] {
    const { results, errors } = this.evaluateParameterRowWithErrors(table, row, options);
    if (errors.length > 0) {
      throw new Error(errors[0].error);
    }
    return results;
  }

  evaluateParameterRowWithErrors(
    table: SourceTableRef,
    row: SqliteRow,
    options?: { parameterLookupSources?: ParameterIndexLookupCreator[] }
  ): { results: EvaluatedParameters[]; errors: EvaluationError[] } {
    let rawResults: EvaluatedParametersResult[];
    if (options?.parameterLookupSources != null) {
      // This array is generally expected to be stable, so makes for a good cache key.
      // It is not a strict requirement to use stable arrays, but it can help for performance.
      let merged = this.mergedParameterIndexCreatorCache.get(options.parameterLookupSources);
      if (merged == null) {
        merged = mergeParameterIndexLookupCreators(this.hydrationState, options.parameterLookupSources);
        this.mergedParameterIndexCreatorCache.set(options.parameterLookupSources, merged);
      }
      rawResults = merged.evaluateParameterRow(table, row);
    } else {
      rawResults = this.innerEvaluateParameterRow(table, row);
    }
    const results = rawResults.filter(isEvaluatedParameters) as EvaluatedParameters[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];
    return { results, errors };
  }

  getBucketParameterQuerier(options: GetQuerierOptions): GetBucketParameterQuerierResult {
    if (this.definitions.length != 1) {
      throw new Error('getBucketParameterQuerier() is not supported for HydratedSyncRules with multiple SyncConfigs');
    }

    const queriers: BucketParameterQuerier[] = [];
    const errors: QuerierError[] = [];
    const pending = { queriers, errors };

    for (const source of this.bucketSources) {
      if (
        (source.definition.subscribedToByDefault && options.hasDefaultStreams) ||
        source.definition.name in options.streams
      ) {
        source.pushBucketParameterQueriers(pending, options);
      }
    }

    const querier = mergeBucketParameterQueriers(queriers);
    return { querier, errors };
  }
}

function assertSharedCompatibility(
  definitions: SyncConfig[],
  override: CompatibilityContext | undefined
): CompatibilityContext {
  const compatibility = override ?? definitions[0].compatibility;
  if (definitions.length == 1) {
    return compatibility;
  }

  for (const definition of definitions) {
    if (!definition.compatibility.equals(compatibility)) {
      throw new Error('All SyncConfigs in a HydratedSyncRules instance must use the same CompatibilityContext');
    }
  }

  return compatibility;
}
