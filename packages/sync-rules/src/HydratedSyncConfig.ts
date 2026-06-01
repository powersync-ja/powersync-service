import { BucketDataSource, HydratedBucketSource } from './BucketSource.js';
import {
  BucketParameterQuerier,
  BucketSource,
  CompatibilityContext,
  EvaluatedParameters,
  EvaluatedRow,
  EvaluationError,
  GetBucketParameterQuerierResult,
  GetQuerierOptions,
  HydrateSyncConfigParams,
  HydrationInput,
  isEvaluatedParameters,
  isEvaluatedRow,
  isEvaluationError,
  mergeBucketParameterQueriers,
  mergeDataSources,
  mergeParameterIndexLookupCreators,
  ParameterIndexLookupCreator,
  parameterLookupScopeKey,
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
import { createScalarExpressionEngine } from './sync_plan/engine/factory.js';
import { EvaluatedParametersResult, EvaluateRowOptions, EvaluationResult, SqliteRow } from './types.js';
import { applyRowContext, uniqueBy } from './utils.js';

export interface MatchingSources {
  bucketDataSources: BucketDataSource[];
  parameterLookupSources: ParameterIndexLookupCreator[];
}

/**
 * HydratedSyncConfig is sync config definitions along with persisted state.
 *
 * This may be a single SyncConfig, or multiple SyncConfigs merged together.
 * In the case of multiple SyncConfigs, they must share the same CompatibilityContext,
 * but can have different bucket sources.
 *
 * The persisted state specifically affects bucket names, as well as V3+ storage structure.
 */
export class HydratedSyncConfig {
  /**
   * These are used by queriers, and do not support merging across multiple SyncConfigs.
   */
  private bucketSources: HydratedBucketSource[] = [];

  eventDescriptors: SqlEventDescriptor[] = [];

  /**
   * Only a single compatibility context is supported across all merged SyncConfigs.
   */
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  /**
   * The source definitions.
   *
   * For most functionality, we don't use these directly, but rather use the
   * merged definitions such as bucketDataSources.
   */
  private readonly sourceDefinitions: SyncConfig[];

  /**
   * Bucket data sources from underlying SyncConfig definitions.
   */
  readonly bucketDataSources: BucketDataSource[];
  readonly bucketParameterLookupSources: ParameterIndexLookupCreator[];

  /**
   * These are used by queriers, and do not support merging across multiple SyncConfigs.
   */
  readonly #bucketSourceDefinitions: BucketSource[];

  private readonly innerEvaluateRow: ScopedEvaluateRow;
  private readonly innerEvaluateParameterRow: ScopedEvaluateParameterRow;
  private readonly hydrationInput: HydrationInput;
  private readonly matchingSourcesCache = new Map<string, MatchingSources>();

  private mergedEvaluatorCache = new WeakMap<BucketDataSource[], { evaluateRow: ScopedEvaluateRow }>();
  private mergedParameterIndexCreatorCache = new WeakMap<
    ParameterIndexLookupCreator[],
    { evaluateParameterRow: ScopedEvaluateParameterRow }
  >();

  constructor(params: { definitions: SyncConfig[]; createParams: HydrateSyncConfigParams }) {
    const hydrationState = params.createParams.hydrationState;
    const definitions = params.definitions;
    if (definitions.length == 0) {
      throw new Error('HydratedSyncRules requires at least one SyncConfig definition');
    }

    this.sourceDefinitions = [...definitions];
    this.compatibility = assertSharedCompatibility(this.sourceDefinitions);
    this.hydrationInput = {
      hydrationState,
      scalarExpressions: createScalarExpressionEngine(this.compatibility, params.createParams.sqlite)
    };

    this.bucketDataSources = uniqueBy(
      definitions.flatMap((definition) => definition.bucketDataSources),
      (source) => hydrationState.getBucketSourceScope(source).bucketPrefix
    );
    this.bucketParameterLookupSources = uniqueBy(
      definitions.flatMap((definition) => definition.bucketParameterLookupSources),
      (source) => parameterLookupScopeKey(hydrationState.getParameterIndexLookupScope(source))
    );

    this.innerEvaluateRow = mergeDataSources(this.hydrationInput, this.bucketDataSources).evaluateRow;
    this.innerEvaluateParameterRow = mergeParameterIndexLookupCreators(
      this.hydrationInput,
      this.bucketParameterLookupSources
    ).evaluateParameterRow;

    this.eventDescriptors = definitions.flatMap((definition) => definition.eventDescriptors);

    if (definitions.length == 1) {
      this.#bucketSourceDefinitions = definitions[0].bucketSources;
      this.bucketSources = this.#bucketSourceDefinitions.map((source) => source.hydrate(this.hydrationInput));
    } else {
      // We do not support merging bucket sources across multiple definitions - these are always used with one
      // SyncConfig at a time.
      this.#bucketSourceDefinitions = [];
      this.bucketSources = [];
    }
  }

  get bucketSourceDefinitions() {
    this.assertSingleSourceDefinition('bucketSourceDefinitions');
    return this.#bucketSourceDefinitions;
  }

  // These methods do not depend on hydration, so we can multiplex them across definitions.

  getSourceTables() {
    const sourceTables = new Map<string, TablePattern>();
    for (const definition of this.sourceDefinitions) {
      definition.writeSourceTables(sourceTables);
    }
    return [...sourceTables.values()];
  }

  tableTriggersEvent(table: SourceTableRef): boolean {
    return this.sourceDefinitions.some((definition) => definition.tableTriggersEvent(table));
  }

  tableSyncsData(table: SourceTableRef): boolean {
    return this.sourceDefinitions.some((definition) => definition.tableSyncsData(table));
  }

  tableSyncsParameters(table: SourceTableRef): boolean {
    return this.sourceDefinitions.some((definition) => definition.tableSyncsParameters(table));
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
      bucketDataSources: this.bucketDataSources.filter((source) => source.tableSyncsData(table)),
      parameterLookupSources: this.bucketParameterLookupSources.filter((source) => source.tableSyncsParameters(table))
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
        merged = mergeDataSources(this.hydrationInput, options.bucketDataSources);
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
        merged = mergeParameterIndexLookupCreators(this.hydrationInput, options.parameterLookupSources);
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
    this.assertSingleSourceDefinition('getBucketParameterQuerier()');

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

  private assertSingleSourceDefinition(debugName: string) {
    // We may split the types in the future to enforce this on a type level instead of runtime level
    if (this.sourceDefinitions.length != 1) {
      throw new Error(`${debugName} is not supported for HydratedSyncRules with multiple SyncConfigs`);
    }
  }
}

function assertSharedCompatibility(definitions: SyncConfig[]): CompatibilityContext {
  const compatibility = definitions[0].compatibility;
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
