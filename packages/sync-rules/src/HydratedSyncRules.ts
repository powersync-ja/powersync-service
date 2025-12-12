import { Scope } from 'ajv/dist/compile/codegen/scope.js';
import { BucketDataSource, CreateSourceParams, HydratedBucketSource } from './BucketSource.js';
import { BucketDataScope, ParameterLookupScope } from './HydrationState.js';
import {
  ParameterIndexLookupCreator,
  BucketParameterQuerier,
  buildBucketName,
  CompatibilityContext,
  EvaluatedParameters,
  EvaluatedRow,
  EvaluationError,
  GetBucketParameterQuerierResult,
  GetQuerierOptions,
  isEvaluatedParameters,
  isEvaluatedRow,
  isEvaluationError,
  mergeBucketParameterQueriers,
  mergeDataSources,
  mergeParameterIndexLookupCreators,
  QuerierError,
  ScopedEvaluateParameterRow,
  ScopedEvaluateRow,
  SqlEventDescriptor,
  SqliteInputValue,
  SqliteValue,
  SqlSyncRules
} from './index.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { EvaluatedParametersResult, EvaluateRowOptions, EvaluationResult, SqliteRow } from './types.js';

/**
 * Hydrated sync rules is sync rule definitions along with persisted state. Currently, the persisted state
 * specifically affects bucket names.
 */
export class HydratedSyncRules {
  bucketSources: HydratedBucketSource[] = [];
  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  readonly definition: SqlSyncRules;

  private readonly innerEvaluateRow: ScopedEvaluateRow;
  private readonly innerEvaluateParameterRow: ScopedEvaluateParameterRow;

  constructor(params: {
    definition: SqlSyncRules;
    createParams: CreateSourceParams;
    bucketDataSources: BucketDataSource[];
    bucketParameterIndexLookupCreators: ParameterIndexLookupCreator[];
    eventDescriptors?: SqlEventDescriptor[];
    compatibility?: CompatibilityContext;
  }) {
    const hydrationState = params.createParams.hydrationState;

    this.definition = params.definition;
    this.innerEvaluateRow = mergeDataSources(hydrationState, params.bucketDataSources).evaluateRow;
    this.innerEvaluateParameterRow = mergeParameterIndexLookupCreators(
      hydrationState,
      params.bucketParameterIndexLookupCreators
    ).evaluateParameterRow;

    if (params.eventDescriptors) {
      this.eventDescriptors = params.eventDescriptors;
    }
    if (params.compatibility) {
      this.compatibility = params.compatibility;
    }

    this.bucketSources = this.definition.bucketSources.map((source) => source.hydrate(params.createParams));
  }

  // These methods do not depend on hydration, so we can just forward them to the definition.

  getSourceTables() {
    return this.definition.getSourceTables();
  }

  tableTriggersEvent(table: SourceTableInterface): boolean {
    return this.definition.tableTriggersEvent(table);
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.definition.tableSyncsData(table);
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    return this.definition.tableSyncsParameters(table);
  }

  applyRowContext<MaybeToast extends undefined = never>(
    source: SqliteRow<SqliteInputValue | MaybeToast>
  ): SqliteRow<SqliteValue | MaybeToast> {
    return this.definition.applyRowContext(source);
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
    const rawResults: EvaluationResult[] = this.innerEvaluateRow(options);
    const results = rawResults.filter(isEvaluatedRow) as EvaluatedRow[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];

    return { results, errors };
  }

  /**
   * Throws errors.
   */
  evaluateParameterRow(table: SourceTableInterface, row: SqliteRow): EvaluatedParameters[] {
    const { results, errors } = this.evaluateParameterRowWithErrors(table, row);
    if (errors.length > 0) {
      throw new Error(errors[0].error);
    }
    return results;
  }

  evaluateParameterRowWithErrors(
    table: SourceTableInterface,
    row: SqliteRow
  ): { results: EvaluatedParameters[]; errors: EvaluationError[] } {
    const rawResults: EvaluatedParametersResult[] = this.innerEvaluateParameterRow(table, row);
    const results = rawResults.filter(isEvaluatedParameters) as EvaluatedParameters[];
    const errors = rawResults.filter(isEvaluationError) as EvaluationError[];
    return { results, errors };
  }

  getBucketParameterQuerier(options: GetQuerierOptions): GetBucketParameterQuerierResult {
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
