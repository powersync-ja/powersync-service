import {
  BucketDataSource,
  BucketParameterLookupSource,
  BucketParameterQuerierSource,
  BucketParameterQuerierSourceDefinition,
  CreateSourceParams,
  HydratedBucketSource
} from './BucketSource.js';
import {
  BucketParameterQuerier,
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
  QuerierError,
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
  bucketDataSources: BucketDataSource[];
  bucketParameterLookupSources: BucketParameterLookupSource[];

  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  readonly definition: SqlSyncRules;

  constructor(params: {
    definition: SqlSyncRules;
    createParams: CreateSourceParams;
    bucketDataSources: BucketDataSource[];
    bucketParameterLookupSources: BucketParameterLookupSource[];
    eventDescriptors?: SqlEventDescriptor[];
    compatibility?: CompatibilityContext;
  }) {
    this.bucketDataSources = params.bucketDataSources;
    this.bucketParameterLookupSources = params.bucketParameterLookupSources;
    this.definition = params.definition;

    if (params.eventDescriptors) {
      this.eventDescriptors = params.eventDescriptors;
    }
    if (params.compatibility) {
      this.compatibility = params.compatibility;
    }

    for (let definition of this.definition.bucketSources) {
      const hydratedBucketSource: HydratedBucketSource = { definition: definition, parameterQuerierSources: [] };
      this.bucketSources.push(hydratedBucketSource);
      for (let querier of definition.parameterQuerierSources) {
        hydratedBucketSource.parameterQuerierSources.push(querier.createParameterQuerierSource(params.createParams));
      }
    }
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
    let rawResults: EvaluationResult[] = [];
    for (let source of this.bucketDataSources) {
      rawResults.push(...source.evaluateRow(options));
    }

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
    let rawResults: EvaluatedParametersResult[] = [];
    for (let source of this.bucketParameterLookupSources) {
      rawResults.push(...source.evaluateParameterRow(table, row));
    }

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
        for (let querier of source.parameterQuerierSources) {
          querier.pushBucketParameterQueriers(pending, options);
        }
      }
    }

    const querier = mergeBucketParameterQueriers(queriers);
    return { querier, errors };
  }
}
