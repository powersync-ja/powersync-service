import {
  BucketDataSource,
  BucketDataSourceDefinition,
  BucketParameterSource,
  BucketParameterSourceDefinition
} from './BucketSource.js';
import {
  BucketParameterQuerier,
  CompatibilityContext,
  CompatibilityOption,
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
  SqlEventDescriptor
} from './index.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { EvaluatedParametersResult, EvaluateRowOptions, EvaluationResult, SqliteRow } from './types.js';

export class SyncRules {
  bucketDataSources: BucketDataSource[] = [];
  bucketParameterSources: BucketParameterSource[] = [];

  eventDescriptors: SqlEventDescriptor[] = [];
  compatibility: CompatibilityContext = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;

  constructor(params: {
    bucketDataSources: BucketDataSource[];
    bucketParameterSources: BucketParameterSource[];
    eventDescriptors?: SqlEventDescriptor[];
    compatibility?: CompatibilityContext;
  }) {
    this.bucketDataSources = params.bucketDataSources;
    this.bucketParameterSources = params.bucketParameterSources;
    if (params.eventDescriptors) {
      this.eventDescriptors = params.eventDescriptors;
    }
    if (params.compatibility) {
      this.compatibility = params.compatibility;
    }
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
    for (let source of this.bucketParameterSources) {
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

    for (const source of this.bucketParameterSources) {
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
