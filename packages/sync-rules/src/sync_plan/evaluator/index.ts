import * as plan from '../plan.js';
import { PreparedStreamBucketDataSource } from './bucket_data_source.js';
import { PreparedParameterIndexLookupCreator } from './parameter_index_lookup_creator.js';
import { StreamBucketSource, StreamInput } from './bucket_source.js';
import { ScalarExpressionEngine } from '../engine/scalar_expression_engine.js';
import { BaseSyncConfig } from '../../BaseSyncConfig.js';

export interface StreamEvaluationContext {
  engine: ScalarExpressionEngine;

  /**
   * Source contents that were used to compile the sync plan.
   *
   * This is not used to evaluate rows, but required for all sync config instances.
   */
  sourceText: string;
}

export class PrecompiledSyncConfig extends BaseSyncConfig {
  constructor(
    private readonly plan: plan.SyncPlan,
    context: StreamEvaluationContext
  ) {
    super(context.sourceText);

    const preparedBuckets = new Map<plan.StreamBucketDataSource, PreparedStreamBucketDataSource>();
    const preparedLookups = new Map<plan.StreamParameterIndexLookupCreator, PreparedParameterIndexLookupCreator>();

    for (const bucket of plan.buckets) {
      const prepared = new PreparedStreamBucketDataSource(bucket, context);
      preparedBuckets.set(bucket, prepared);
      this.bucketDataSources.push(prepared);
    }

    for (const parameter of plan.parameterIndexes) {
      const prepared = new PreparedParameterIndexLookupCreator(parameter, context);
      preparedLookups.set(parameter, prepared);
      this.bucketParameterLookupSources.push(prepared);
    }

    const streamInput: StreamInput = {
      ...context,
      preparedBuckets,
      preparedLookups
    };
    for (const stream of plan.streams) {
      this.bucketSources.push(new StreamBucketSource(stream, streamInput));
    }
  }

  protected asSyncConfig(): this {
    return this;
  }

  extractSyncPlan(): plan.SyncPlan | null {
    return this.plan;
  }
}
