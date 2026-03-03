import * as plan from '../plan.js';
import { PreparedStreamBucketDataSource } from './bucket_data_source.js';
import { PreparedParameterIndexLookupCreator } from './parameter_index_lookup_creator.js';
import { StreamBucketSource, StreamInput } from './bucket_source.js';
import { ScalarExpressionEngine } from '../engine/scalar_expression_engine.js';
import { SyncConfig } from '../../SyncConfig.js';
import { CompatibilityContext } from '../../compatibility.js';
import { SqlEventDescriptor } from '../../index.js';

export interface StreamEvaluationContext {
  defaultSchema: string;
  engine: ScalarExpressionEngine;

  /**
   * Source contents that were used to compile the sync plan.
   *
   * This is not used to evaluate rows, but required for all sync config instances.
   */
  sourceText: string;
}

export class PrecompiledSyncConfig extends SyncConfig {
  constructor(
    readonly plan: plan.SyncPlan,
    compatibility: CompatibilityContext,
    eventDefinitions: SqlEventDescriptor[],
    context: StreamEvaluationContext
  ) {
    super(context.sourceText);
    this.compatibility = compatibility;
    this.eventDescriptors = eventDefinitions;

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
}
