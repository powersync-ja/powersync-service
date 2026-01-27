import { SqlSyncRules } from '../../SqlSyncRules.js';
import * as plan from '../plan.js';
import { PreparedStreamBucketDataSource } from './bucket_data_source.js';
import { PreparedParameterIndexLookupCreator } from './parameter_index_lookup_creator.js';
import { StreamBucketSource, StreamInput } from './bucket_source.js';
import { ScalarExpressionEngine } from './scalar_expression_evaluator.js';

export interface StreamEvaluationContext {
  engine: ScalarExpressionEngine;
}

export function addPrecompiledSyncPlanToRules(
  plan: plan.SyncPlan,
  rules: SqlSyncRules,
  context: StreamEvaluationContext
) {
  const preparedBuckets = new Map<plan.StreamBucketDataSource, PreparedStreamBucketDataSource>();
  const preparedLookups = new Map<plan.StreamParameterIndexLookupCreator, PreparedParameterIndexLookupCreator>();

  for (const bucket of plan.buckets) {
    const prepared = new PreparedStreamBucketDataSource(bucket, context);
    preparedBuckets.set(bucket, prepared);
    rules.bucketDataSources.push(prepared);
  }

  for (const parameter of plan.parameterIndexes) {
    const prepared = new PreparedParameterIndexLookupCreator(parameter, context);
    preparedLookups.set(parameter, prepared);
    rules.bucketParameterLookupSources.push(prepared);
  }

  const streamInput: StreamInput = {
    ...context,
    preparedBuckets,
    preparedLookups
  };
  for (const stream of plan.streams) {
    rules.bucketSources.push(new StreamBucketSource(stream, streamInput));
  }
}
