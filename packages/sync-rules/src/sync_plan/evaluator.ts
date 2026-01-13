import {
  BucketDataSource,
  BucketSource,
  BucketSourceType,
  CreateSourceParams,
  HydratedBucketSource,
  ParameterIndexLookupCreator
} from '../BucketSource.js';
import { SqlSyncRules } from '../index.js';
import { SyncPlan } from './plan.js';
import { serializeSyncPlan } from './serialize.js';

export interface StreamEvaluationContext {
  engine: SqlEngine;
}

export interface SqlEngine {}

export function addPrecompiledSyncPlanToRules(plan: SyncPlan, rules: SqlSyncRules, context: StreamEvaluationContext) {}

export class StreamBucketSource implements BucketSource {
  constructor(readonly plan: SyncPlan) {
    this.dataSources = [];
    this.parameterIndexLookupCreators = [];
  }

  dataSources: BucketDataSource[];
  parameterIndexLookupCreators: ParameterIndexLookupCreator[];

  hydrate(params: CreateSourceParams): HydratedBucketSource {
    throw new Error('Method not implemented.');
  }

  debugRepresentation(): unknown {
    return serializeSyncPlan(this.plan);
  }
}
