import { BucketInclusionReason, ResolvedBucket } from '../BucketDescription.js';
import { ParameterLookupSource, PendingQueriers } from '../BucketParameterQuerier.js';
import { BucketDataSource, CreateSourceParams, ParameterIndexLookupCreator } from '../BucketSource.js';
import { GetQuerierOptions, RequestedStream } from '../SqlSyncRules.js';
import { RequestParameters, SqliteValue } from '../types.js';
import { buildBucketName, JSONBucketNameSerialize } from '../utils.js';
import { StreamEvaluationContext } from './evaluator.js';
import { RequestParameterEvaluators, SqliteParameterValue } from './lookup_stages.js';
import * as plan from './plan.js';
import { PreparedRequestFilters, prepareFilters } from './request_filters.js';

export interface StreamInput extends StreamEvaluationContext {
  preparedBuckets: Map<plan.StreamBucketDataSource, BucketDataSource>;
  preparedLookups: Map<plan.StreamParameterIndexLookupCreator, ParameterIndexLookupCreator>;
}

export class PreparedQuerier {
  private readonly requestFilters: PreparedRequestFilters;
  private readonly lookupStages: RequestParameterEvaluators;
  private readonly dataSource: BucketDataSource;

  constructor(
    readonly stream: plan.StreamOptions,
    querier: plan.StreamQuerier,
    options: StreamInput
  ) {
    this.dataSource = options.preparedBuckets.get(querier.bucket)!;
    this.requestFilters = prepareFilters(options.engine, querier.requestFilters);
    this.lookupStages = RequestParameterEvaluators.prepare(querier.lookupStages, querier.sourceInstantiation, options);
  }

  partialInstantiationForGlobalRequestdata(
    result: PendingQueriers,
    options: GetQuerierOptions
  ): RequestParameterEvaluators | null {
    // If the request has a condition like WHERE auth.param('is_admin') that doesn't match, we don't have to push
    // queriers at all.
    try {
      if (!this.requestFilters.requestMatches(options.globalParameters)) {
        return null;
      }

      // To reduce work we need to duplicate for each subscription, pre-evaluate parameters that only depend on the auth
      // token or global request data.
      const evaluator = this.lookupStages.clone();
      evaluator.partiallyInstantiate({
        request: options.globalParameters,
        hasSubscriptionData: false
      });

      if (evaluator.isDefinitelyUninstantiable()) {
        return null;
      }

      return evaluator;
    } catch (e) {
      result.errors.push({
        descriptor: this.stream.name,
        message: e.message
      });

      return null;
    }
  }

  querierForSubscription(
    hydration: CreateSourceParams,
    result: PendingQueriers,
    params: RequestParameters,
    subscription: RequestedStream | null,
    evaluators: RequestParameterEvaluators
  ) {
    const reason: BucketInclusionReason = subscription != null ? { subscription: subscription.opaque_id } : 'default';
    evaluators = evaluators.clone();

    try {
      if (!this.requestFilters.subscriptionMatches(params)) {
        return;
      }

      evaluators.partiallyInstantiate({ request: params, hasSubscriptionData: true });
      if (evaluators.isDefinitelyUninstantiable()) {
        return;
      }

      const bucketScope = hydration.hydrationState.getBucketSourceScope(this.dataSource);

      const parametersToBucket = (instantiation: SqliteParameterValue[]): ResolvedBucket => {
        return {
          definition: this.stream.name,
          inclusion_reasons: [reason],
          bucket: buildBucketName(bucketScope, JSONBucketNameSerialize.stringify(instantiation)),
          priority: this.stream.priority
        };
      };

      // Do we need parameter lookups to resolve parameters?
      const staticInstantiation = evaluators.extractFullInstantiation();
      if (staticInstantiation) {
        // We don't! Return static querier.
        result.queriers.push({
          staticBuckets: staticInstantiation.map(parametersToBucket),
          hasDynamicBuckets: false,
          parameterQueryLookups: [],
          async queryDynamicBucketDescriptions() {
            return [];
          }
        });
      } else {
        result.queriers.push({
          staticBuckets: [],
          hasDynamicBuckets: true,
          parameterQueryLookups: [],
          async queryDynamicBucketDescriptions(source) {
            const instantiation = await evaluators.instantiate({
              hydrationState: hydration.hydrationState,
              source,
              request: params,
              hasSubscriptionData: true
            });

            return [...instantiation.map(parametersToBucket)];
          }
        });
      }
    } catch (e) {
      result.errors.push({
        descriptor: this.stream.name,
        message: `Error evaluating bucket ids: ${e.message}`,
        subscription: subscription ?? undefined
      });
    }
  }
}
