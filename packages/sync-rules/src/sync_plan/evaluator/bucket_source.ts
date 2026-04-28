import { BucketInclusionReason, ResolvedBucket } from '../../BucketDescription.js';
import { PendingQueriers } from '../../BucketParameterQuerier.js';
import {
  BucketDataSource,
  BucketSource,
  BucketSourceType,
  CreateSourceParams,
  HydratedBucketSource,
  ParameterIndexLookupCreator
} from '../../BucketSource.js';
import { RequestedStream } from '../../SqlSyncRules.js';
import { RequestParameters, SqliteParameterValue } from '../../types.js';
import { bucketDescription, JSONBucketNameSerialize, resolvedBucket } from '../../utils.js';
import { mapExternalDataToInstantiation, ScalarExpressionEngine } from '../engine/scalar_expression_engine.js';
import { SqlExpression } from '../expression.js';
import * as plan from '../plan.js';
import { StreamEvaluationContext } from './index.js';
import { parametersForRequest, RequestParameterEvaluators } from './parameter_evaluator.js';

export interface StreamInput extends StreamEvaluationContext {
  preparedBuckets: Map<plan.StreamBucketDataSource, BucketDataSource>;
  preparedLookups: Map<plan.StreamParameterIndexLookupCreator, ParameterIndexLookupCreator>;
}

export class StreamBucketSource implements BucketSource {
  readonly dataSources: BucketDataSource[] = [];
  readonly parameterIndexLookupCreators: ParameterIndexLookupCreator[] = [];

  constructor(
    readonly stream: plan.CompiledSyncStream,
    private readonly input: StreamInput
  ) {
    for (const querier of stream.queriers) {
      const mappedSource = input.preparedBuckets.get(querier.bucket)!;
      this.dataSources.push(mappedSource);
    }
  }

  get name(): string {
    return this.stream.stream.name;
  }

  get subscribedToByDefault(): boolean {
    return this.stream.stream.isSubscribedByDefault;
  }

  get type(): BucketSourceType {
    return BucketSourceType.SYNC_STREAM;
  }

  debugRepresentation() {
    // TODO: Implement debugRepresentation for compiled sync streams
    return `stream ${this.stream.stream.name}`;
  }

  hydrate(params: CreateSourceParams): HydratedBucketSource {
    const queriers = this.stream.queriers.map((q) => new PreparedQuerier(this.stream.stream, q, this.input));

    return {
      definition: this,
      pushBucketParameterQueriers: (result, options) => {
        const subscriptions = options.streams[this.name] ?? [];
        if (!this.subscribedToByDefault && !subscriptions.length) {
          // The client is not subscribing to this stream, so don't query buckets related to it.
          return;
        }

        let hasExplicitDefaultSubscription = false;
        for (const subscription of subscriptions) {
          let subscriptionParams = options.globalParameters;
          if (subscription.parameters != null) {
            subscriptionParams = subscriptionParams.withAddedStreamParameters(subscription.parameters);
          } else {
            hasExplicitDefaultSubscription = true;
          }

          for (const querier of queriers) {
            querier.querierForSubscription(params, result, subscriptionParams, subscription);
          }
        }

        // If the stream is subscribed to by default and there is no explicit subscription that would match the default
        // subscription, also include the default querier.
        if (this.subscribedToByDefault && !hasExplicitDefaultSubscription && options.hasDefaultStreams) {
          for (const querier of queriers) {
            querier.querierForSubscription(params, result, options.globalParameters, null);
          }
        }
      }
    };
  }
}

class PreparedQuerier {
  private readonly matchesParameters: (parameters: RequestParameters) => boolean;
  private readonly lookupStages: RequestParameterEvaluators;
  private readonly dataSource: BucketDataSource;

  constructor(
    readonly stream: plan.StreamOptions,
    querier: plan.StreamQuerier,
    options: StreamInput
  ) {
    this.dataSource = options.preparedBuckets.get(querier.bucket)!;
    this.matchesParameters = PreparedQuerier.prepareFilters(options.engine, querier.requestFilters);

    this.lookupStages = RequestParameterEvaluators.prepare(querier.lookupStages, querier.sourceInstantiation, options);
  }

  querierForSubscription(
    hydration: CreateSourceParams,
    result: PendingQueriers,
    params: RequestParameters,
    subscription: RequestedStream | null
  ) {
    const reason: BucketInclusionReason = subscription != null ? { subscription: subscription.opaque_id } : 'default';

    try {
      if (!this.matchesParameters(params)) {
        return;
      }

      const subscriptionEvaluators = this.lookupStages.clone();
      const partialInstantiationResult = subscriptionEvaluators.partiallyInstantiate({ request: params });
      const bucketScope = hydration.hydrationState.getBucketSourceScope(this.dataSource);
      const parametersToBucket = (instantiation: SqliteParameterValue[]): ResolvedBucket => {
        const desc = bucketDescription(
          bucketScope,
          JSONBucketNameSerialize.stringify(instantiation),
          subscription?.priorityOverride ?? this.stream.priority
        );
        return resolvedBucket(desc, {
          definition: this.stream.name,
          inclusion_reasons: [reason]
        });
      };

      if (partialInstantiationResult != null) {
        // This partial instantiation is enough to resolve parameters.
        if (partialInstantiationResult.length != 0) {
          result.queriers.push({
            staticBuckets: partialInstantiationResult.map(parametersToBucket),
            hasDynamicBuckets: false,
            async queryDynamicBucketDescriptions() {
              return [];
            }
          });
        }

        return;
      }

      result.queriers.push({
        staticBuckets: [],
        hasDynamicBuckets: true,
        async queryDynamicBucketDescriptions(source) {
          const evaluators = subscriptionEvaluators.clone();
          const instantiation = await evaluators.instantiate({
            hydrationState: hydration.hydrationState,
            source,
            request: params
          });

          return [...instantiation].map(parametersToBucket);
        }
      });
    } catch (e) {
      result.errors.push({
        descriptor: this.stream.name,
        message: `Error evaluating bucket ids: ${e.message}`,
        subscription: subscription ?? undefined
      });
    }
  }

  private static prepareFilters(
    engine: ScalarExpressionEngine,
    requestFilters: SqlExpression<plan.RequestSqlParameterValue>[]
  ) {
    const mapExpressions = mapExternalDataToInstantiation<plan.ColumnSqlParameterValue>();
    const evaluator = engine.prepareEvaluator({
      outputs: [],
      filters: requestFilters.map((f) => mapExpressions.transform(f))
    });
    const instantiation = mapExpressions.instantiation;

    return (parameters: RequestParameters) => {
      // We've added request filters as filters to prepareEvaluator, so if we get a row then the subscription matches.
      return evaluator.evaluate(parametersForRequest(parameters, instantiation)).length != 0;
    };
  }
}
