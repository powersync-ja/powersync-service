import {
  BucketDataSource,
  BucketSource,
  BucketSourceType,
  CreateSourceParams,
  HydratedBucketSource,
  ParameterIndexLookupCreator
} from '../../BucketSource.js';
import { StreamEvaluationContext } from './index.js';
import * as plan from '../plan.js';
import { mapExternalDataToInstantiation, ScalarExpressionEngine } from '../engine/scalar_expression_engine.js';
import { SqlExpression } from '../expression.js';
import { RequestParameters, SqliteParameterValue } from '../../types.js';
import { parametersForRequest, RequestParameterEvaluators } from './parameter_evaluator.js';
import { PendingQueriers } from '../../BucketParameterQuerier.js';
import { RequestedStream } from '../../SqlSyncRules.js';
import { BucketInclusionReason, ResolvedBucket } from '../../BucketDescription.js';
import { buildBucketName, JSONBucketNameSerialize } from '../../utils.js';

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
        if (this.subscribedToByDefault && !hasExplicitDefaultSubscription) {
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

      subscriptionEvaluators.partiallyInstantiate({ request: params });
      if (subscriptionEvaluators.isDefinitelyUninstantiable()) {
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
      const staticInstantiation = subscriptionEvaluators.extractFullInstantiation();
      if (staticInstantiation) {
        // We don't! Return static querier.
        result.queriers.push({
          staticBuckets: staticInstantiation.map(parametersToBucket),
          hasDynamicBuckets: false,
          async queryDynamicBucketDescriptions() {
            return [];
          }
        });
      } else {
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
      }
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
