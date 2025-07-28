import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
import { BucketInclusionReason, BucketPriority, DEFAULT_BUCKET_PRIORITY } from '../BucketDescription.js';
import { BucketParameterQuerier } from '../BucketParameterQuerier.js';
import { BucketSource, ResultSetDescription } from '../BucketSource.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { GetQuerierOptions } from '../SqlSyncRules.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluatedParametersResult,
  EvaluateRowOptions,
  EvaluationResult,
  RequestParameters,
  SourceSchema,
  SqliteRow
} from '../types.js';
import { isDynamicLookup, StreamVariant } from './filter.js';

export class SyncStream implements BucketSource {
  name: string;
  subscribedToByDefault: boolean;
  priority: BucketPriority;
  variants: StreamVariant[];
  data: BaseSqlDataQuery;

  constructor(name: string, data: BaseSqlDataQuery) {
    this.name = name;
    this.subscribedToByDefault = false;
    this.priority = DEFAULT_BUCKET_PRIORITY;
    this.variants = [];
    this.data = data;
  }

  pushBucketParameterQueriers(result: BucketParameterQuerier[], options: GetQuerierOptions): void {
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

      this.queriersForSubscription(result, subscription.opaque_id, subscriptionParams);
    }

    // If the stream is subscribed to by default and there is no explicit subscription that would match the default
    // subscription, also include the default querier.
    if (this.subscribedToByDefault && !hasExplicitDefaultSubscription) {
      this.queriersForSubscription(result, null, options.globalParameters);
    }
  }

  private queriersForSubscription(
    result: BucketParameterQuerier[],
    subscription_id: string | null,
    params: RequestParameters
  ) {
    const reason: BucketInclusionReason = subscription_id != null ? { subscription: subscription_id } : 'default';

    for (const variant of this.variants) {
      const querier = variant.querier(this, reason, params);
      if (querier) {
        result.push(querier);
      }
    }
  }

  hasDynamicBucketQueries(): boolean {
    throw new Error('Method not implemented.');
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.data.applies(table);
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (const variant of this.variants) {
      for (const parameter of variant.parameters) {
        const lookup = parameter.lookup;
        if (isDynamicLookup(lookup) && lookup.parameterTable.matches(table)) {
          return true;
        }
      }
    }

    return false;
  }

  getSourceTables(): Set<TablePattern> {
    throw new Error('Method not implemented.');
  }

  resolveResultSets(schema: SourceSchema): ResultSetDescription[] {
    throw new Error('Method not implemented.');
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[] {
    const result: EvaluatedParametersResult[] = [];

    for (const variant of this.variants) {
      variant.pushParameterRowEvaluation(result, sourceTable, row);
    }

    return result;
  }

  evaluateRow(options: EvaluateRowOptions): EvaluationResult[] {
    if (!this.data.applies(options.sourceTable)) {
      return [];
    }

    const stream = this;
    return this.data.evaluateRowWithOptions({
      table: options.sourceTable,
      row: options.record,
      bucketIds() {
        const bucketIds: string[] = [];
        for (const variant of stream.variants) {
          bucketIds.push(...variant.bucketIdsForRow(stream.name, options));
        }

        return bucketIds;
      }
    });
  }
}
