import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
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
import { StreamVariant } from './filter.js';

export class SyncStream implements BucketSource {
  name: string;
  subscribedToByDefault: boolean;
  variants: StreamVariant[];
  data: BaseSqlDataQuery;

  constructor(name: string) {
    this.name = name;
    this.subscribedToByDefault = false;
    this.variants = [];
    this.data = '' as any;
  }

  pushBucketParameterQueriers(result: BucketParameterQuerier[], options: GetQuerierOptions): void {
    const subscriptions = options.streams[this.name] ?? [];

    if (!this.subscribedToByDefault && subscriptions.length) {
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
  ) {}

  hasDynamicBucketQueries(): boolean {
    throw new Error('Method not implemented.');
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    throw new Error('Method not implemented.');
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    throw new Error('Method not implemented.');
  }

  getSourceTables(): Set<TablePattern> {
    throw new Error('Method not implemented.');
  }

  resolveResultSets(schema: SourceSchema): ResultSetDescription[] {
    throw new Error('Method not implemented.');
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[] {
    throw new Error('Method not implemented.');
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
