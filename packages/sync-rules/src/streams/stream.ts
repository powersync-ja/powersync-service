import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
import { BucketInclusionReason, BucketPriority, DEFAULT_BUCKET_PRIORITY } from '../BucketDescription.js';
import { BucketParameterQuerier, mergeBucketParameterQueriers, PendingQueriers } from '../BucketParameterQuerier.js';
import {
  BucketDataSource,
  BucketDataSourceDefinition,
  BucketParameterSource,
  BucketParameterSourceDefinition,
  BucketSourceType,
  CreateSourceParams
} from '../BucketSource.js';
import { ColumnDefinition } from '../ExpressionType.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { GetQuerierOptions, RequestedStream } from '../SqlSyncRules.js';
import { TablePattern } from '../TablePattern.js';
import {
  BucketIdTransformer,
  EvaluatedParametersResult,
  EvaluateRowOptions,
  EvaluationResult,
  RequestParameters,
  SourceSchema,
  TableRow
} from '../types.js';
import { StreamVariant } from './variant.js';

export class SyncStream implements BucketDataSourceDefinition, BucketParameterSourceDefinition {
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

  public get type(): BucketSourceType {
    return BucketSourceType.SYNC_STREAM;
  }

  public get bucketParameters(): string[] {
    // FIXME: check whether this is correct.
    // Could there be multiple variants with different bucket parameters?
    return this.data.bucketParameters;
  }

  createDataSource(params: CreateSourceParams): BucketDataSource {
    return {
      definition: this,

      evaluateRow: (options: EvaluateRowOptions): EvaluationResult[] => {
        if (!this.data.applies(options.sourceTable)) {
          return [];
        }

        const stream = this;
        const row: TableRow = {
          sourceTable: options.sourceTable,
          record: options.record
        };

        return this.data.evaluateRowWithOptions({
          table: options.sourceTable,
          row: options.record,
          bucketIds() {
            const bucketIds: string[] = [];
            for (const variant of stream.variants) {
              bucketIds.push(...variant.bucketIdsForRow(stream.name, row, params.bucketIdTransformer));
            }

            return bucketIds;
          }
        });
      }
    };
  }

  createParameterSource(params: CreateSourceParams): BucketParameterSource {
    return {
      definition: this,

      pushBucketParameterQueriers: (result: PendingQueriers, options: GetQuerierOptions): void => {
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

          this.queriersForSubscription(result, subscription, subscriptionParams, params.bucketIdTransformer);
        }

        // If the stream is subscribed to by default and there is no explicit subscription that would match the default
        // subscription, also include the default querier.
        if (this.subscribedToByDefault && !hasExplicitDefaultSubscription) {
          this.queriersForSubscription(result, null, options.globalParameters, params.bucketIdTransformer);
        }
      },
      evaluateParameterRow: (sourceTable, row) => {
        const result: EvaluatedParametersResult[] = [];

        for (const variant of this.variants) {
          variant.pushParameterRowEvaluation(result, sourceTable, row);
        }

        return result;
      }
    };
  }

  private queriersForSubscription(
    result: PendingQueriers,
    subscription: RequestedStream | null,
    params: RequestParameters,
    bucketIdTransformer: BucketIdTransformer
  ) {
    const reason: BucketInclusionReason = subscription != null ? { subscription: subscription.opaque_id } : 'default';
    const queriers: BucketParameterQuerier[] = [];

    try {
      for (const variant of this.variants) {
        const querier = variant.querier(this, reason, params, bucketIdTransformer);
        if (querier) {
          queriers.push(querier);
        }
      }

      result.queriers.push(...queriers);
    } catch (e) {
      result.errors.push({
        descriptor: this.name,
        message: `Error evaluating bucket ids: ${e.message}`,
        subscription: subscription ?? undefined
      });
    }
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.data.applies(table);
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (const variant of this.variants) {
      for (const subquery of variant.subqueries) {
        if (subquery.parameterTable.matches(table)) {
          return true;
        }
      }
    }

    return false;
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    result.add(this.data.sourceTable);
    for (let variant of this.variants) {
      for (const subquery of variant.subqueries) {
        result.add(subquery.parameterTable);
      }
    }

    // Note: No physical tables for global_parameter_queries

    return result;
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>) {
    this.data.resolveResultSets(schema, tables);
  }

  debugRepresentation() {
    return {
      name: this.name,
      type: BucketSourceType[this.type],
      variants: this.variants.map((v) => v.debugRepresentation()),
      data: {
        table: this.data.sourceTable,
        columns: this.data.columnOutputNames()
      }
    };
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    result[this.data.table!.sqlName] ??= [];
    const r = {
      query: this.data.sql
    };

    result[this.data.table!.sqlName].push(r);
  }
}
