import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
import { BucketInclusionReason, BucketPriority, DEFAULT_BUCKET_PRIORITY } from '../BucketDescription.js';
import { PendingQueriers } from '../BucketParameterQuerier.js';
import {
  BucketDataSource,
  BucketDataSourceDefinition,
  BucketParameterLookupSource,
  BucketParameterLookupSourceDefinition,
  BucketParameterQuerierSource,
  BucketParameterQuerierSourceDefinition,
  BucketSource,
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

export class SyncStream implements BucketSource {
  name: string;
  subscribedToByDefault: boolean;
  priority: BucketPriority;
  variants: StreamVariant[];
  data: BaseSqlDataQuery;

  public readonly dataSources: BucketDataSourceDefinition[];
  public readonly parameterLookupSources: BucketParameterLookupSourceDefinition[];
  public readonly parameterQuerierSources: BucketParameterQuerierSourceDefinition[];

  constructor(name: string, data: BaseSqlDataQuery, variants: StreamVariant[]) {
    this.name = name;
    this.subscribedToByDefault = false;
    this.priority = DEFAULT_BUCKET_PRIORITY;
    this.variants = variants;
    this.data = data;

    this.dataSources = variants.map((variant) => new SyncStreamDataSource(this, data, variant));
    this.parameterQuerierSources = variants.map((variant) => new SyncStreamParameterQuerierSource(this, variant));
    this.parameterLookupSources = variants.map((variant) => new SyncStreamParameterLookupSource(this, variant));
  }

  public get type(): BucketSourceType {
    return BucketSourceType.SYNC_STREAM;
  }
}

export class SyncStreamDataSource implements BucketDataSourceDefinition {
  constructor(
    private stream: SyncStream,
    private data: BaseSqlDataQuery,
    private variant: StreamVariant
  ) {}

  /**
   * Not relevant for sync streams.
   */
  get bucketParameters() {
    return [];
  }

  getSourceTables(): Set<TablePattern> {
    return new Set<TablePattern>([this.data.sourceTable]);
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return this.data.applies(table);
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    return this.data.resolveResultSets(schema, tables);
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    result[this.data.table!.sqlName] ??= [];
    const r = {
      query: this.data.sql
    };

    result[this.data.table!.sqlName].push(r);
  }

  debugRepresentation() {
    return {
      name: this.stream.name,
      type: BucketSourceType[BucketSourceType.SYNC_STREAM],
      variants: this.stream.variants.map((v) => v.debugRepresentation()),
      data: {
        table: this.data.sourceTable,
        columns: this.data.columnOutputNames()
      }
    };
  }

  createDataSource(params: CreateSourceParams): BucketDataSource {
    return {
      evaluateRow: (options: EvaluateRowOptions): EvaluationResult[] => {
        if (!this.data.applies(options.sourceTable)) {
          return [];
        }

        const stream = this.stream;
        const row: TableRow = {
          sourceTable: options.sourceTable,
          record: options.record
        };

        // There is some duplication in work here when there are multiple variants on a stream:
        // Each variant does the same row transformation (only the filters / bucket ids differ).
        // However, architecturally we do need to be able to evaluate each variant separately.
        return this.data.evaluateRowWithOptions({
          table: options.sourceTable,
          row: options.record,
          bucketIds: () => {
            return this.variant.bucketIdsForRow(stream.name, row, params.bucketIdTransformer);
          }
        });
      }
    };
  }
}

export class SyncStreamParameterQuerierSource implements BucketParameterQuerierSourceDefinition {
  // We could eventually split this into a separate source per variant.

  constructor(
    private stream: SyncStream,
    private variant: StreamVariant
  ) {}

  /**
   * Not relevant for sync streams.
   */
  get bucketParameters() {
    return [];
  }

  createParameterQuerierSource(params: CreateSourceParams): BucketParameterQuerierSource {
    const stream = this.stream;
    return {
      pushBucketParameterQueriers: (result: PendingQueriers, options: GetQuerierOptions): void => {
        const subscriptions = options.streams[stream.name] ?? [];

        if (!stream.subscribedToByDefault && !subscriptions.length) {
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
        if (stream.subscribedToByDefault && !hasExplicitDefaultSubscription) {
          this.queriersForSubscription(result, null, options.globalParameters, params.bucketIdTransformer);
        }
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

    try {
      const querier = this.variant.querier(this.stream, reason, params, bucketIdTransformer);
      if (querier) {
        result.queriers.push(querier);
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

export class SyncStreamParameterLookupSource implements BucketParameterLookupSourceDefinition {
  constructor(
    private stream: SyncStream,
    private variant: StreamVariant
  ) {}

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (const subquery of this.variant.subqueries) {
      result.add(subquery.parameterTable);
    }

    return result;
  }

  createParameterLookupSource(params: CreateSourceParams): BucketParameterLookupSource {
    return {
      evaluateParameterRow: (sourceTable, row) => {
        const result: EvaluatedParametersResult[] = [];
        this.variant.pushParameterRowEvaluation(result, sourceTable, row);
        return result;
      }
    };
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (const subquery of this.variant.subqueries) {
      if (subquery.parameterTable.matches(table)) {
        return true;
      }
    }

    return false;
  }
}
