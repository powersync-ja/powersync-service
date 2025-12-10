import { BaseSqlDataQuery } from '../BaseSqlDataQuery.js';
import { BucketInclusionReason, BucketPriority, DEFAULT_BUCKET_PRIORITY } from '../BucketDescription.js';
import { BucketParameterQuerier, PendingQueriers } from '../BucketParameterQuerier.js';
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

  public readonly dataSource: BucketDataSourceDefinition;
  public readonly parameterLookupSources: BucketParameterLookupSourceDefinition[];
  public readonly parameterQuerierSources: BucketParameterQuerierSourceDefinition[];

  constructor(name: string, data: BaseSqlDataQuery) {
    this.name = name;
    this.subscribedToByDefault = false;
    this.priority = DEFAULT_BUCKET_PRIORITY;
    this.variants = [];
    this.data = data;

    this.dataSource = new SyncStreamDataSource(this, data);
    this.parameterQuerierSources = [new SyncStreamParameterQuerierSource(this)];
    this.parameterLookupSources = [new SyncStreamParameterLookupSource(this)];
  }

  public get type(): BucketSourceType {
    return BucketSourceType.SYNC_STREAM;
  }
}

export class SyncStreamDataSource implements BucketDataSourceDefinition {
  constructor(
    private stream: SyncStream,
    private data: BaseSqlDataQuery
  ) {}

  get bucketParameters() {
    // FIXME: check whether this is correct.
    // Could there be multiple variants with different bucket parameters?
    return this.data.bucketParameters;
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
      definition: this,

      evaluateRow: (options: EvaluateRowOptions): EvaluationResult[] => {
        if (!this.data.applies(options.sourceTable)) {
          return [];
        }

        const stream = this.stream;
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
}

export class SyncStreamParameterQuerierSource implements BucketParameterQuerierSourceDefinition {
  // We could eventually split this into a separate source per variant.

  constructor(private stream: SyncStream) {}

  get bucketParameters(): string[] {
    // FIXME: check whether this is correct.
    // Could there be multiple variants with different bucket parameters?
    return this.stream.data.bucketParameters;
  }

  createParameterQuerierSource(params: CreateSourceParams): BucketParameterQuerierSource {
    const stream = this.stream;
    return {
      definition: this,

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
    const queriers: BucketParameterQuerier[] = [];

    try {
      for (const variant of this.stream.variants) {
        const querier = variant.querier(this.stream, reason, params, bucketIdTransformer);
        if (querier) {
          queriers.push(querier);
        }
      }

      result.queriers.push(...queriers);
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
  // We could eventually split this into a separate source per variant.

  constructor(private stream: SyncStream) {}

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    for (let variant of this.stream.variants) {
      for (const subquery of variant.subqueries) {
        result.add(subquery.parameterTable);
      }
    }

    return result;
  }

  createParameterLookupSource(params: CreateSourceParams): BucketParameterLookupSource {
    return {
      definition: this,

      evaluateParameterRow: (sourceTable, row) => {
        const result: EvaluatedParametersResult[] = [];

        for (const variant of this.stream.variants) {
          variant.pushParameterRowEvaluation(result, sourceTable, row);
        }

        return result;
      }
    };
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    for (const variant of this.stream.variants) {
      for (const subquery of variant.subqueries) {
        if (subquery.parameterTable.matches(table)) {
          return true;
        }
      }
    }

    return false;
  }
}
