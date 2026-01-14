import { BucketInclusionReason } from '../BucketDescription.js';
import { PendingQueriers, UnscopedParameterLookup } from '../BucketParameterQuerier.js';
import {
  BucketDataSource,
  BucketSource,
  BucketSourceType,
  CreateSourceParams,
  HydratedBucketSource,
  ParameterIndexLookupCreator
} from '../BucketSource.js';
import { ColumnDefinition } from '../ExpressionType.js';
import { ParameterLookupScope } from '../HydrationState.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { GetQuerierOptions, RequestedStream, SqlSyncRules } from '../SqlSyncRules.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluateRowOptions,
  idFromData,
  RequestParameters,
  SourceSchema,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow,
  UnscopedEvaluatedParametersResult,
  UnscopedEvaluatedRow,
  UnscopedEvaluationResult
} from '../types.js';
import { filterJsonRow, isJsonValue, JSONBucketNameSerialize } from '../utils.js';
import { isValidParameterValue, isValidParameterValueRow, RequestParameterEvaluators } from './lookup_stages.js';
import * as plan from './plan.js';
import { PreparedQuerier, StreamInput } from './querier_impl.js';
import { prepareRowEvaluator, RowEvaluator, SqlBuilder, SqlEngine } from './sql_engine.js';

export interface StreamEvaluationContext {
  engine: SqlEngine;
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

class PreparedStreamDataSource {
  readonly tablePattern: TablePattern;
  private readonly outputs: ('star' | { index: number; alias: string })[] = [];
  private readonly evaluator: RowEvaluator;
  private readonly fixedOutputTableName?: string;

  constructor(evaluator: plan.StreamDataSource, { engine }: StreamEvaluationContext) {
    const outputExpressions: plan.SqlExpression<plan.ColumnSqlParameterValue>[] = [];
    for (const column of evaluator.columns) {
      if (column === 'star') {
        this.outputs.push('star');
      } else {
        const expressionIndex = outputExpressions.length;
        outputExpressions.push(column.expr);
        this.outputs.push({ index: expressionIndex, alias: column.alias ?? column.expr.sql });
      }
    }

    this.evaluator = prepareRowEvaluator(engine, outputExpressions, evaluator.filters, evaluator.parameters);
    this.fixedOutputTableName = evaluator.outputTableName;
    this.tablePattern = evaluator.sourceTable;
  }

  evaluateRow(options: EvaluateRowOptions, results: UnscopedEvaluationResult[]) {
    try {
      row: for (const source of this.evaluator.evaluate(options.record)) {
        const record: SqliteJsonRow = {};
        for (const output of this.outputs) {
          if (output === 'star') {
            Object.assign(record, filterJsonRow(options.record));
          } else {
            const value = source.outputs[output.index];
            if (isJsonValue(value)) {
              record[output.alias] = value;
            }
          }
        }
        const id = idFromData(record);

        for (const bucketParameter of source.partitionValues) {
          if (!isValidParameterValue(bucketParameter)) {
            continue row;
          }
        }

        results.push({
          id,
          data: record,
          table: this.fixedOutputTableName ?? options.sourceTable.name,
          serializedBucketParameters: JSONBucketNameSerialize.stringify(source.partitionValues)
        } satisfies UnscopedEvaluatedRow);
      }

      return results;
    } catch (e) {
      return results.push({ error: e.message });
    }
  }
}

class PreparedStreamBucketDataSource implements BucketDataSource {
  private readonly sourceTables = new Set<TablePattern>();
  private readonly sources: PreparedStreamDataSource[] = [];

  constructor(
    readonly source: plan.StreamBucketDataSource,
    context: StreamEvaluationContext
  ) {
    for (const data of source.sources) {
      const prepared = new PreparedStreamDataSource(data, context);

      this.sources.push(prepared);
      this.sourceTables.add(prepared.tablePattern);
    }
  }

  get uniqueName(): string {
    return this.source.uniqueName;
  }

  get bucketParameters(): string[] {
    // We can pick an arbitrary evaluator within the source, since they're all guaranteed to have the same parameters.
    const evaluator = this.source.sources[0];

    // It doesn't matter what we return here because it's for debugging purposes only. We at least want to get the
    // amount of array elements right.
    return evaluator.parameters.map((p) => p.expr.sql);
  }

  getSourceTables(): Set<TablePattern> {
    return this.sourceTables;
  }

  private *sourcesForTable(table: SourceTableInterface) {
    for (const source of this.sources) {
      if (source.tablePattern.matches(table)) {
        yield source;
      }
    }
  }

  tableSyncsData(table: SourceTableInterface): boolean {
    return !this.sourcesForTable(table).next().done;
  }

  evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[] {
    const results: UnscopedEvaluationResult[] = [];
    for (const source of this.sourcesForTable(options.sourceTable)) {
      source.evaluateRow(options, results);
    }

    return results;
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    throw new Error('resolveResultSets not implemented.');
  }

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void {
    throw new Error('debugWriteOutputTables not implemented.');
  }
}

class PreparedParameterIndexLookupCreator implements ParameterIndexLookupCreator {
  readonly defaultLookupScope: ParameterLookupScope;
  private readonly evaluator: RowEvaluator;

  constructor(
    private readonly source: plan.StreamParameterIndexLookupCreator,
    context: StreamEvaluationContext
  ) {
    this.defaultLookupScope = source.defaultLookupScope;
    this.evaluator = prepareRowEvaluator(context.engine, source.outputs, source.filters, source.parameters);
  }

  getSourceTables(): Set<TablePattern> {
    const set = new Set<TablePattern>();
    set.add(this.source.sourceTable);
    return set;
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): UnscopedEvaluatedParametersResult[] {
    const results: UnscopedEvaluatedParametersResult[] = [];
    if (!this.source.sourceTable.matches(sourceTable)) {
      return results;
    }

    try {
      for (const outputRow of this.evaluator.evaluate(row)) {
        if (!isValidParameterValueRow(outputRow.outputs) || !isValidParameterValueRow(outputRow.partitionValues)) {
          continue;
        }

        const bucketParameters: Record<string, SqliteJsonValue> = {};
        for (let i = 0; i < outputRow.outputs.length; i++) {
          const value = outputRow.outputs[i];
          bucketParameters[i.toString()] = value;
        }

        const lookup = UnscopedParameterLookup.normalized(outputRow.partitionValues);
        results.push({ lookup, bucketParameters: [bucketParameters] });
      }
    } catch (e) {
      results.push({ error: e.message });
    }

    return results;
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    return this.source.sourceTable.matches(table);
  }
}

class StreamBucketSource implements BucketSource {
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
    throw new Error('debugRepresentation not implemented.');
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
        const activeQueriers: { querier: PreparedQuerier; partialInstantiation: RequestParameterEvaluators }[] = [];
        for (const querier of queriers) {
          const partialInstantiation = querier.partialInstantiationForGlobalRequestdata(result, options);
          if (partialInstantiation == null) {
            // Nothing in this request can fullfil the querier, continue
            continue;
          }

          activeQueriers.push({ querier, partialInstantiation });
        }

        for (const subscription of subscriptions) {
          let subscriptionParams = options.globalParameters;
          if (subscription.parameters != null) {
            subscriptionParams = subscriptionParams.withAddedStreamParameters(subscription.parameters);
          } else {
            hasExplicitDefaultSubscription = true;
          }

          for (const { querier, partialInstantiation } of activeQueriers) {
            querier.querierForSubscription(params, result, subscriptionParams, subscription, partialInstantiation);
          }
        }

        // If the stream is subscribed to by default and there is no explicit subscription that would match the default
        // subscription, also include the default querier.
        if (this.subscribedToByDefault && !hasExplicitDefaultSubscription) {
          for (const { querier, partialInstantiation } of activeQueriers) {
            querier.querierForSubscription(params, result, options.globalParameters, null, partialInstantiation);
          }
        }
      }
    };
  }
}
