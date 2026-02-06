import { ParameterLookupScope } from '../HydrationState.js';
import { TablePattern } from '../TablePattern.js';
import { SqlExpression } from './expression.js';
import {
  ColumnSource,
  ColumnSqlParameterValue,
  CompiledSyncStream,
  EvaluateTableValuedFunction,
  ExpandingLookup,
  ParameterLookup,
  ParameterValue,
  PartitionKey,
  RequestSqlParameterValue,
  StreamBucketDataSource,
  StreamDataSource,
  StreamOptions,
  StreamParameterIndexLookupCreator,
  StreamQuerier,
  SyncPlan
} from './plan.js';

/**
 * Serializes a sync plan into a simple JSON object.
 *
 * While {@link SyncPlan}s are already serializable for the most part, it contains a graph of references from e.g.
 * queriers to bucket creators. To represent this efficiently, we assign numbers to referenced elements while
 * serializing instead of duplicating definitions.
 */
export function serializeSyncPlan(plan: SyncPlan): SerializedSyncPlanUnstable {
  const dataSourceIndex = new Map<StreamDataSource, number>();
  const bucketIndex = new Map<StreamBucketDataSource, number>();
  const parameterIndex = new Map<StreamParameterIndexLookupCreator, number>();
  const expandingLookups = new Map<ExpandingLookup, LookupReference>();

  function serializeTablePattern(pattern: TablePattern): SerializedTablePattern {
    return {
      connection: pattern.connectionTag,
      schema: pattern.schema,
      table: pattern.tablePattern
    };
  }

  function serializeDataSources(): SerializedDataSource[] {
    return plan.dataSources.map((source, i) => {
      dataSourceIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        outputTableName: source.outputTableName,
        filters: source.filters,
        partitionBy: source.parameters,
        columns: source.columns
      } satisfies SerializedDataSource;
    });
  }

  function serializeParameterIndexes(): SerializedParameterIndexLookupCreator[] {
    return plan.parameterIndexes.map((source, i) => {
      parameterIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        filters: source.filters,
        partitionBy: source.parameters,
        output: source.outputs,
        lookupScope: source.defaultLookupScope
      } satisfies SerializedParameterIndexLookupCreator;
    });
  }

  function serializeParameterValue(value: ParameterValue): SerializedParameterValue {
    if (value.type == 'request') {
      return { type: 'request', expr: value.expr };
    } else if (value.type == 'lookup') {
      return { type: 'lookup', lookup: expandingLookups.get(value.lookup)!, resultIndex: value.resultIndex };
    } else {
      return { type: 'intersection', values: value.values.map(serializeParameterValue) };
    }
  }

  function serializeStreamQuerier(source: StreamQuerier): SerializedStreamQuerier {
    const stages: SerializedExpandingLookup[][] = [];

    source.lookupStages.map((stage, stageIndex) => {
      stages.push(
        stage.map((e, indexInStage) => {
          const ref: LookupReference = {
            stageId: stageIndex,
            idInStage: indexInStage
          };
          let mapped: SerializedExpandingLookup;

          if (e.type == 'parameter') {
            mapped = {
              type: 'parameter',
              lookup: parameterIndex.get(e.lookup)!,
              instantiation: e.instantiation.map(serializeParameterValue)
            };
          } else {
            mapped = {
              type: 'table_valued',
              functionName: e.functionName,
              functionInputs: e.functionInputs,
              outputs: e.outputs,
              filters: e.filters
            };
          }

          expandingLookups.set(e, ref);
          return mapped;
        })
      );
    });

    return {
      requestFilters: source.requestFilters,
      lookupStages: stages,
      bucket: bucketIndex.get(source.bucket)!,
      sourceInstantiation: source.sourceInstantiation.map(serializeParameterValue)
    };
  }

  return {
    version: 'unstable', // TODO: Mature to 1 before storing in bucket storage
    dataSources: serializeDataSources(),
    buckets: plan.buckets.map((bkt, index) => {
      bucketIndex.set(bkt, index);
      return {
        hash: bkt.hashCode,
        uniqueName: bkt.uniqueName,
        sources: bkt.sources.map((e) => dataSourceIndex.get(e)!)
      };
    }),
    parameterIndexes: serializeParameterIndexes(),
    streams: plan.streams.map((s) => ({
      stream: s.stream,
      queriers: s.queriers.map(serializeStreamQuerier)
    }))
  };
}

export function deserializeSyncPlan(serialized: unknown): SyncPlan {
  // TODO: Mature to version 1
  if ((serialized as SerializedSyncPlanUnstable).version != 'unstable') {
    throw new Error('Unknown sync plan version passed to deserializeSyncPlan()');
  }

  function deserializeTablePattern(pattern: SerializedTablePattern): TablePattern {
    return new TablePattern(`${pattern.connection}.${pattern.schema}`, pattern.table);
  }

  const plan = serialized as SerializedSyncPlanUnstable;
  const dataSources = plan.dataSources.map((source) => {
    return {
      hashCode: source.hash,
      sourceTable: deserializeTablePattern(source.table),
      outputTableName: source.outputTableName,
      filters: source.filters,
      parameters: source.partitionBy,
      columns: source.columns
    } satisfies StreamDataSource;
  });
  const buckets = plan.buckets.map((bkt) => {
    return {
      hashCode: bkt.hash,
      uniqueName: bkt.uniqueName,
      sources: bkt.sources.map((idx) => dataSources[idx])
    } satisfies StreamBucketDataSource;
  });
  const parameterIndexes = plan.parameterIndexes.map((source) => {
    return {
      hashCode: source.hash,
      sourceTable: deserializeTablePattern(source.table),
      filters: source.filters,
      parameters: source.partitionBy,
      outputs: source.output,
      defaultLookupScope: source.lookupScope
    } satisfies StreamParameterIndexLookupCreator;
  });

  function deserializeParameterValue(stages: ExpandingLookup[][], value: SerializedParameterValue): ParameterValue {
    switch (value.type) {
      case 'request':
        return value;
      case 'lookup':
        return {
          type: 'lookup',
          lookup: stages[value.lookup.stageId][value.lookup.idInStage],
          resultIndex: value.resultIndex
        };
      case 'intersection':
        return { type: 'intersection', values: value.values.map((v) => deserializeParameterValue(stages, v)) };
    }
  }

  function deserializeExpandingLookup(stages: ExpandingLookup[][], source: SerializedExpandingLookup): ExpandingLookup {
    switch (source.type) {
      case 'parameter':
        return {
          type: 'parameter',
          lookup: parameterIndexes[source.lookup],
          instantiation: source.instantiation.map((v) => deserializeParameterValue(stages, v))
        } satisfies ParameterLookup;
      case 'table_valued':
        return {
          type: 'table_valued',
          functionName: source.functionName,
          functionInputs: source.functionInputs,
          outputs: source.outputs,
          filters: source.filters
        } satisfies EvaluateTableValuedFunction;
    }
  }

  function deserializeStreamQuerier(source: SerializedStreamQuerier): StreamQuerier {
    const lookupStages: ExpandingLookup[][] = [];
    for (const serializedStage of source.lookupStages) {
      const stage: ExpandingLookup[] = [];
      for (const serializedElement of serializedStage) {
        stage.push(deserializeExpandingLookup(lookupStages, serializedElement));
      }

      lookupStages.push(stage);
    }

    return {
      requestFilters: source.requestFilters,
      lookupStages,
      bucket: buckets[source.bucket],
      sourceInstantiation: source.sourceInstantiation.map((v) => deserializeParameterValue(lookupStages, v))
    };
  }

  const streams = plan.streams.map((source) => {
    return {
      stream: source.stream,
      queriers: source.queriers.map(deserializeStreamQuerier)
    } satisfies CompiledSyncStream;
  });

  return {
    dataSources,
    buckets,
    parameterIndexes,
    streams
  };
}

interface SerializedSyncPlanUnstable {
  version: 'unstable';
  dataSources: SerializedDataSource[];
  buckets: SerializedBucketDataSource[];
  parameterIndexes: SerializedParameterIndexLookupCreator[];
  streams: SerializedStream[];
}

interface SerializedBucketDataSource {
  hash: number;
  uniqueName: string;
  sources: number[];
}

interface SerializedTablePattern {
  connection: string;
  schema: string;
  table: string;
}

interface SerializedDataSource {
  table: SerializedTablePattern;
  outputTableName?: string;
  hash: number;
  columns: ColumnSource[];
  filters: SqlExpression<ColumnSqlParameterValue>[];
  partitionBy: PartitionKey[];
}

interface SerializedParameterIndexLookupCreator {
  table: SerializedTablePattern;
  hash: number;
  lookupScope: ParameterLookupScope;
  output: SqlExpression<ColumnSqlParameterValue>[];
  filters: SqlExpression<ColumnSqlParameterValue>[];
  partitionBy: PartitionKey[];
}

interface SerializedStream {
  stream: StreamOptions;
  queriers: SerializedStreamQuerier[];
}

interface SerializedStreamQuerier {
  requestFilters: SqlExpression<RequestSqlParameterValue>[];
  lookupStages: SerializedExpandingLookup[][];
  bucket: number;
  sourceInstantiation: SerializedParameterValue[];
}

type SerializedExpandingLookup =
  | {
      type: 'parameter';
      lookup: number;
      instantiation: SerializedParameterValue[];
    }
  | {
      type: 'table_valued';
      functionName: string;
      functionInputs: SqlExpression<RequestSqlParameterValue>[];
      outputs: SqlExpression<ColumnSqlParameterValue>[];
      filters: SqlExpression<ColumnSqlParameterValue>[];
    };

interface LookupReference {
  stageId: number;
  idInStage: number;
}

type SerializedParameterValue =
  | { type: 'request'; expr: SqlExpression<RequestSqlParameterValue> }
  | { type: 'lookup'; lookup: LookupReference; resultIndex: number }
  | { type: 'intersection'; values: SerializedParameterValue[] };
