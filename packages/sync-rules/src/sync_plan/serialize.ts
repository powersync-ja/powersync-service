import { TablePattern } from '../TablePattern.js';
import {
  ColumnSource,
  ColumnSqlParameterValue,
  ExpandingLookup,
  ParameterValue,
  PartitionKey,
  RequestSqlParameterValue,
  SqlExpression,
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
        filters: source.filters,
        partition_by: source.parameters,
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
        partition_by: source.parameters,
        output: source.outputs
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
  hash: number;
  columns: ColumnSource[];
  filters: SqlExpression<ColumnSqlParameterValue>[];
  partition_by: PartitionKey[];
}

interface SerializedParameterIndexLookupCreator {
  table: SerializedTablePattern;
  hash: number;
  output: SqlExpression<ColumnSqlParameterValue>[];
  filters: SqlExpression<ColumnSqlParameterValue>[];
  partition_by: PartitionKey[];
}

interface SerializedStream {
  stream: StreamOptions;
  queriers: SerializedStreamQuerier[];
}

interface SerializedStreamQuerier {
  requestFilters: SqlExpression<RequestSqlParameterValue>[];
  lookupStages: any[][];
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
