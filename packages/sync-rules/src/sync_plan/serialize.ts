import { StreamOptions } from '../compiler/bucket_resolver.js';
import { ColumnSource, StarColumnSource } from '../compiler/rows.js';
import { TablePattern } from '../TablePattern.js';
import {
  ExpandingLookup,
  ParameterValue,
  SqlExpression,
  StreamBucketDataSource,
  StreamDataSource,
  StreamParameterIndexLookupCreator,
  StreamQuerier,
  SyncPlan
} from './plan.js';

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
      };
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
      };
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
      stream: source.stream,
      requestFilters: source.requestFilters,
      lookupStages: stages,
      bucket: bucketIndex.get(source.bucket)!,
      sourceInstantiation: source.sourceInstantiation.map(serializeParameterValue)
    };
  }

  return {
    version: 'unstable', // TODO: Mature to 1 before storing in bucket storage
    data: serializeDataSources(),
    buckets: plan.buckets.map((bkt, index) => {
      bucketIndex.set(bkt, index);
      return {
        hash: bkt.hashCode,
        uniqueName: bkt.uniqueName,
        sources: bkt.sources.map((e) => dataSourceIndex.get(e)!)
      };
    }),
    parameterIndexes: serializeParameterIndexes(),
    queriers: plan.queriers.map(serializeStreamQuerier)
  };
}

interface SerializedSyncPlanUnstable {
  version: 'unstable';
  data: SerializedDataSource[];
  buckets: SerializedBucketDataSource[];
  parameterIndexes: SerializedParameterIndexLookupCreator[];
  queriers: SerializedStreamQuerier[];
}

interface SerializedBucketDataSource {
  hash: number;
  uniqueName: string;
  sources: number[];
}

type SerializedSyncExpression = SqlExpression;

type SerializedColumnSource = 'star' | { expr: SerializedSyncExpression; alias: string | null };

interface SerializedTablePattern {
  connection: string;
  schema: string;
  table: string;
}

interface SerializedDataSource {
  table: SerializedTablePattern;
  hash: number;
  columns: SerializedColumnSource[];
  filters: SerializedSyncExpression[];
  partition_by: SerializedSyncExpression[];
}

interface SerializedParameterIndexLookupCreator {
  table: SerializedTablePattern;
  hash: number;
  output: SerializedSyncExpression[];
  filters: SerializedSyncExpression[];
  partition_by: SerializedSyncExpression[];
}

interface SerializedStreamQuerier {
  stream: StreamOptions;
  requestFilters: SerializedSyncExpression[];
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
      functionInputs: SerializedSyncExpression[];
      outputs: SerializedSyncExpression[];
      filters: SerializedSyncExpression[];
    };

interface LookupReference {
  stageId: number;
  idInStage: number;
}

type SerializedParameterValue =
  | { type: 'request'; expr: SerializedSyncExpression }
  | { type: 'lookup'; lookup: LookupReference; resultIndex: number }
  | { type: 'intersection'; values: SerializedParameterValue[] };
