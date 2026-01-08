import { StreamOptions } from '../compiler/bucket_resolver.js';
import { equalsIgnoringResultSet } from '../compiler/compatibility.js';
import { StableHasher } from '../compiler/equality.js';
import { ColumnInRow, ConnectionParameterSource, SyncExpression } from '../compiler/expression.js';
import { ColumnSource, StarColumnSource } from '../compiler/rows.js';
import { TablePattern } from '../TablePattern.js';
import {
  ExpandingLookup,
  ParameterValue,
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

  // Serialize an expression in a context that makes it obvious what result set ColumnInRow refers to.
  function serializeExpressionWithImpliedResultSet(expr: SyncExpression): SerializedSyncExpression {
    return {
      hash: StableHasher.hashWith(equalsIgnoringResultSet, expr),
      sql: expr.sql,
      instantiation: expr.instantiation.map((e) => {
        if (e instanceof ColumnInRow) {
          return { column: e.column };
        } else {
          return { connection: e.source };
        }
      })
    };
  }

  function serializeColumnSource(source: ColumnSource): SerializedColumnSource {
    if (source instanceof StarColumnSource) {
      return 'star';
    } else {
      return {
        expr: serializeExpressionWithImpliedResultSet(source.expression.expression),
        alias: source.alias ?? null
      };
    }
  }

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
        filters: source.filters.map(serializeExpressionWithImpliedResultSet),
        partition_by: source.parameters.map(serializeExpressionWithImpliedResultSet),
        columns: source.columns.map(serializeColumnSource)
      };
    });
  }

  function serializeParameterIndexes(): SerializedParameterIndexLookupCreator[] {
    return plan.parameterIndexes.map((source, i) => {
      parameterIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        filters: source.filters.map(serializeExpressionWithImpliedResultSet),
        partition_by: source.parameters.map(serializeExpressionWithImpliedResultSet),
        output: source.outputs.map(serializeExpressionWithImpliedResultSet)
      };
    });
  }

  function serializeParameterValue(value: ParameterValue): SerializedParameterValue {
    if (value.type == 'request') {
      return { type: 'request', expr: serializeExpressionWithImpliedResultSet(value.expr) };
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
              functionInputs: e.functionInputs.map(serializeExpressionWithImpliedResultSet),
              outputs: e.outputs.map(serializeExpressionWithImpliedResultSet),
              filters: e.filters.map(serializeExpressionWithImpliedResultSet)
            };
          }

          expandingLookups.set(e, ref);
          return mapped;
        })
      );
    });

    return {
      stream: source.stream,
      requestFilters: source.requestFilters.map(serializeExpressionWithImpliedResultSet),
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

interface SerializedSyncExpression {
  hash: number;
  sql: string;
  instantiation: ({ column: string } | { connection: ConnectionParameterSource })[];
}

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
