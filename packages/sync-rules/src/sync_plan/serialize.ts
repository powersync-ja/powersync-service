import { ParameterLookupDefinitionId } from '../HydrationState.js';
import { ImplicitSchemaTablePattern, TablePattern } from '../TablePattern.js';
import { SqlExpression } from './expression.js';
import { MapSourceVisitor, visitExpr } from './expression_visitor.js';
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
  RowMetadataSqlValue,
  StreamBucketDataSource,
  StreamDataSource,
  StreamOptions,
  StreamParameterIndexLookupCreator,
  StreamQuerier,
  SyncPlan,
  TableProcessor,
  TableProcessorData,
  TableProcessorTableValuedFunction,
  TableProcessorTableValuedFunctionOutput
} from './plan.js';

/**
 * Serializes a sync plan into a simple JSON object.
 *
 * While {@link SyncPlan}s are already serializable for the most part, it contains a graph of references from e.g.
 * queriers to bucket creators. To represent this efficiently, we assign numbers to referenced elements while
 * serializing instead of duplicating definitions.
 */
export function serializeSyncPlan(plan: SyncPlan): SerializedSyncPlan {
  const dataSourceIndex = new Map<StreamDataSource, number>();
  const bucketIndex = new Map<StreamBucketDataSource, number>();
  const parameterIndex = new Map<StreamParameterIndexLookupCreator, number>();
  const expandingLookups = new Map<ExpandingLookup, LookupReference>();
  const addedTableValuedFunctions = new Map<TableProcessorTableValuedFunction, number>();
  let usesRowMetadataSqlValue = false;

  const replaceFunctionReferenceWithIndex = new MapSourceVisitor<
    ColumnSqlParameterValue | RowMetadataSqlValue | TableProcessorTableValuedFunctionOutput,
    ColumnSqlParameterValue | RowMetadataSqlValue | SerializedTableProcessorTableValuedFunctionOutput
  >((value) => {
    usesRowMetadataSqlValue ||= 'metadata' in value;

    if ('function' in value) {
      return { function: addedTableValuedFunctions.get(value.function)!, outputName: value.outputName };
    } else {
      return value;
    }
  });

  function serializeTableProcessorDataExpr(
    expr: SqlExpression<TableProcessorData>
  ): SqlExpression<SerializedTableProcessorData> {
    return visitExpr(replaceFunctionReferenceWithIndex, expr, null);
  }

  function serializeTablePattern(pattern: ImplicitSchemaTablePattern): SerializedTablePattern {
    return {
      connection: pattern.connectionTag,
      schema: pattern.schema,
      table: pattern.tablePattern
    };
  }

  function serializeTableValued(source: TableProcessor): TableProcessorTableValuedFunction[] {
    return source.tableValuedFunctions.map((fn, i) => {
      addedTableValuedFunctions.set(fn, i);

      return {
        functionName: fn.functionName,
        functionInputs: fn.functionInputs.map(
          // Since we don't support table-valued functions as inputs to other table-valued functions, this doesn't
          // change expressions. It ensures we track row metadata use in any input, though.
          (s) => serializeTableProcessorDataExpr(s) as SqlExpression<ColumnSqlParameterValue>
        )
      };
    });
  }

  function translateParameters(source: TableProcessor): SerializedPartitionKey[] {
    return source.parameters.map((key) => {
      return { expr: serializeTableProcessorDataExpr(key.expr) };
    });
  }

  function serializeDataSources(): SerializedDataSource[] {
    return plan.dataSources.map((source, i) => {
      dataSourceIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        outputTableName: source.outputTableName,
        tableValuedFunctions: serializeTableValued(source),
        filters: source.filters.map(serializeTableProcessorDataExpr),
        partitionBy: translateParameters(source),
        columns: source.columns.map((c): SerializedColumnSource => {
          if (c == 'star') {
            return 'star';
          } else {
            return { expr: serializeTableProcessorDataExpr(c.expr), alias: c.alias };
          }
        })
      } satisfies SerializedDataSource;
    });
  }

  function serializeParameterIndexes(): SerializedParameterIndexLookupCreator[] {
    return plan.parameterIndexes.map((source, i) => {
      parameterIndex.set(source, i);

      return {
        hash: source.hashCode,
        table: serializeTablePattern(source.sourceTable),
        tableValuedFunctions: serializeTableValued(source),
        filters: source.filters.map(serializeTableProcessorDataExpr),
        partitionBy: translateParameters(source),
        output: source.outputs.map((out) => visitExpr(replaceFunctionReferenceWithIndex, out, null)),
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
    })),
    version: usesRowMetadataSqlValue ? 2 : 1
  };
}

export function deserializeSyncPlan(serialized: unknown): SyncPlan {
  const { version } = serialized as SerializedSyncPlan;
  if (version < 1) {
    throw new Error('Unknown sync plan version passed to deserializeSyncPlan()');
  }
  if (version > maxSupportedSyncPlanVersion) {
    throw new Error(
      `Encountered a sync plan with version ${version}, the maximum supported version is ${maxSupportedSyncPlanVersion}. This can happen when the PowerSync service version is downgraded after deploying Sync Streams, consider upgrading or re-deploying.`
    );
  }

  function deserializeTablePattern(pattern: SerializedTablePattern): ImplicitSchemaTablePattern {
    if (pattern.schema) {
      return new TablePattern(`${pattern.connection}.${pattern.schema}`, pattern.table);
    } else {
      return new ImplicitSchemaTablePattern(null, pattern.table);
    }
  }

  let tableValuedFunctionsInScope: TableProcessorTableValuedFunction[] = [];

  const replaceFunctionIndexWithReference = new MapSourceVisitor<
    ColumnSqlParameterValue | RowMetadataSqlValue | SerializedTableProcessorTableValuedFunctionOutput,
    ColumnSqlParameterValue | RowMetadataSqlValue | TableProcessorTableValuedFunctionOutput
  >((value) => {
    if ('function' in value) {
      return { function: tableValuedFunctionsInScope[value.function], outputName: value.outputName };
    } else {
      return value;
    }
  });

  function deserializeTableProcessorDataExpr(
    expr: SqlExpression<SerializedTableProcessorData>
  ): SqlExpression<TableProcessorData> {
    return visitExpr(replaceFunctionIndexWithReference, expr, null);
  }

  function deserializeParameters(source: SerializedPartitionKey[]): PartitionKey[] {
    return source.map((serializedKey) => {
      return { expr: deserializeTableProcessorDataExpr(serializedKey.expr) };
    });
  }

  const plan = serialized as SerializedSyncPlan;
  const dataSources = plan.dataSources.map((source): StreamDataSource => {
    const functions = (tableValuedFunctionsInScope = source.tableValuedFunctions);

    return {
      hashCode: source.hash,
      sourceTable: deserializeTablePattern(source.table),
      tableValuedFunctions: functions,
      outputTableName: source.outputTableName,
      filters: source.filters.map(deserializeTableProcessorDataExpr),
      parameters: deserializeParameters(source.partitionBy),
      columns: source.columns.map((c): ColumnSource => {
        if (c == 'star') {
          return 'star';
        } else {
          return { expr: deserializeTableProcessorDataExpr(c.expr), alias: c.alias };
        }
      })
    };
  });
  const buckets = plan.buckets.map((bkt): StreamBucketDataSource => {
    return {
      hashCode: bkt.hash,
      uniqueName: bkt.uniqueName,
      sources: bkt.sources.map((idx) => dataSources[idx])
    };
  });
  const parameterIndexes = plan.parameterIndexes.map((source): StreamParameterIndexLookupCreator => {
    const functions = (tableValuedFunctionsInScope = source.tableValuedFunctions);

    return {
      hashCode: source.hash,
      sourceTable: deserializeTablePattern(source.table),
      tableValuedFunctions: functions,
      filters: source.filters.map(deserializeTableProcessorDataExpr),
      parameters: deserializeParameters(source.partitionBy),
      outputs: source.output.map((out) => visitExpr(replaceFunctionIndexWithReference, out, null)),
      defaultLookupScope: source.lookupScope
    };
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
        } satisfies EvaluateTableValuedFunction<RequestSqlParameterValue>;
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

/**
 * Every change to the format of {@link SerializedSyncPlan} needs a version bump and a changelog entry in this
 * documentation comment. Even for seemingly backward-compatible changes, like adding new fields, older services would
 * be unaware of them and thus interpret the sync plan incorrectly. Increasing this version ensures that older services
 * wouldn't even try to deserialize sync plans.
 *
 * ### Version 2
 *
 * - Add {@link RowMetadataSqlValue} to data for row and parameter evaluators, exposing the exact table and schema name
 *   when matching on wildcard table patterns.
 *   The deserialization logic can remain the same for v1 and v2.
 *
 * ### Version 1
 *
 * - Initial version
 */
export type SerializedSyncPlanVersion = 1 | 2;

export const maxSupportedSyncPlanVersion: SerializedSyncPlanVersion = 2;

export interface SerializedSyncPlan {
  version: SerializedSyncPlanVersion;
  dataSources: SerializedDataSource[];
  buckets: SerializedBucketDataSource[];
  parameterIndexes: SerializedParameterIndexLookupCreator[];
  streams: SerializedStream[];
}

export interface SerializedBucketDataSource {
  hash: number;
  uniqueName: string;
  sources: number[];
}

export interface SerializedTablePattern {
  connection: string | null;
  schema: string | null;
  table: string;
}

export interface SerializedTableProcessorTableValuedFunctionOutput {
  function: number;
  outputName: string;
}

export type SerializedTableProcessorData =
  | ColumnSqlParameterValue
  | RowMetadataSqlValue
  | SerializedTableProcessorTableValuedFunctionOutput;

export interface SerializedPartitionKey {
  expr: SqlExpression<SerializedTableProcessorData>;
}

export type SerializedColumnSource = 'star' | { expr: SqlExpression<SerializedTableProcessorData>; alias: string };

export interface SerializedDataSource {
  table: SerializedTablePattern;
  outputTableName?: string;
  hash: number;
  columns: SerializedColumnSource[];
  filters: SqlExpression<SerializedTableProcessorData>[];
  tableValuedFunctions: TableProcessorTableValuedFunction[];
  partitionBy: SerializedPartitionKey[];
}

export interface SerializedParameterIndexLookupCreator {
  table: SerializedTablePattern;
  hash: number;
  lookupScope: ParameterLookupDefinitionId;
  output: SqlExpression<SerializedTableProcessorData>[];
  filters: SqlExpression<SerializedTableProcessorData>[];
  tableValuedFunctions: TableProcessorTableValuedFunction[];
  partitionBy: SerializedPartitionKey[];
}

export interface SerializedStream {
  stream: StreamOptions;
  queriers: SerializedStreamQuerier[];
}

export interface SerializedStreamQuerier {
  requestFilters: SqlExpression<RequestSqlParameterValue>[];
  lookupStages: SerializedExpandingLookup[][];
  bucket: number;
  sourceInstantiation: SerializedParameterValue[];
}

export type SerializedExpandingLookup =
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

export interface LookupReference {
  stageId: number;
  idInStage: number;
}

export type SerializedParameterValue =
  | { type: 'request'; expr: SqlExpression<RequestSqlParameterValue> }
  | { type: 'lookup'; lookup: LookupReference; resultIndex: number }
  | { type: 'intersection'; values: SerializedParameterValue[] };
