import type { EventDefinitionId } from '../events/EventDescriptor.js';
import { ParameterLookupDefinitionId } from '../HydrationState.js';
import { ImplicitSchemaTablePattern, TablePattern } from '../TablePattern.js';
import { SqlExpression } from './expression.js';
import { MapSourceVisitor, visitExpr } from './expression_visitor.js';
import {
  ColumnSource,
  ColumnSqlParameterValue,
  CompiledEventDescriptor,
  CompiledSyncStream,
  EvaluateTableValuedFunction,
  EventRowEvaluator,
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
import { serializedEventDefinitionId } from './plan_equality_serialized.js';

function createTableProcessorSerializer() {
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

  function serializeEventRowEvaluator(source: EventRowEvaluator): SerializedEventRowEvaluator {
    return {
      hash: source.hashCode,
      table: serializeTablePattern(source.sourceTable),
      tableValuedFunctions: serializeTableValued(source),
      filters: source.filters.map(serializeTableProcessorDataExpr),
      partitionBy: translateParameters(source),
      columns: source.columns.map((column): SerializedColumnSource => {
        if (column == 'star') {
          return 'star';
        }

        return { expr: serializeTableProcessorDataExpr(column.expr), alias: column.alias };
      })
    };
  }

  function serializeEventDefinition(
    event: Pick<CompiledEventDescriptor, 'name' | 'sourceQueries'>
  ): Omit<SerializedEventDescriptor, 'id'> {
    return {
      name: event.name,
      sourceQueries: event.sourceQueries.map((query) => ({
        sql: query.sql,
        table: serializeTablePattern(query.sourceTable),
        variants: query.variants.map(serializeEventRowEvaluator)
      }))
    };
  }

  function serializeEvent(event: CompiledEventDescriptor): SerializedEventDescriptor {
    return { id: event.id, ...serializeEventDefinition(event) };
  }

  return {
    get usesRowMetadataSqlValue() {
      return usesRowMetadataSqlValue;
    },
    serializeTableProcessorDataExpr,
    serializeTablePattern,
    serializeTableValued,
    translateParameters,
    serializeEventDefinition,
    serializeEvent
  };
}

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
  const tableProcessorSerializer = createTableProcessorSerializer();
  const { serializeTableProcessorDataExpr, serializeTablePattern, serializeTableValued, translateParameters } =
    tableProcessorSerializer;

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
        output: source.outputs.map(serializeTableProcessorDataExpr),
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

  const events = plan.events.map(tableProcessorSerializer.serializeEvent);
  const serialized: SerializedSyncPlan = {
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
    version: tableProcessorSerializer.usesRowMetadataSqlValue ? 2 : 1
  };

  // Compiled events are intentionally additive to plan versions 1 and 2. The service also persists their raw SQL in
  // the legacy eventDescriptors field, so older readers can ignore this field and retain equivalent event behavior.
  if (events.length != 0) {
    serialized.events = events;
  }

  return serialized;
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

  function deserializeEventRowEvaluator(source: SerializedEventRowEvaluator): EventRowEvaluator {
    const functions = (tableValuedFunctionsInScope = source.tableValuedFunctions);

    return {
      hashCode: source.hash,
      sourceTable: deserializeTablePattern(source.table),
      tableValuedFunctions: functions,
      filters: source.filters.map(deserializeTableProcessorDataExpr),
      parameters: deserializeParameters(source.partitionBy),
      columns: source.columns.map((column): ColumnSource => {
        if (column == 'star') {
          return 'star';
        }

        return { expr: deserializeTableProcessorDataExpr(column.expr), alias: column.alias };
      })
    };
  }

  if (plan.events != null && !Array.isArray(plan.events)) {
    throw new Error('Compiled sync plan events must be an array.');
  }
  const serializedEvents = plan.events ?? [];
  const events = serializedEvents.map((event): CompiledEventDescriptor => {
    return {
      id: event.id,
      name: event.name,
      sourceQueries: event.sourceQueries.map((query) => ({
        sql: query.sql,
        sourceTable: deserializeTablePattern(query.table),
        variants: query.variants.map(deserializeEventRowEvaluator)
      }))
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
    streams,
    events
  };
}

/** Derive the ID assigned while finalizing a compiled event definition. */
export function compiledEventDefinitionId(
  event: Pick<CompiledEventDescriptor, 'name' | 'sourceQueries'>
): EventDefinitionId {
  const definition = createTableProcessorSerializer().serializeEventDefinition(event);
  return serializedEventDefinitionId(definition);
}

/** Serialize a single compiled event using the exact representation persisted in a sync plan. */
export function serializeEventDescriptor(event: CompiledEventDescriptor): SerializedEventDescriptor {
  return createTableProcessorSerializer().serializeEvent(event);
}

/**
 * Changes to {@link SerializedSyncPlan} require a version bump when older services would interpret the plan
 * incorrectly. Optional additive fields are only safe without a bump when older readers can ignore them while another
 * persisted representation preserves equivalent behavior.
 *
 * Compiled `events` are an explicit additive exception: service-core continues to persist raw event SQL alongside the
 * plan for the legacy evaluator. Older readers ignore `events` and use that legacy mirror. Removing the mirror or
 * relying on compiled-only event semantics will require a version bump.
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
  /**
   * Optional additive compiled event definitions. Older readers safely ignore this because service-core dual-writes
   * equivalent raw SQL in its legacy `eventDescriptors` field.
   */
  events?: SerializedEventDescriptor[];
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

export interface SerializedEventDescriptor {
  /** Content-addressed identity derived from the rest of this event definition. */
  id: EventDefinitionId;
  name: string;
  sourceQueries: SerializedEventSourceQuery[];
}

export interface SerializedEventSourceQuery {
  /** Raw SQL retained for the legacy compatibility mirror and as part of the exact serialized event definition. */
  sql: string;
  table: SerializedTablePattern;
  variants: SerializedEventRowEvaluator[];
}

export interface SerializedEventRowEvaluator {
  table: SerializedTablePattern;
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
