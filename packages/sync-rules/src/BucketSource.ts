import {
  BucketParameterQuerier,
  PendingQueriers,
  ScopedParameterLookup,
  UnscopedParameterLookup
} from './BucketParameterQuerier.js';
import { ColumnDefinition } from './ExpressionType.js';
import { HydrationState, ParameterLookupDefinitionId } from './HydrationState.js';
import { SourceTableRef } from './SourceTableRef.js';
import { GetQuerierOptions } from './SqlSyncRules.js';
import { ScalarExpressionEngine } from './sync_plan/engine/scalar_expression_engine.js';
import { SQLite } from './sync_plan/engine/sqlite.js';
import { TablePattern } from './TablePattern.js';
import {
  EvaluatedParameters,
  EvaluatedParametersResult,
  EvaluatedRow,
  EvaluateRowOptions,
  EvaluationResult,
  isEvaluationError,
  SourceSchema,
  SqliteRow,
  UnscopedEvaluatedParametersResult,
  UnscopedEvaluationResult
} from './types.js';
import { withBucketSource } from './utils.js';

export interface CreateSourceParams {
  hydrationState: HydrationState;
}

export interface HydrateSyncConfigParams extends CreateSourceParams {
  /**
   * The SQLite engine to use to evaluate scalar expressions in Sync Streams.
   */
  sqlite: SQLite | null;
}

export interface HydrationInput extends CreateSourceParams {
  scalarExpressions: ScalarExpressionEngine;
}

/**
 * A BucketSource is a _logical_ bucket or sync stream definition. It is primarily used to group together
 * related BucketDataSource, BucketParameterLookupSource and BucketParameterQuerierSource definitions,
 * for the purpose of subscribing to specific streams. It does not directly define the implementation
 * or replication process.
 */
export interface BucketSource {
  readonly name: string;
  readonly type: BucketSourceType;
  readonly subscribedToByDefault: boolean;

  /**
   * BucketDataSource describing the data in this bucket/stream definition.
   *
   * The same data source could in theory be present in multiple stream definitions.
   *
   * Sources must _only_ be split into multiple ones if they will result in different buckets being created.
   * Specifically, bucket definitions would always have a single data source, while stream definitions may have
   * one per variant.
   */
  readonly dataSources: BucketDataSource[];

  /**
   * BucketParameterLookupSource describing the parameter tables used in this bucket/stream definition.
   *
   * The same source could in theory be present in multiple stream definitions.
   */
  readonly parameterIndexLookupCreators: ParameterIndexLookupCreator[];

  debugRepresentation(): any;

  hydrate(params: HydrationInput): HydratedBucketSource;
}

/**
 * Internal interface for individual queriers. This is not used on its in the public API directly, apart
 * from in HydratedBucketSource. Everywhere else it is just to standardize the internal functions that we re-use.
 */
export interface BucketParameterQuerierSource {
  /**
   * Reports {@link BucketParameterQuerier}s resolving buckets that a specific stream request should have access to.
   *
   * @param result The target array to insert queriers and errors into.
   * @param options Options, including parameters that may affect the buckets loaded by this source.
   */
  pushBucketParameterQueriers(result: PendingQueriers, options: GetQuerierOptions): void;
}

export interface HydratedBucketSource extends BucketParameterQuerierSource {
  readonly definition: BucketSource;
}

export type ScopedEvaluateRow = (options: EvaluateRowOptions) => EvaluationResult[];
export type ScopedEvaluateParameterRow = (sourceTable: SourceTableRef, row: SqliteRow) => EvaluatedParametersResult[];

/**
 * Encodes a static definition of a bucket source, as parsed from sync rules or stream definitions.
 *
 * This does not require any "hydration" itself: All results are independent of bucket names.
 * The higher-level HydratedSyncRules will use a HydrationState to generate bucket names.
 */
export interface BucketDataSource {
  /**
   * Unique name of the data source within a replication stream.
   *
   * This may be used as the basis for bucketPrefix (or it could be ignored).
   */
  readonly uniqueName: string;

  /**
   * For debug use only.
   */
  readonly bucketParameters: string[];

  getSourceTables(): Set<TablePattern>;
  tableSyncsData(table: SourceTableRef): boolean;

  createEvaluator(input: HydrationInput): BucketDataEvaluator;

  /**
   * Given a static schema, infer all logical tables and associated columns that appear in buckets defined by this
   * source.
   *
   * This is use to generate the client-side schema.
   */
  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void;

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void;
}

export interface BucketDataEvaluator {
  /**
   * Given a row as it appears in a table that affects sync data, return buckets, logical table names and transformed
   * data for rows to add to buckets.
   */
  evaluateRow(options: EvaluateRowOptions): UnscopedEvaluationResult[];
}

/**
 * This defines how to extract parameter index lookup values from parameter queries or stream subqueries.
 *
 * This is only relevant for parameter queries and subqueries that query tables.
 */
export interface ParameterIndexLookupCreator {
  /**
   * lookupName + queryId is used to uniquely identify parameter queries for parameter storage.
   *
   * The values here specifically identify the definition uniquely within a single SyncConfig.
   * It does not guarantee uniqueness across different SyncConfigs.
   */
  readonly sourceId: ParameterLookupDefinitionId;

  getSourceTables(): Set<TablePattern>;

  /** Whether the table possibly affects the buckets resolved by this source. */
  tableSyncsParameters(table: SourceTableRef): boolean;

  createEvaluator(input: HydrationInput): ParameterIndexLookupEvaluator;
}

export interface ParameterIndexLookupEvaluator {
  /**
   * Given a row in a source table that affects sync parameters, returns a structure to index which buckets rows should
   * be associated with.
   *
   * The returned {@link UnscopedParameterLookup} can be referenced by {@link pushBucketParameterQueriers} to allow the storage
   * system to find buckets.
   */
  evaluateParameterRow(sourceTable: SourceTableRef, row: SqliteRow): UnscopedEvaluatedParametersResult[];
}

export enum BucketSourceType {
  SYNC_RULE,
  SYNC_STREAM
}

export type ResultSetDescription = { name: string; columns: ColumnDefinition[] };

function hydrateEvaluateRow(input: HydrationInput, source: BucketDataSource): ScopedEvaluateRow {
  const evaluator = source.createEvaluator(input);
  const scope = input.hydrationState.getBucketSourceScope(source);
  return (options: EvaluateRowOptions): EvaluationResult[] => {
    return evaluator.evaluateRow(options).map((result) => {
      if (isEvaluationError(result)) {
        return result;
      }
      const evaluated: EvaluatedRow = withBucketSource(
        {
          bucket: scope.bucketPrefix + result.serializedBucketParameters,
          id: result.id,
          table: result.table,
          data: result.data
        },
        scope.source
      );
      return evaluated;
    });
  };
}

function hydrateEvaluateParameterRow(
  input: HydrationInput,
  source: ParameterIndexLookupCreator
): ScopedEvaluateParameterRow {
  const evaluator = source.createEvaluator(input);
  const scope = input.hydrationState.getParameterIndexLookupScope(source);
  return (sourceTable: SourceTableRef, row: SqliteRow): EvaluatedParametersResult[] => {
    return evaluator.evaluateParameterRow(sourceTable, row).map((result) => {
      if (isEvaluationError(result)) {
        return result;
      }
      return {
        bucketParameters: result.bucketParameters,
        lookup: ScopedParameterLookup.normalized(scope, result.lookup)
      } satisfies EvaluatedParameters;
    });
  };
}

export function mergeDataSources(
  input: HydrationInput,
  sources: BucketDataSource[]
): { evaluateRow: ScopedEvaluateRow } {
  const withScope = sources.map((source) => hydrateEvaluateRow(input, source));
  return {
    evaluateRow(options: EvaluateRowOptions): EvaluationResult[] {
      return withScope.flatMap((source) => source(options));
    }
  };
}

export function mergeParameterIndexLookupCreators(
  input: HydrationInput,
  sources: ParameterIndexLookupCreator[]
): { evaluateParameterRow: ScopedEvaluateParameterRow } {
  const withScope = sources.map((source) => hydrateEvaluateParameterRow(input, source));
  return {
    evaluateParameterRow(sourceTable: SourceTableRef, row: SqliteRow): EvaluatedParametersResult[] {
      return withScope.flatMap((source) => source(sourceTable, row));
    }
  };
}
