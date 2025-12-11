import { BucketParameterQuerier, ParameterLookup, PendingQueriers } from './BucketParameterQuerier.js';
import { ColumnDefinition } from './ExpressionType.js';
import { DEFAULT_HYDRATION_STATE, HydrationState, ParameterLookupScope } from './HydrationState.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { GetQuerierOptions } from './SqlSyncRules.js';
import { TablePattern } from './TablePattern.js';
import { EvaluatedParametersResult, EvaluateRowOptions, EvaluationResult, SourceSchema, SqliteRow } from './types.js';

export interface CreateSourceParams {
  hydrationState: HydrationState;
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
  readonly dataSources: BucketDataSourceDefinition[];

  /**
   * BucketParameterQuerierSource describing the parameter queries / stream subqueries in this bucket/stream definition.
   *
   * The same source could in theory be present in multiple stream definitions.
   */
  readonly parameterQuerierSources: BucketParameterQuerierSourceDefinition[];

  /**
   * BucketParameterLookupSource describing the parameter tables used in this bucket/stream definition.
   *
   * The same source could in theory be present in multiple stream definitions.
   */
  readonly parameterLookupSources: BucketParameterLookupSourceDefinition[];

  debugRepresentation(): any;
}

export interface HydratedBucketSource {
  readonly definition: BucketSource;

  readonly parameterQuerierSources: BucketParameterQuerierSource[];
}

/**
 * Encodes a static definition of a bucket source, as parsed from sync rules or stream definitions.
 */
export interface BucketDataSourceDefinition {
  /**
   * Bucket prefix if no transformations are defined.
   *
   * Transformations may use this as a base, or may generate an entirely different prefix.
   */
  readonly defaultBucketPrefix: string;

  /**
   * For debug use only.
   */
  readonly bucketParameters: string[];
  getSourceTables(): Set<TablePattern>;
  createDataSource(params: CreateSourceParams): BucketDataSource;
  tableSyncsData(table: SourceTableInterface): boolean;

  /**
   * Given a static schema, infer all logical tables and associated columns that appear in buckets defined by this
   * source.
   *
   * This is use to generate the client-side schema.
   */
  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void;

  debugWriteOutputTables(result: Record<string, { query: string }[]>): void;
}

/**
 * A parameter lookup source defines how to extract parameter lookup values from parameter queries.
 *
 * This is only relevant for parameter queries that query tables.
 */
export interface BucketParameterLookupSourceDefinition {
  /**
   * lookupName + queryId is used to uniquely identify parameter queries for parameter storage.
   *
   * This defines the default values if no transformations are applied.
   */
  readonly defaultLookupScope: ParameterLookupScope;

  getSourceTables(): Set<TablePattern>;
  createParameterLookupSource(params: CreateSourceParams): BucketParameterLookupSource;

  /** Whether the table possibly affects the buckets resolved by this source. */
  tableSyncsParameters(table: SourceTableInterface): boolean;
}

/**
 * Parameter querier source definitions define how to bucket parameter queries are evaluated.
 *
 * This may use request data only, or it may use parameter lookup data persisted by a BucketParameterLookupSourceDefinition.
 */
export interface BucketParameterQuerierSourceDefinition {
  /**
   * For debug use only.
   */
  readonly bucketParameters: string[];

  /**
   * The data source linked to this querier. This determines the bucket names that the querier generates.
   *
   * Note that queriers do not persist data themselves; they only resolve which buckets to load based on request parameters.
   */
  readonly querierDataSource: BucketDataSourceDefinition;

  createParameterQuerierSource(params: CreateSourceParams): BucketParameterQuerierSource;
}

/**
 * An interface declaring
 *
 *  - which buckets the sync service should create when processing change streams from the database.
 *  - how data in source tables maps to data in buckets (e.g. when we're not selecting all columns).
 *  - which buckets a given connection has access to.
 *
 * There are two ways to define bucket sources: Via sync rules made up of parameter and data queries, and via stream
 * definitions that only consist of a single query.
 */
export interface BucketDataSource {
  /**
   * Given a row as it appears in a table that affects sync data, return buckets, logical table names and transformed
   * data for rows to add to buckets.
   */
  evaluateRow(options: EvaluateRowOptions): EvaluationResult[];
}

export interface BucketParameterLookupSource {
  /**
   * Given a row in a source table that affects sync parameters, returns a structure to index which buckets rows should
   * be associated with.
   *
   * The returned {@link ParameterLookup} can be referenced by {@link pushBucketParameterQueriers} to allow the storage
   * system to find buckets.
   */
  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[];
}

export interface BucketParameterQuerierSource {
  /**
   * Reports {@link BucketParameterQuerier}s resolving buckets that a specific stream request should have access to.
   *
   * @param result The target array to insert queriers and errors into.
   * @param options Options, including parameters that may affect the buckets loaded by this source.
   */
  pushBucketParameterQueriers(result: PendingQueriers, options: GetQuerierOptions): void;
}

export interface DebugMergedSource
  extends BucketDataSource,
    BucketParameterLookupSource,
    BucketParameterQuerierSource {}

export enum BucketSourceType {
  SYNC_RULE,
  SYNC_STREAM
}

export type ResultSetDescription = { name: string; columns: ColumnDefinition[] };

export function mergeDataSources(sources: BucketDataSource[]): BucketDataSource {
  return {
    evaluateRow(options: EvaluateRowOptions): EvaluationResult[] {
      let results: EvaluationResult[] = [];
      for (let source of sources) {
        results.push(...source.evaluateRow(options));
      }
      return results;
    }
  };
}

export function mergeParameterLookupSources(sources: BucketParameterLookupSource[]): BucketParameterLookupSource {
  return {
    evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[] {
      let results: EvaluatedParametersResult[] = [];
      for (let source of sources) {
        results.push(...source.evaluateParameterRow(sourceTable, row));
      }
      return results;
    }
  };
}

export function mergeParameterQuerierSources(sources: BucketParameterQuerierSource[]): BucketParameterQuerierSource {
  return {
    pushBucketParameterQueriers(result: PendingQueriers, options: GetQuerierOptions): void {
      for (let source of sources) {
        source.pushBucketParameterQueriers(result, options);
      }
    }
  };
}

/**
 * For production purposes, we typically need to operate on the different sources separately. However, for debugging,
 * it is useful to have a single merged source that can evaluate everything.
 */
export function debugHydratedMergedSource(bucketSource: BucketSource, params?: CreateSourceParams): DebugMergedSource {
  const hydrationState = params?.hydrationState ?? DEFAULT_HYDRATION_STATE;
  const resolvedParams = { hydrationState };
  const dataSource = mergeDataSources(
    bucketSource.dataSources.map((source) => source.createDataSource(resolvedParams))
  );
  const parameterLookupSource = mergeParameterLookupSources(
    bucketSource.parameterLookupSources.map((source) => source.createParameterLookupSource(resolvedParams))
  );
  const parameterQuerierSource = mergeParameterQuerierSources(
    bucketSource.parameterQuerierSources.map((source) => source.createParameterQuerierSource(resolvedParams))
  );
  return {
    evaluateParameterRow: parameterLookupSource.evaluateParameterRow.bind(parameterLookupSource),
    evaluateRow: dataSource.evaluateRow.bind(dataSource),
    pushBucketParameterQueriers: parameterQuerierSource.pushBucketParameterQueriers.bind(parameterQuerierSource)
  };
}
