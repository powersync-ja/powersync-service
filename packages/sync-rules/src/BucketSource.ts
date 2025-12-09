import { BucketParameterQuerier, ParameterLookup, PendingQueriers } from './BucketParameterQuerier.js';
import { ColumnDefinition } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { GetQuerierOptions } from './SqlSyncRules.js';
import { TablePattern } from './TablePattern.js';
import {
  BucketIdTransformer,
  EvaluatedParametersResult,
  EvaluateRowOptions,
  EvaluationResult,
  SourceSchema,
  SqliteRow
} from './types.js';

export interface CreateSourceParams {
  bucketIdTransformer: BucketIdTransformer;
}

/**
 * Encodes a static definition of a bucket source, as parsed from sync rules or stream definitions.
 */
export interface BucketDataSourceDefinition {
  readonly name: string;
  readonly type: BucketSourceType;
  readonly subscribedToByDefault: boolean;
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

  debugRepresentation(): any;
}

export interface BucketParameterSourceDefinition {
  readonly name: string;
  readonly type: BucketSourceType;
  readonly subscribedToByDefault: boolean;

  getSourceTables(): Set<TablePattern>;
  createParameterSource(params: CreateSourceParams): BucketParameterSource;

  /**
   * Whether {@link pushBucketParameterQueriers} may include a querier where
   * {@link BucketParameterQuerier.hasDynamicBuckets} is true.
   *
   * This is mostly used for testing.
   */
  hasDynamicBucketQueries(): boolean;

  getSourceTables(): Set<TablePattern>;

  /** Whether the table possibly affects the buckets resolved by this source. */
  tableSyncsParameters(table: SourceTableInterface): boolean;

  debugRepresentation(): any;
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
  readonly definition: BucketDataSourceDefinition;

  /**
   * Given a row as it appears in a table that affects sync data, return buckets, logical table names and transformed
   * data for rows to add to buckets.
   */
  evaluateRow(options: EvaluateRowOptions): EvaluationResult[];
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
export interface BucketParameterSource {
  readonly definition: BucketParameterSourceDefinition;
  /**
   * Given a row in a source table that affects sync parameters, returns a structure to index which buckets rows should
   * be associated with.
   *
   * The returned {@link ParameterLookup} can be referenced by {@link pushBucketParameterQueriers} to allow the storage
   * system to find buckets.
   */
  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[];

  /**
   * Reports {@link BucketParameterQuerier}s resolving buckets that a specific stream request should have access to.
   *
   * @param result The target array to insert queriers and errors into.
   * @param options Options, including parameters that may affect the buckets loaded by this source.
   */
  pushBucketParameterQueriers(result: PendingQueriers, options: GetQuerierOptions): void;

  /**
   * @deprecated Use `pushBucketParameterQueriers` instead and merge at the top-level.
   */
  getBucketParameterQuerier(options: GetQuerierOptions): BucketParameterQuerier;
}

export enum BucketSourceType {
  SYNC_RULE,
  SYNC_STREAM
}

export type ResultSetDescription = { name: string; columns: ColumnDefinition[] };
