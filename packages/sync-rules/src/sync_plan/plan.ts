import { BucketPriority } from '../BucketDescription.js';
import { ParameterLookupScope } from '../HydrationState.js';
import { TablePattern } from '../TablePattern.js';
import { UnscopedEvaluatedParameters } from '../types.js';

/**
 * A compiled "sync plan", a description for
 *
 *   1. how rows get filtered, mapped and put into buckets.
 *   2. how rows get filtered, mapped and put into parameter lookups.
 *   3. how connections use parameter lookups to infer buckets.
 *
 * Sync plans can be compiled from sync stream definitions. There is always a single plan for all stream definitions,
 * which allows re-using parameter lookups if multiple streams have similar subpatterns.
 *
 * Additionally, sync plans can be serialized to (and restored from) a simple JSON representation. We store sync plans
 * in bucket storage and restore them on service startup, allowing us to make changes to the compiler without that
 * affecting existing deployed sync instances.
 *
 * However, this also means that sync plan definitions themselves must have a stable representation. We can add to them,
 * but we must never alter the semantics for existing serialized plans.
 */
export interface SyncPlan {
  dataSources: StreamDataSource[];
  buckets: StreamBucketDataSource[];
  parameterIndexes: StreamParameterIndexLookupCreator[];
  streams: CompiledSyncStream[];
}

/**
 * Something that processes a source row if it matches filters.
 */
export interface TableProcessor {
  /**
   * A hash code describing the structure of this processor.
   *
   * While these processors are plain JavaScript objects that can be compared by deep equality, this hash code may
   * improve performance when many such sources need to be compared. Two equal data sources will always have the same
   * hash code.
   */
  hashCode: number;
  /**
   * The source table to process into buckets or parameter lookups.
   */
  sourceTable: TablePattern;
  /**
   * All of these expressions exclusively depend on the {@link sourceTable}.
   *
   * All of the filters must evaluate to a "true-ish" value for the row to be processed.
   */
  filters: SqlExpression<ColumnSqlParameterValue>[];
  /**
   * How to partition rows.
   *
   * For data sources, this describes parameter of the bucket being created. For parameter lookups, these form
   * input values for the lookup.
   */
  parameters: PartitionKey[];
}

export interface PartitionKey {
  expr: SqlExpression<ColumnSqlParameterValue>;
}

/**
 * A description for a data source processing rows to replicate.
 *
 * A single data source might be used in multiple buckets ({@link StreamBucketDataSource}). In those cases, we'd store
 * duplicate bucket data but can still share the actual processing logic between multiple streams (to avoid evaluating
 * the same filters and expressions multiple times).
 */
export interface StreamDataSource extends TableProcessor {
  /**
   * The name of the output table for evaluated rows.
   *
   * If null, the name of the table being evaluated should be used instead.
   */
  outputTableName?: string;

  /**
   * Output columns describing the row to store in buckets.
   */
  columns: ColumnSource[];
}

export type ColumnSource = 'star' | { expr: SqlExpression<ColumnSqlParameterValue>; alias: string };

/**
 * A mapping describing how {@link StreamDataSource}s are combined into buckets.
 *
 * One instance of this always describes a single bucket data source. A stream may consist of multiple such sources if
 * different variants are used. It is also possible for a bucket data source to be reused between streams in some
 * instances.
 */
export interface StreamBucketDataSource {
  hashCode: number;

  /**
   * A unique name of this source in all streams compiled from a sync rules file.
   */
  uniqueName: string;

  /**
   * All data sources to put into this bucket.
   *
   * Note that all data sources must have the same amount of paremeters, since they would be instantiated to the same
   * values by a {@link StreamQuerier}.
   */
  sources: StreamDataSource[];
}

/**
 * A description for a data source creating parameter lookup values that can be queried when fetching buckets for a
 * connection.
 */
export interface StreamParameterIndexLookupCreator extends TableProcessor {
  hashCode: number;
  defaultLookupScope: ParameterLookupScope;

  /**
   * Outputs to persist in the lookup.
   *
   * Note: In {@link UnscopedEvaluatedParameters}, outputs are called `bucketParameters`. This doesn't apply to sync
   * streams because the output of parameters might be passed through additional stages or transformed by the querier
   * before becoming a parameter value.
   */
  outputs: SqlExpression<ColumnSqlParameterValue>[];
}

export interface StreamOptions {
  name: string;
  isSubscribedByDefault: boolean;
  priority: BucketPriority;
}

export interface CompiledSyncStream {
  stream: StreamOptions;
  queriers: StreamQuerier[];
}

export interface StreamQuerier {
  /**
   * Static filters on the subscription, connection or token.
   *
   * All of them must match for this querier to be considered for a subscription.
   */
  requestFilters: SqlExpression<RequestSqlParameterValue>[];

  /**
   * Ordered lookups that need to be evaluated and expanded before we can evaluate {@link sourceInstantiation}.
   *
   * Each element of the outer array represents a stage of lookups that can run concurrently.
   */
  lookupStages: ExpandingLookup[][];
  /**
   * The bucket being resolved by this querier.
   *
   * While a querier can have more than one source, it never uses more than one instantiation path. This means that all
   * sources in {@link StreamBucketDataSource.sources} must have compatible parameters.
   */
  bucket: StreamBucketDataSource;
  sourceInstantiation: ParameterValue[];
}

/**
 * An expression that can be evaluated by SQLite.
 *
 * The type parameter `T` describes which values this expression uses. For instance, an expression in a
 * {@link TableProcessor} would only use {@link ColumnSqlParameterValue}s since no request is available in that context.
 */
export interface SqlExpression<T extends SqlParameterValue> {
  /**
   * To SQL expression to evaluate.
   *
   * The expression is guaranteed to not contain any column references. All dependencies to row data are encoded as SQL
   * parameters that have an instantiation in {@link instantiation}.
   *
   * For instance, the stream `SELECT UPPER(name) FROM users;` would have `UPPER(?1)` as SQL and a
   */
  sql: string;
  values: T[];
}

export interface SqlParameterValue {
  /**
   * The start index and length of the parameter in {@link SqlExpression.sql} that this parameter is instantiating.
   *
   * This allows merging equal parameters, which is useful to compose SQL expressions.
   */
  sqlPosition: [number, number];
}

/**
 * A value that resolves to a given column in a row being processed.
 */
export interface ColumnSqlParameterValue extends SqlParameterValue {
  column: string;
}

export type ConnectionParameterSource = 'auth' | 'subscription' | 'connection';

/**
 * A value that resolves to either the current subscription parameters, the JWT of the connecting user or global
 * request parameters.
 */
export interface RequestSqlParameterValue extends SqlParameterValue {
  request: ConnectionParameterSource;
}

/**
 * A lookup returning multiple rows when instantiated.
 */
export type ExpandingLookup = ParameterLookup | EvaluateTableValuedFunction;

export interface ParameterLookup {
  type: 'parameter';
  lookup: StreamParameterIndexLookupCreator;
  /**
   * Must have the same length as {@link TableProcessor.parameters} for {@link lookup}.
   */
  instantiation: ParameterValue[];
}

export interface EvaluateTableValuedFunction {
  type: 'table_valued';
  functionName: string;
  functionInputs: SqlExpression<RequestSqlParameterValue>[];
  outputs: SqlExpression<ColumnSqlParameterValue>[];
  filters: SqlExpression<ColumnSqlParameterValue>[];
}

export type ParameterValue =
  | { type: 'request'; expr: SqlExpression<RequestSqlParameterValue> }
  | { type: 'lookup'; lookup: ExpandingLookup; resultIndex: number }
  | { type: 'intersection'; values: ParameterValue[] };
