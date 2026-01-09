import { StreamOptions } from '../compiler/bucket_resolver.js';
import { ConnectionParameterSource } from '../compiler/expression.js';
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
  queriers: StreamQuerier[];
}

/**
 * Something that processes a source row with if it matches filters.
 */
export interface TableProcessor {
  /**
   * The source table to replicate.
   */
  sourceTable: TablePattern;
  /**
   * All of these expressions exclusively depend on the {@link sourceTable}.
   *
   * ALl of the filters must evaluate to a "true-ish" value for the row to be processed.
   */
  filters: SqlExpression[];
  /**
   * The parameter instantiation when assigning rows to buckets.
   */
  parameters: SqlExpression[];
}

/**
 * A description for a data source that might be used by multiple streams.
 */
export interface StreamDataSource extends TableProcessor {
  hashCode: number;
  // TODO: Unique name (hashCode might have collisions?)

  /**
   * Output columns describing the row to store in buckets.
   */
  columns: ColumnSource[];
}

export type ColumnSource = 'star' | { expr: SqlExpression; alias: string | null };

/**
 * A mapping describing how {@link StreamDataSource}s are combined into buckets.
 *
 * One instance of this always describe a bucket data source.
 */
export interface StreamBucketDataSource {
  hashCode: number;
  uniqueName: string;

  /**
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
  // TODO: default lookup scope (hashCode might have collisions?)

  /**
   * Outputs to persist in the lookup.
   *
   * Note: In {@link UnscopedEvaluatedParameters}, outputs are called `bucketParameters`. This doesn't apply to sync
   * streams because the output of parameters might be passed through additional stages or transformed by the querier
   * before becoming a parameter value.
   */
  outputs: SqlExpression[];
}

export interface StreamQuerier {
  stream: StreamOptions;
  /**
   * Static filters on the subscription, connection or token.
   *
   * All of them must match for this querier to be considered for a subscription.
   */
  requestFilters: SqlExpression[];

  /**
   * Ordered lookups that need to be evaluated and expanded before we can evaluate {@link sourceInstantiation}.
   *
   * Each element of the outer array represents a stage of lookups that can run concurrently.
   */
  lookupStages: ExpandingLookup[][];
  /**
   * All data sources being resolved by this querier.
   *
   * While a querier can have more than one source, it never uses more than one instantiation path. This means that all
   * sources must have compatible parameters.
   */
  bucket: StreamBucketDataSource;
  sourceInstantiation: ParameterValue[];
}

export interface SqlExpression {
  hash: number;
  sql: string;
  instantiation: ({ column: string } | { connection: ConnectionParameterSource })[];
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
  functionInputs: SqlExpression[];
  outputs: SqlExpression[];
  filters: SqlExpression[];
}

export type ParameterValue =
  | { type: 'request'; expr: SqlExpression }
  | { type: 'lookup'; lookup: ExpandingLookup; resultIndex: number }
  | { type: 'intersection'; values: ParameterValue[] };
