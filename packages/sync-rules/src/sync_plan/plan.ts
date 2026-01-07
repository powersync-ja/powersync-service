import { SyncExpression } from '../compiler/expression.js';
import { ColumnSource } from '../compiler/rows.js';
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
  dataSources: StreamBucketDataSource[];
  parameterIndexes: StreamParameterIndexLookupCreator[];
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
  filters: SyncExpression[];
  /**
   * The parameter instantiation when assigning rows to buckets.
   */
  parameters: SyncExpression[];
}

/**
 * A description for a data source that might be used by multiple streams.
 */
export interface StreamBucketDataSource extends TableProcessor {
  hashCode: number;
  // TODO: Unique name (hashCode might have collisions?)

  /**
   * Output columns describing the row to store in buckets.
   */
  columns: ColumnSource[];
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
  outputs: SyncExpression[];
}
