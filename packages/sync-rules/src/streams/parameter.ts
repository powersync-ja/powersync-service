import { ParameterLookup } from '../BucketParameterQuerier.js';
import { BucketParameterLookupSource, BucketParameterLookupSourceDefinition } from '../BucketSource.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluateRowOptions,
  ParameterValueSet,
  RequestParameters,
  SqliteJsonValue,
  SqliteRow,
  SqliteValue,
  TableRow
} from '../types.js';

/**
 * A source of parameterization, causing data from the source table to be distributed into multiple buckets instead of
 * a single one.
 *
 * Parameters are introduced when the select statement defining the stream has a where clause with elements where:
 *
 *   1. Values in the row to sync are compared against request parameters: {@link CompareRowValueWithStreamParameter}.
 *   2. Values in the row to sync are compared against a subquery: {@link InOperator}.
 */
export interface BucketParameter {
  lookup: StaticLookup | EqualsRowInSubqueryLookup | OverlapsSubqueryLookup;
  /**
   * Given a row in the table the stream is selecting from, return all possible instantiations of this parameter that
   * would match the row.
   *
   * This is used to assign rows to buckets. For instance, considering the query
   * `SELECT * FROM asset WHERE owner = request.user_id()`, we would introduce a parameter. For that parameter,
   * `filterRow(assetRow)` would return `assetRow.owner`.
   * When a user connects, {@link StaticLookup.fromRequest} would return the user ID from the token. A matching bucket would
   * then contain the oplog data for assets with the matching `owner` column.
   */
  filterRow(row: TableRow): SqliteJsonValue[];
}

export interface SubqueryEvaluator {
  parameterTable: TablePattern;

  lookupsForRequest(params: RequestParameters): ParameterLookup[];

  lookupSources(streamName: string): BucketParameterLookupSourceDefinition[];
}

export interface SubqueryLookups {
  lookups: ParameterLookup[];
  /**
   * The value that the single column in the subquery evaluated to.
   */
  value: SqliteJsonValue;
}

/**
 * An association of rows to subscription parameters that does not depend on a subquery.
 */
export interface StaticLookup {
  type: 'static';

  /**
   * The value this lookup evaluates to for a specific request.
   *
   * This is typically a singleton array, e.g. the user's id in `WHERE owner_id = token.user_id()`. To desugar `IN`
   * queries on parameter data, this can also return multiple values though: `WHERE owner_id IN subscription.parameters() -> 'user_ids'`.
   */
  fromRequest(parameters: ParameterValueSet): SqliteValue[];
}

/**
 * An association of rows that is matched if a value in the source row is contained in the results of a subquery.
 */
export interface EqualsRowInSubqueryLookup {
  type: 'in';
  subquery: SubqueryEvaluator;
}

/**
 * An association of rows that is matched if a source-row value (interpreted as a JSON array) overlaps with rows
 * contained in the results of a subqery.
 */
export interface OverlapsSubqueryLookup {
  type: 'overlap';
  subquery: SubqueryEvaluator;
}
