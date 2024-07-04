import { JsonContainer } from '@powersync/service-jsonbig';
import { SourceTableInterface } from './SourceTableInterface.js';
import { ColumnDefinition, ExpressionType } from './ExpressionType.js';
import { TablePattern } from './TablePattern.js';

export interface SyncRules {
  evaluateRow(options: EvaluateRowOptions): EvaluationResult[];

  evaluateParameterRow(table: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[];
}

export interface EvaluatedParameters {
  lookup: SqliteJsonValue[];

  /**
   * Parameters used to generate bucket id. May be incomplete.
   *
   * JSON-serializable.
   */
  bucket_parameters: Record<string, SqliteJsonValue>[];
}

export type EvaluatedParametersResult = EvaluatedParameters | EvaluationError;

export interface EvaluatedRow {
  bucket: string;

  /** Output table - may be different from input table. */
  table: string;

  /**
   * Convenience attribute. Must match data.id.
   */
  id: string;

  /** Must be JSON-serializable. */
  data: SqliteJsonRow;

  /** For debugging purposes only. */
  ruleId?: string;
}

export interface EvaluationError {
  error: string;
}

export function isEvaluationError(e: any): e is EvaluationError {
  return typeof e.error == 'string';
}

export function isEvaluatedRow(e: EvaluationResult): e is EvaluatedRow {
  return typeof (e as EvaluatedRow).bucket == 'string';
}

export function isEvaluatedParameters(e: EvaluatedParametersResult): e is EvaluatedParameters {
  return Array.isArray((e as any).lookup);
}

export type EvaluationResult = EvaluatedRow | EvaluationError;

export interface SyncParameters {
  token_parameters: SqliteJsonRow;
  user_parameters: SqliteJsonRow;
}

/**
 * A value that is both SQLite and JSON-compatible.
 *
 * Uint8Array is not supported.
 */
export type SqliteJsonValue = number | string | bigint | null;

/**
 * A value supported by the SQLite type system.
 */
export type SqliteValue = number | string | null | bigint | Uint8Array;

/**
 * A set of values that are both SQLite and JSON-compatible.
 *
 * This is a flat object -> no nested arrays or objects.
 */
export type SqliteJsonRow = { [column: string]: SqliteJsonValue };

/**
 * SQLite-compatible row (NULL, TEXT, INTEGER, REAL, BLOB).
 * JSON is represented as TEXT.
 */
export type SqliteRow = { [column: string]: SqliteValue };

/**
 * SQLite-compatible row (NULL, TEXT, INTEGER, REAL, BLOB).
 * JSON is represented as TEXT.
 *
 * Toasted values are `undefined`.
 */
export type ToastableSqliteRow = { [column: string]: SqliteValue | undefined };

/**
 * A value as received from the database.
 */
export type DatabaseInputValue =
  | SqliteValue
  | boolean
  | DatabaseInputValue[]
  | JsonContainer
  | { [key: string]: DatabaseInputValue };

/**
 * Database input row. Can contain nested arrays and objects.
 */
export type DatabaseInputRow = { [column: string]: DatabaseInputValue };

/**
 * A set of known parameters that a query is evaluated on.
 */
export type QueryParameters = { [table: string]: SqliteRow };

/**
 * A single set of parameters that would make a WHERE filter true.
 *
 * Each parameter is prefixed with a table name, e.g. 'bucket.param'.
 *
 * Data queries: this is converted into a bucket id, given named bucket parameters.
 *
 * Parameter queries: this is converted into a lookup array.
 */
export type FilterParameters = { [parameter: string]: SqliteJsonValue };

export interface InputParameter {
  /**
   * An unique identifier per parameter in a query.
   *
   * This is used to identify the same parameters used in a query multiple times.
   *
   * The value itself does not necessarily have any specific meaning.
   */
  key: string;

  /**
   * True if the parameter expands to an array. This means parametersToLookupValue() can
   * return a JSON array. This is different from `unbounded` on the clause.
   */
  expands: boolean;

  /**
   * Given FilterParameters from a data row, return the associated value.
   *
   * Only relevant for parameter queries.
   */
  filteredRowToLookupValue(filterParameters: FilterParameters): SqliteJsonValue;

  /**
   * Given SyncParamters, return the associated value to lookup.
   *
   * Only relevant for parameter queries.
   */
  parametersToLookupValue(parameters: SyncParameters): SqliteValue;
}

export interface EvaluateRowOptions {
  sourceTable: SourceTableInterface;
  record: SqliteRow;
}

/**
 * This is a clause that matches row and parameter values.
 *
 * Example:
 * [WHERE] users.org_id = bucket.org_id
 *
 * For a given a row, this produces a set of parameters that would make the clause evaluate to true.
 */
export interface ParameterMatchClause {
  error: boolean;

  /**
   * The parameter fields that are used for this filter, for example:
   *  * ['bucket.region_id'] for a data query
   *  * ['token_parameters.user_id'] for a parameter query
   *
   * These parameters are always matched by this clause, and no additional parameters are matched.
   */
  inputParameters: InputParameter[];

  /**
   * True if the filter depends on an unbounded array column. This means filterRow can return
   * multiple items.
   *
   * We restrict filters to only allow a single unbounded column for bucket parameters, otherwise the number of
   * bucketParameter combinations could grow too much.
   */
  unbounded: boolean;

  /**
   * Given a data row, give a set of filter parameters that would make the filter be true.
   *
   * For StaticSqlParameterQuery, the tables are token_parameters and user_parameters.
   * For others, it is the table of the data or parameter query.
   *
   * @param tables - {table => row}
   * @return The filter parameters
   */
  filterRow(tables: QueryParameters): TrueIfParametersMatch;
}

/**
 * This is a clause that operates on request or bucket parameters.
 */
export interface ParameterValueClause {
  /**
   * An unique key for the clause.
   *
   * For bucket parameters, this is `bucket.${name}`.
   * For expressions, the exact format is undefined.
   */
  key: string;

  /**
   * Given SyncParamters, return the associated value to lookup.
   *
   * Only relevant for parameter queries.
   */
  lookupParameterValue(parameters: SyncParameters): SqliteValue;
}

export interface QuerySchema {
  getType(table: string, column: string): ExpressionType;
  getColumns(table: string): ColumnDefinition[];
}

/**
 * A clause that uses row values as input.
 *
 * For parameter queries, that is the parameter table being queried.
 * For data queries, that is the data table being queried.
 */
export interface RowValueClause {
  evaluate(tables: QueryParameters): SqliteValue;
  getType(schema: QuerySchema): ExpressionType;
}

/**
 * Completely static value.
 *
 * Extends RowValueClause to simplify code in some places.
 */
export interface StaticValueClause extends RowValueClause {
  readonly value: SqliteValue;
}

export interface ClauseError {
  error: true;
}

export type CompiledClause = RowValueClause | ParameterMatchClause | ParameterValueClause | ClauseError;

/**
 * true if any of the filter parameter sets match
 */
export type TrueIfParametersMatch = FilterParameters[];

export interface QueryBucketIdOptions {
  getParameterSets: (lookups: SqliteJsonValue[][]) => Promise<SqliteJsonRow[]>;
  parameters: SyncParameters;
}

export interface SourceSchemaTable {
  table: string;
  getType(column: string): ExpressionType | undefined;
  getColumns(): ColumnDefinition[];
}
export interface SourceSchema {
  getTables(sourceTable: TablePattern): SourceSchemaTable[];
}
