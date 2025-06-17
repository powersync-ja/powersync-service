import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { ColumnDefinition } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SyncRulesOptions } from './SqlSyncRules.js';
import { TablePattern } from './TablePattern.js';
import { toSyncRulesParameters } from './utils.js';
import { BucketPriority } from './BucketDescription.js';
import { ParameterLookup } from './BucketParameterQuerier.js';

export interface SyncRules {
  evaluateRow(options: EvaluateRowOptions): EvaluationResult[];

  evaluateParameterRow(table: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[];
}

export interface QueryParseOptions extends SyncRulesOptions {
  accept_potentially_dangerous_queries?: boolean;
  priority?: BucketPriority;
}

export interface StreamParseOptions extends QueryParseOptions {
  default?: boolean;
}

export interface EvaluatedParameters {
  lookup: ParameterLookup;

  /**
   * Parameters used to generate bucket id. May be incomplete.
   *
   * JSON-serializable.
   */
  bucketParameters: Record<string, SqliteJsonValue>[];
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
  return 'lookup' in e;
}

export type EvaluationResult = EvaluatedRow | EvaluationError;

export interface RequestJwtPayload {
  /**
   * user_id
   */
  sub: string;

  [key: string]: any;
}

export interface ParameterValueSet {
  lookup(table: string, column: string): SqliteValue;

  /**
   * JSON string of raw request parameters.
   */
  rawUserParameters: string;

  /**
   * JSON string of raw request parameters.
   */
  rawTokenPayload: string;

  userId: string;
}

export class RequestParameters implements ParameterValueSet {
  tokenParameters: SqliteJsonRow;
  userParameters: SqliteJsonRow;

  /**
   * JSON string of raw request parameters.
   */
  rawUserParameters: string;

  /**
   * JSON string of raw request parameters.
   */
  rawTokenPayload: string;

  userId: string;

  constructor(tokenPayload: RequestJwtPayload, clientParameters: Record<string, any>) {
    // This type is verified when we verify the token
    const legacyParameters = tokenPayload.parameters as Record<string, any> | undefined;

    const tokenParameters = {
      ...legacyParameters,
      // sub takes presedence over any embedded parameters
      user_id: tokenPayload.sub
    };

    this.tokenParameters = toSyncRulesParameters(tokenParameters);
    this.userId = tokenPayload.sub;
    this.rawTokenPayload = JSONBig.stringify(tokenPayload);

    this.rawUserParameters = JSONBig.stringify(clientParameters);
    this.userParameters = toSyncRulesParameters(clientParameters);
  }

  lookup(table: string, column: string): SqliteJsonValue {
    if (table == 'token_parameters') {
      return this.tokenParameters[column];
    } else if (table == 'user_parameters') {
      return this.userParameters[column];
    }
    throw new Error(`Unknown table: ${table}`);
  }
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
   * Given RequestParameters, return the associated value to lookup.
   *
   * Only relevant for parameter queries.
   */
  parametersToLookupValue(parameters: ParameterValueSet): SqliteValue;
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

  /** request.user_id(), request.jwt(), token_parameters.* */
  usesAuthenticatedRequestParameters: boolean;
  /** request.parameters(), user_parameters.* */
  usesUnauthenticatedRequestParameters: boolean;
}

/**
 * This is a clause that operates on request or bucket parameters.
 */
export interface ParameterValueClause {
  /** request.user_id(), request.jwt(), token_parameters.* */
  usesAuthenticatedRequestParameters: boolean;
  /** request.parameters(), user_parameters.* */
  usesUnauthenticatedRequestParameters: boolean;

  /**
   * An unique key for the clause.
   *
   * For bucket parameters, this is `bucket.${name}`.
   * For expressions, the exact format is undefined.
   */
  key: string;

  /**
   * Given RequestParameters, return the associated value to lookup.
   *
   * Only relevant for parameter queries.
   */
  lookupParameterValue(parameters: ParameterValueSet): SqliteValue;
}

export interface QuerySchema {
  getColumn(table: string, column: string): ColumnDefinition | undefined;
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
  getColumnDefinition(schema: QuerySchema): ColumnDefinition | undefined;
}

/**
 * Completely static value.
 *
 * Extends RowValueClause and ParameterValueClause to simplify code in some places.
 */
export interface StaticValueClause extends RowValueClause, ParameterValueClause {
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

export interface SourceSchemaTable {
  name: string;
  getColumn(column: string): ColumnDefinition | undefined;
  getColumns(): ColumnDefinition[];
}
export interface SourceSchema {
  getTables(sourceTable: TablePattern): SourceSchemaTable[];
}
