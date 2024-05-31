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
 */
export type FilterParameters = { [parameter: string]: SqliteJsonValue };

export interface EvaluateRowOptions {
  sourceTable: SourceTableInterface;
  record: SqliteRow;
}

/**
 * Given a row, produces a set of parameters that would make the clause evaluate to true.
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
  bucketParameters: string[];

  /**
   * True if the filter depends on an unbounded array column.
   *
   * We restrict filters to only allow a single unbounded column for bucket parameters, otherwise the number of
   * bucketParameter combinations could grow too much.
   */
  unbounded: boolean;

  /**
   * Given a data row, give a set of filter parameters that would make the filter be true.
   *
   * @param tables - {table => row}
   * @return The filter parameters
   */
  filter(tables: QueryParameters): TrueIfParametersMatch;
}

/**
 * Given a row, produces a set of parameters that would make the clause evaluate to true.
 */
export interface ParameterValueClause {
  /**
   * The parameter fields used for this, e.g. 'bucket.region_id'
   */
  bucketParameter: string;
}

export interface QuerySchema {
  getType(table: string, column: string): ExpressionType;
  getColumns(table: string): ColumnDefinition[];
}

/**
 * Only needs row values as input, producing a static value as output.
 */
export interface StaticRowValueClause {
  evaluate(tables: QueryParameters): SqliteValue;
  getType(schema: QuerySchema): ExpressionType;
}

export interface ClauseError {
  error: true;
}

export type CompiledClause = StaticRowValueClause | ParameterMatchClause | ParameterValueClause | ClauseError;

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
