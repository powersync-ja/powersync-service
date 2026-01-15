import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { BucketPriority } from './BucketDescription.js';
import { ScopedParameterLookup, UnscopedParameterLookup } from './BucketParameterQuerier.js';
import { CompatibilityContext } from './compatibility.js';
import { ColumnDefinition } from './ExpressionType.js';
import { RequestFunctionCall } from './request_functions.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SyncRulesOptions } from './SqlSyncRules.js';
import { TablePattern } from './TablePattern.js';
import { CustomSqliteValue } from './types/custom_sqlite_value.js';
import { SOURCE, toSyncRulesParameters } from './utils.js';
import { BucketDataSource } from './index.js';

export interface QueryParseOptions extends SyncRulesOptions {
  accept_potentially_dangerous_queries?: boolean;
  priority?: BucketPriority;
  compatibility: CompatibilityContext;
}

export interface StreamParseOptions extends QueryParseOptions {
  auto_subscribe?: boolean;
}

export interface EvaluatedParameters {
  lookup: ScopedParameterLookup;

  /**
   * Parameters used to generate bucket id. May be incomplete.
   *
   * JSON-serializable.
   */
  bucketParameters: Record<string, SqliteJsonValue>[];
}

export interface UnscopedEvaluatedParameters {
  lookup: UnscopedParameterLookup;

  /**
   * Parameters used to generate bucket id. May be incomplete.
   *
   * JSON-serializable.
   */
  bucketParameters: Record<string, SqliteJsonValue>[];
}

export type EvaluatedParametersResult = EvaluatedParameters | EvaluationError;
export type UnscopedEvaluatedParametersResult = UnscopedEvaluatedParameters | EvaluationError;

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

  /**
   * Source for the evaluated row.
   */
  source: BucketDataSource;
}

/**
 * Bucket data as evaluated by the BucketDataSource.
 *
 * The bucket name must still be resolved, external to this.
 */
export interface UnscopedEvaluatedRow {
  /**
   * Serialized evaluated parameters used to generate the bucket id. Serialized as a JSON array.
   *
   * Examples:
   *  [] // no bucket parameters
   *  [1] // single numeric parameter
   *  [1,"foo"] // multiple parameters
   *
   * The bucket name is derived by using concetenating these parameters with the generated bucket name.
   */
  serializedBucketParameters: string;

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

export function isEvaluationError(
  e: EvaluationResult | UnscopedEvaluationResult | EvaluatedParametersResult | UnscopedEvaluatedParametersResult
): e is EvaluationError {
  return typeof (e as EvaluationError).error == 'string';
}

export function isEvaluatedRow(e: EvaluationResult): e is EvaluatedRow {
  return typeof (e as EvaluatedRow).bucket == 'string';
}

export function isSourceEvaluatedRow(e: UnscopedEvaluationResult): e is UnscopedEvaluatedRow {
  return typeof (e as UnscopedEvaluatedRow).serializedBucketParameters == 'string';
}

export function isEvaluatedParameters(e: EvaluatedParametersResult): e is EvaluatedParameters {
  return 'lookup' in e;
}

export type EvaluationResult = EvaluatedRow | EvaluationError;
export type UnscopedEvaluationResult = UnscopedEvaluatedRow | EvaluationError;

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
  userParameters: SqliteJsonRow;

  /**
   * For streams, the raw JSON string of stream parameters.
   */
  rawStreamParameters: string | null;
  streamParameters: SqliteJsonRow | null;

  /**
   * JSON string of raw request parameters.
   */
  rawTokenPayload: string;
  parsedTokenPayload: SqliteJsonRow;
  legacyTokenParameters: SqliteJsonRow;

  userId: string;
}

export class RequestParameters implements ParameterValueSet {
  parsedTokenPayload: SqliteJsonRow;
  legacyTokenParameters: SqliteJsonRow;
  userParameters: SqliteJsonRow;

  /**
   * JSON string of raw request parameters.
   */
  rawUserParameters: string;

  streamParameters: SqliteJsonRow | null;
  rawStreamParameters: string | null;

  /**
   * JSON string of raw request parameters.
   */
  rawTokenPayload: string;

  userId: string;

  constructor(tokenPayload: RequestJwtPayload, clientParameters: Record<string, any>);
  constructor(params: RequestParameters);

  constructor(tokenPayload: RequestJwtPayload | RequestParameters, clientParameters?: Record<string, any>) {
    if (tokenPayload instanceof RequestParameters) {
      this.parsedTokenPayload = tokenPayload.parsedTokenPayload;
      this.legacyTokenParameters = tokenPayload.legacyTokenParameters;
      this.userParameters = tokenPayload.userParameters;
      this.rawUserParameters = tokenPayload.rawUserParameters;
      this.rawTokenPayload = tokenPayload.rawTokenPayload;
      this.streamParameters = tokenPayload.streamParameters;
      this.rawStreamParameters = tokenPayload.rawStreamParameters;
      this.userId = tokenPayload.userId;
      return;
    }

    // This type is verified when we verify the token
    const legacyParameters = tokenPayload.parameters as Record<string, any> | undefined;

    const tokenParameters = {
      ...legacyParameters,
      // sub takes presedence over any embedded parameters
      user_id: tokenPayload.sub
    };

    // Client and token parameters don't contain DateTime values or other custom types, so we don't need to consider
    // compatibility.
    this.parsedTokenPayload = tokenPayload;
    this.legacyTokenParameters = toSyncRulesParameters(
      tokenParameters,
      CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
    );
    this.userId = tokenPayload.sub;
    this.rawTokenPayload = JSONBig.stringify(tokenPayload);

    this.rawUserParameters = JSONBig.stringify(clientParameters);
    this.userParameters = toSyncRulesParameters(clientParameters!, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
    this.streamParameters = null;
    this.rawStreamParameters = null;
  }

  lookup(table: string, column: string): SqliteJsonValue {
    if (table == 'token_parameters') {
      return this.legacyTokenParameters[column];
    } else if (table == 'user_parameters') {
      return this.userParameters[column];
    } else if (table == 'subscription_parameters' && this.streamParameters != null) {
      return this.streamParameters[column];
    }
    throw new Error(`Unknown table: ${table}`);
  }

  withAddedStreamParameters(params: Record<string, any>): RequestParameters {
    const clone = new RequestParameters(this);
    clone.streamParameters = params;
    clone.rawStreamParameters = JSONBig.stringify(params);

    return clone;
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
 * A value that is either supported by SQLite natively, or one that can be lowered into a SQLite-value given additional
 * context.
 */
export type SqliteInputValue = SqliteValue | CustomSqliteValue;

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
export type SqliteRow<T = SqliteValue> = { [column: string]: T };

export type SqliteInputRow = SqliteRow<SqliteInputValue>;

/**
 * SQLite-compatible row (NULL, TEXT, INTEGER, REAL, BLOB).
 * JSON is represented as TEXT.
 *
 * Toasted values are `undefined`.
 */
export type ToastableSqliteRow<V = SqliteValue> = SqliteRow<V | undefined>;

/**
 * A value as received from the database.
 */
export type DatabaseInputValue =
  | SqliteValue
  | boolean
  | DatabaseInputValue[]
  | JsonContainer
  | CustomSqliteValue
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

export interface EvaluateRowOptions extends TableRow {}

/**
 * A row associated with the table it's coming from.
 */
export interface TableRow<R = SqliteRow> {
  sourceTable: SourceTableInterface;
  record: R;
}

export interface BaseClause {
  /**
   * If this clause is composed from semantic child-clauses, calls the given callback function for each child.
   */
  visitChildren?: (visitor: (clause: CompiledClause) => void) => void;
}

export type StaticFilter =
  | StaticOperator
  | AndOperator
  | OrOperator
  | AnyOperator
  | StaticColumnExpression
  | StaticValue;

export interface AnyOperator {
  any: true;
}

export function isAny(filter: StaticFilter): filter is AnyOperator {
  return (filter as AnyOperator).any === true;
}

export interface StaticOperator {
  left: StaticFilter;
  operator: string; // '=' | '!=' | '<' | '<=' | '>' | '>=' | 'IS' | 'IS NOT' | 'IN';
  right: StaticFilter;
}

export interface StaticValue {
  value: SqliteValue;
}

export interface StaticColumnExpression {
  column: string;
}

export interface AndOperator {
  and: StaticFilter[];
}

export interface OrOperator {
  or: StaticFilter[];
}

export type MongoExpression =
  | Record<string, unknown>
  | unknown[]
  | string
  | number
  | boolean
  | null
  | bigint
  | Uint8Array;

export interface StaticFilterToMongoOptions {
  mapValue?: (value: SqliteValue) => unknown;
  parseJsonArrayForIn?: boolean;
}

export function staticFilterToMongoExpression(
  filter: StaticFilter,
  options: StaticFilterToMongoOptions = {}
): MongoExpression {
  const mapValue = options.mapValue ?? ((value: SqliteValue) => value);
  const parseJsonArrayForIn = options.parseJsonArrayForIn ?? true;
  // Marker for "cannot prefilter" so we avoid over-restricting the query.
  const ANY = Symbol('static-filter-any');

  const literalValue = (value: SqliteValue): MongoExpression => ({ $literal: mapValue(value) });

  const toExpr = (node: StaticFilter): MongoExpression | typeof ANY => {
    if (isAny(node)) {
      return ANY;
    }

    if ('and' in node) {
      const parts = node.and.map(toExpr).filter((part) => part !== ANY) as MongoExpression[];
      if (parts.length === 0) {
        return ANY;
      }
      if (parts.length === 1) {
        return parts[0];
      }
      return { $and: parts };
    }

    if ('or' in node) {
      const parts = node.or.map(toExpr);
      if (parts.some((part) => part === ANY)) {
        return ANY;
      }
      const expressions = parts.filter((part) => part !== ANY) as MongoExpression[];
      if (expressions.length === 0) {
        return { $const: false };
      }
      if (expressions.length === 1) {
        return expressions[0];
      }
      return { $or: expressions };
    }

    if ('operator' in node) {
      const left = toExpr(node.left);
      const right = toExpr(node.right);
      if (left === ANY || right === ANY) {
        return ANY;
      }

      const leftExpr = left as MongoExpression;
      let rightExpr = right as MongoExpression;
      const operator = node.operator.toUpperCase();

      switch (operator) {
        case '=':
        case '==':
          return { $eq: [leftExpr, rightExpr] };
        case '!=':
        case '<>':
          return { $ne: [leftExpr, rightExpr] };
        case '<':
          return { $lt: [leftExpr, rightExpr] };
        case '<=':
          return { $lte: [leftExpr, rightExpr] };
        case '>':
          return { $gt: [leftExpr, rightExpr] };
        case '>=':
          return { $gte: [leftExpr, rightExpr] };
        case 'IS':
          return { $eq: [leftExpr, rightExpr] };
        case 'IS NOT':
          return { $ne: [leftExpr, rightExpr] };
        case 'IN': {
          if (parseJsonArrayForIn && 'value' in node.right) {
            const value = node.right.value;
            if (typeof value !== 'string') {
              throw new Error('IN operator expects JSON array literal');
            }
            let parsed: unknown;
            try {
              parsed = JSON.parse(value);
            } catch {
              throw new Error('IN operator expects JSON array literal');
            }
            if (!Array.isArray(parsed)) {
              throw new Error('IN operator expects JSON array literal');
            }
            rightExpr = { $literal: parsed };
          }
          return { $in: [leftExpr, rightExpr] };
        }
        default:
          throw new Error(`Operator not supported: ${node.operator}`);
      }
    }

    if ('column' in node) {
      return `$${node.column}`;
    }

    if ('value' in node) {
      return literalValue(node.value);
    }

    throw new Error('Unsupported static filter');
  };

  const result = toExpr(filter);
  return result === ANY ? { $const: true } : result;
}

/**
 * This is a clause that matches row and parameter values for equality.
 *
 * Example:
 * [WHERE] users.org_id = bucket.org_id
 *
 * For a given a row, this produces a set of parameters that would make the clause evaluate to true.
 */
export interface ParameterMatchClause extends BaseClause {
  error: boolean;

  /**
   * This is a filter that can be applied "statically" on the row, to pre-filter rows.
   *
   * This may be under-specific, i.e. it may include rows that do not actually match the full filter.
   */
  staticFilter: StaticFilter;

  specialType?: 'or';

  /**
   * The parameter fields that are used for this filter, for example:
   *  * ['bucket.region_id'] for a data query
   *  * ['token_parameters.user_id'] for a parameter query
   *
   * These parameters are always matched by this clause, and no additional parameters are matched.
   *
   * For a single match clause, this array will have a single element. When match clauses are combined with `AND`,
   * the result is represented as a {@link ParameterMatchClause} with multiple input parameters.
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
export interface ParameterValueClause extends BaseClause {
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

/**
 * A {@link ParameterValueClause} created by selecting from the legacy `token_parameters` or `user_parameters` tables.
 */
export interface LegacyParameterFromTableClause extends ParameterValueClause {
  table: string;
}

export function isLegacyParameterFromTableClause(clause: CompiledClause): clause is LegacyParameterFromTableClause {
  return (clause as LegacyParameterFromTableClause).table != null;
}

export interface QuerySchema {
  /**
   * @param table The unaliased table, as it appears in the source schema.
   * @param column Name of the column to look up.
   */
  getColumn(table: string, column: string): ColumnDefinition | undefined;
  /**
   *
   * @param table The unaliased table, as it appears in the source schema.
   */
  getColumns(table: string): ColumnDefinition[];
}

/**
 * A clause that uses row values as input.
 *
 * For parameter queries, that is the parameter table being queried.
 * For data queries, that is the data table being queried.
 */
export interface RowValueClause extends BaseClause {
  evaluate(tables: QueryParameters): SqliteValue;
  getColumnDefinition(schema: QuerySchema): ColumnDefinition | undefined;
  staticFilter: StaticFilter;
}

/**
 * Completely static value.
 *
 * Extends RowValueClause and ParameterValueClause to simplify code in some places.
 */
export interface StaticValueClause extends RowValueClause, ParameterValueClause {
  readonly value: SqliteValue;
}

export interface ClauseError extends BaseClause {
  error: true;
}

export type CompiledClause =
  | RowValueClause
  | ParameterMatchClause
  | ParameterValueClause
  | RequestFunctionCall // extends ParameterValueClause
  | LegacyParameterFromTableClause // extends ParameterValueClause
  | ClauseError;

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
