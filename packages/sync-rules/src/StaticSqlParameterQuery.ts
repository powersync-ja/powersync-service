import { SelectedColumn, SelectFromStatement } from 'pgsql-ast-parser';
import { BucketDescription, BucketPriority, DEFAULT_BUCKET_PRIORITY } from './BucketDescription.js';
import { SqlRuleError } from './errors.js';
import { AvailableTable, SqlTools } from './sql_filters.js';
import { checkUnsupportedFeatures, isClauseError, isParameterValueClause, sqliteBool } from './sql_support.js';
import {
  BucketIdTransformer,
  ParameterValueClause,
  QueryParseOptions,
  RequestParameters,
  SqliteJsonValue
} from './types.js';
import { getBucketId, isJsonValue } from './utils.js';

export interface StaticSqlParameterQueryOptions {
  sql: string;
  parameterExtractors: Record<string, ParameterValueClause>;
  priority: BucketPriority;
  descriptorName: string;
  bucketParameters: string[];
  queryId: string;
  filter: ParameterValueClause | undefined;
  errors?: SqlRuleError[];
}

/**
 * Represents a bucket parameter query without any tables, e.g.:
 *
 *    SELECT token_parameters.user_id
 *    SELECT token_parameters.user_id as user_id WHERE token_parameters.is_admin
 */
export class StaticSqlParameterQuery {
  static fromSql(
    descriptorName: string,
    sql: string,
    q: SelectFromStatement,
    options: QueryParseOptions,
    queryId: string
  ) {
    let errors: SqlRuleError[] = [];

    errors.push(...checkUnsupportedFeatures(sql, q));

    const tools = new SqlTools({
      table: undefined,
      parameterTables: [new AvailableTable('token_parameters'), new AvailableTable('user_parameters')],
      supportsParameterExpressions: true,
      compatibilityContext: options.compatibility,
      sql
    });
    const where = q.where;

    const filter = tools.compileParameterValueExtractor(where);
    const columns = q.columns ?? [];
    const bucketParameters = (q.columns ?? [])
      .map((column) => tools.getOutputName(column))
      .filter((c) => !tools.isBucketPriorityParameter(c));

    let priority = options?.priority;
    let parameterExtractors: Record<string, ParameterValueClause> = {};

    for (let column of columns) {
      if (column.alias != null) {
        tools.checkSpecificNameCase(column.alias);
      }
      const name = tools.getSpecificOutputName(column);
      if (tools.isBucketPriorityParameter(name)) {
        if (priority !== undefined) {
          errors.push(new SqlRuleError('Cannot set priority multiple times.', sql));
          continue;
        }

        priority = tools.extractBucketPriority(column.expr);
        continue;
      }

      const extractor = tools.compileParameterValueExtractor(column.expr);
      if (isClauseError(extractor)) {
        // Error logged already
        continue;
      }
      parameterExtractors[name] = extractor;
    }

    errors.push(...tools.errors);

    const query = new StaticSqlParameterQuery({
      sql,
      descriptorName,
      bucketParameters,
      parameterExtractors,
      priority: priority ?? DEFAULT_BUCKET_PRIORITY,
      filter: isClauseError(filter) ? undefined : filter,
      queryId,
      errors
    });
    if (query.usesDangerousRequestParameters && !options?.accept_potentially_dangerous_queries) {
      let err = new SqlRuleError(
        "Potentially dangerous query based on parameters set by the client. The client can send any value for these parameters so it's not a good place to do authorization.",
        sql
      );
      err.type = 'warning';
      query.errors.push(err);
    }
    return query;
  }

  /**
   * Raw source sql query, for debugging purposes.
   */
  readonly sql: string;

  /**
   * Matches the keys in `bucketParameters`.
   *
   * This is used to map request parameters -> bucket parameters.
   */
  readonly parameterExtractors: Record<string, ParameterValueClause>;

  readonly priority: BucketPriority;

  /**
   * Bucket definition name.
   */
  readonly descriptorName: string;

  /**
   * _Output_ bucket parameters, excluding the `bucket.` prefix.
   *
   * Each one will be present in the `parameterExtractors` map.
   */
  readonly bucketParameters: string[];

  /**
   * Unique identifier for this query within a bucket definition.
   *
   * Typically auto-generated based on query order.
   *
   * This is not used directly, but we keep this to match behavior of other parameter queries.
   */
  readonly queryId: string;

  /**
   * The query filter (WHERE clause). Given request parameters, the filter will determine whether or not this query returns a row.
   *
   * undefined if the clause is not valid.
   */
  readonly filter: ParameterValueClause | undefined;

  readonly errors: SqlRuleError[];

  constructor(options: StaticSqlParameterQueryOptions) {
    this.sql = options.sql;
    this.parameterExtractors = options.parameterExtractors;
    this.priority = options.priority;
    this.descriptorName = options.descriptorName;
    this.bucketParameters = options.bucketParameters;
    this.queryId = options.queryId;
    this.filter = options.filter;
    this.errors = options.errors ?? [];
  }

  getStaticBucketDescriptions(parameters: RequestParameters, transformer: BucketIdTransformer): BucketDescription[] {
    if (this.filter == null) {
      // Error in filter clause
      return [];
    }
    const filterValue = this.filter.lookupParameterValue(parameters);
    if (sqliteBool(filterValue) === 0n) {
      return [];
    }

    let result: Record<string, SqliteJsonValue> = {};
    for (let name of this.bucketParameters) {
      const value = this.parameterExtractors[name].lookupParameterValue(parameters);
      if (isJsonValue(value)) {
        result[`bucket.${name}`] = value;
      } else {
        // Not valid.
        // Should we error instead?
        return [];
      }
    }

    return [
      {
        bucket: getBucketId(this.descriptorName, this.bucketParameters, result, transformer),
        priority: this.priority
      }
    ];
  }

  get hasAuthenticatedBucketParameters(): boolean {
    // select where request.jwt() ->> 'role' == 'authorized'
    // we do not count this as a sufficient check
    // const authenticatedFilter = this.filter.usesAuthenticatedRequestParameters;

    // select request.user_id() as user_id
    const authenticatedExtractor =
      Object.values(this.parameterExtractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesAuthenticatedRequestParameters
      ) != null;
    return authenticatedExtractor;
  }

  get usesUnauthenticatedRequestParameters(): boolean {
    // select where request.parameters() ->> 'include_comments'
    const unauthenticatedFilter = this.filter?.usesUnauthenticatedRequestParameters;

    // select request.parameters() ->> 'project_id'
    const unauthenticatedExtractor =
      Object.values(this.parameterExtractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesUnauthenticatedRequestParameters
      ) != null;

    return unauthenticatedFilter || unauthenticatedExtractor;
  }

  get usesDangerousRequestParameters() {
    return this.usesUnauthenticatedRequestParameters && !this.hasAuthenticatedBucketParameters;
  }
}
