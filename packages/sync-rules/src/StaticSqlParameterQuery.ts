import { SelectedColumn, SelectFromStatement } from 'pgsql-ast-parser';
import { BucketDescription, BucketPriority, DEFAULT_BUCKET_PRIORITY } from './BucketDescription.js';
import { SqlRuleError } from './errors.js';
import { SqlTools } from './sql_filters.js';
import { checkUnsupportedFeatures, isClauseError, isParameterValueClause, sqliteBool } from './sql_support.js';
import { ParameterValueClause, QueryParseOptions, RequestParameters, SqliteJsonValue } from './types.js';
import { getBucketId, isJsonValue } from './utils.js';

export interface StaticSqlParameterQueryOptions {
  sql: string;
  columns: SelectedColumn[];
  parameter_extractors: Record<string, ParameterValueClause>;
  priority: BucketPriority;
  descriptor_name: string;
  /** _Output_ bucket parameters */
  bucket_parameters: string[];
  id: string;
  tools: SqlTools;

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
    descriptor_name: string,
    sql: string,
    q: SelectFromStatement,
    options: QueryParseOptions,
    queryId: string
  ) {
    let errors: SqlRuleError[] = [];

    errors.push(...checkUnsupportedFeatures(sql, q));

    const tools = new SqlTools({
      table: undefined,
      parameter_tables: ['token_parameters', 'user_parameters'],
      supports_parameter_expressions: true,
      sql
    });
    const where = q.where;

    const filter = tools.compileParameterValueExtractor(where);
    const columns = q.columns ?? [];
    const bucket_parameters = (q.columns ?? [])
      .map((column) => tools.getOutputName(column))
      .filter((c) => !tools.isBucketPriorityParameter(c));

    let priority = options?.priority;
    let parameter_extractors: Record<string, ParameterValueClause> = {};

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
      parameter_extractors[name] = extractor;
    }

    errors.push(...tools.errors);

    const query = new StaticSqlParameterQuery({
      sql,
      descriptor_name,
      bucket_parameters,
      columns,
      parameter_extractors,
      tools,
      priority: priority ?? DEFAULT_BUCKET_PRIORITY,
      filter: isClauseError(filter) ? undefined : filter,
      id: queryId,
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

  readonly sql: string;
  readonly columns: SelectedColumn[];
  readonly parameter_extractors: Record<string, ParameterValueClause>;
  readonly priority: BucketPriority;
  readonly descriptor_name: string;
  /** _Output_ bucket parameters */
  readonly bucket_parameters: string[];
  readonly id: string;
  readonly tools: SqlTools;

  readonly filter: ParameterValueClause | undefined;

  readonly errors: SqlRuleError[];

  constructor(options: StaticSqlParameterQueryOptions) {
    this.sql = options.sql;
    this.columns = options.columns;
    this.parameter_extractors = options.parameter_extractors;
    this.priority = options.priority;
    this.descriptor_name = options.descriptor_name;
    this.bucket_parameters = options.bucket_parameters;
    this.id = options.id;
    this.tools = options.tools;
    this.filter = options.filter;
    this.errors = options.errors ?? [];
  }

  getStaticBucketDescriptions(parameters: RequestParameters): BucketDescription[] {
    if (this.filter == null) {
      // Error in filter clause
      return [];
    }
    const filterValue = this.filter.lookupParameterValue(parameters);
    if (sqliteBool(filterValue) === 0n) {
      return [];
    }

    let result: Record<string, SqliteJsonValue> = {};
    for (let name of this.bucket_parameters) {
      const value = this.parameter_extractors[name].lookupParameterValue(parameters);
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
        bucket: getBucketId(this.descriptor_name, this.bucket_parameters, result),
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
      Object.values(this.parameter_extractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesAuthenticatedRequestParameters
      ) != null;
    return authenticatedExtractor;
  }

  get usesUnauthenticatedRequestParameters(): boolean {
    // select where request.parameters() ->> 'include_comments'
    const unauthenticatedFilter = this.filter?.usesUnauthenticatedRequestParameters;

    // select request.parameters() ->> 'project_id'
    const unauthenticatedExtractor =
      Object.values(this.parameter_extractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesUnauthenticatedRequestParameters
      ) != null;

    return unauthenticatedFilter || unauthenticatedExtractor;
  }

  get usesDangerousRequestParameters() {
    return this.usesUnauthenticatedRequestParameters && !this.hasAuthenticatedBucketParameters;
  }
}
