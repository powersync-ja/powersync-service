import { FromCall, SelectedColumn, SelectFromStatement } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { SqlTools } from './sql_filters.js';
import { checkUnsupportedFeatures, isClauseError, isParameterValueClause, sqliteBool } from './sql_support.js';
import { TABLE_VALUED_FUNCTIONS, TableValuedFunction } from './TableValuedFunctions.js';
import {
  ParameterValueClause,
  ParameterValueSet,
  QueryParseOptions,
  RequestParameters,
  SqliteJsonValue,
  SqliteRow
} from './types.js';
import { getBucketId, isJsonValue } from './utils.js';
import { BucketDescription, BucketPriority, DEFAULT_BUCKET_PRIORITY } from './BucketDescription.js';

export interface TableValuedFunctionSqlParameterQueryOptions {
  sql: string;
  parameterExtractors: Record<string, ParameterValueClause>;
  priority: BucketPriority;
  descriptorName: string;
  bucketParameters: string[];
  queryId: string;

  filter: ParameterValueClause | undefined;
  callClause: ParameterValueClause | undefined;
  function: TableValuedFunction;
  callTableName: string;

  errors: SqlRuleError[];
}

/**
 * Represents a parameter query using a table-valued function.
 *
 * Right now this only supports json_each:
 *
 *    SELECT json_each.value as v FROM json_each(request.parameters() -> 'array')
 *
 * This can currently not be combined with parameter table queries or multiple table-valued functions.
 */
export class TableValuedFunctionSqlParameterQuery {
  static fromSql(
    descriptorName: string,
    sql: string,
    call: FromCall,
    q: SelectFromStatement,
    options: QueryParseOptions,
    queryId: string
  ): TableValuedFunctionSqlParameterQuery {
    let errors: SqlRuleError[] = [];

    errors.push(...checkUnsupportedFeatures(sql, q));

    if (!(call.function.name in TABLE_VALUED_FUNCTIONS)) {
      throw new SqlRuleError(`Table-valued function ${call.function.name} is not defined.`, sql, call);
    }

    const callTable = call.alias?.name ?? call.function.name;
    const callExpression = call.args[0];

    const tools = new SqlTools({
      table: callTable,
      parameterTables: ['token_parameters', 'user_parameters', callTable],
      supportsParameterExpressions: true,
      sql
    });
    const where = q.where;

    const filter = tools.compileParameterValueExtractor(where);
    const callClause = tools.compileParameterValueExtractor(callExpression);
    const columns = q.columns ?? [];
    const bucketParameters = columns.map((column) => tools.getOutputName(column));

    const functionImpl = TABLE_VALUED_FUNCTIONS[call.function.name]!;
    let priority = options.priority;
    let parameterExtractors: Record<string, ParameterValueClause> = {};

    for (let column of columns) {
      if (column.alias != null) {
        tools.checkSpecificNameCase(column.alias);
      }
      const name = tools.getSpecificOutputName(column);
      if (tools.isBucketPriorityParameter(name)) {
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

    const query = new TableValuedFunctionSqlParameterQuery({
      sql,
      descriptorName,
      bucketParameters,
      parameterExtractors,
      filter: isClauseError(filter) ? undefined : filter,
      callClause: isClauseError(callClause) ? undefined : callClause,
      function: functionImpl,
      callTableName: callTable,
      priority: priority ?? DEFAULT_BUCKET_PRIORITY,
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
   * This is used to map (request parameters + individual function call result row) -> bucket parameters.
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
   * The WHERE clause. This is applied on (request parameters + individual function call result row).
   *
   * This is used to determine whether or not this query returns a row.
   *
   * undefined if the clause is not valid.
   */
  readonly filter: ParameterValueClause | undefined;

  /**
   * This is the argument to the table-valued function. It is evaluated on the request parameters.
   *
   * Only a single argument is supported currently.
   */
  readonly callClause: ParameterValueClause | undefined;

  /**
   * The table-valued function that will be called, with the output of `callClause`.
   */
  readonly function: TableValuedFunction;

  /**
   * The name or alias of the "table" with the function call results.
   *
   * Only used internally.
   */
  readonly callTableName: string;

  readonly errors: SqlRuleError[];

  constructor(options: TableValuedFunctionSqlParameterQueryOptions) {
    this.sql = options.sql;
    this.parameterExtractors = options.parameterExtractors;
    this.priority = options.priority;
    this.descriptorName = options.descriptorName;
    this.bucketParameters = options.bucketParameters;
    this.queryId = options.queryId;

    this.filter = options.filter;
    this.callClause = options.callClause;
    this.function = options.function;
    this.callTableName = options.callTableName;

    this.errors = options.errors;
  }

  getStaticBucketDescriptions(parameters: RequestParameters): BucketDescription[] {
    if (this.filter == null || this.callClause == null) {
      // Error in filter clause
      return [];
    }

    const valueString = this.callClause.lookupParameterValue(parameters);
    const rows = this.function.call([valueString]);
    let total: BucketDescription[] = [];
    for (let row of rows) {
      const description = this.getIndividualBucketDescription(row, parameters);
      if (description !== null) {
        total.push(description);
      }
    }
    return total;
  }

  private getIndividualBucketDescription(row: SqliteRow, parameters: RequestParameters): BucketDescription | null {
    const mergedParams: ParameterValueSet = {
      ...parameters,
      lookup: (table, column) => {
        if (table == this.callTableName) {
          return row[column]!;
        } else {
          return parameters.lookup(table, column);
        }
      }
    };
    const filterValue = this.filter!.lookupParameterValue(mergedParams);
    if (sqliteBool(filterValue) === 0n) {
      return null;
    }

    let result: Record<string, SqliteJsonValue> = {};
    for (let name of this.bucketParameters) {
      const value = this.parameterExtractors[name].lookupParameterValue(mergedParams);
      if (isJsonValue(value)) {
        result[`bucket.${name}`] = value;
      } else {
        throw new Error(`Invalid parameter value: ${value}`);
      }
    }

    return {
      definition: this.descriptorName,
      bucket: getBucketId(this.descriptorName, this.bucketParameters, result),
      priority: this.priority
    };
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

    // select value from json_each(request.jwt() ->> 'project_ids')
    const authenticatedArgument = this.callClause?.usesAuthenticatedRequestParameters ?? false;

    return authenticatedExtractor || authenticatedArgument;
  }

  get usesUnauthenticatedRequestParameters(): boolean {
    // select where request.parameters() ->> 'include_comments'
    const unauthenticatedFilter = this.filter?.usesUnauthenticatedRequestParameters;

    // select request.parameters() ->> 'project_id'
    const unauthenticatedExtractor =
      Object.values(this.parameterExtractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesUnauthenticatedRequestParameters
      ) != null;

    // select value from json_each(request.parameters() ->> 'project_ids')
    const unauthenticatedArgument = this.callClause?.usesUnauthenticatedRequestParameters ?? false;

    return unauthenticatedFilter || unauthenticatedExtractor || unauthenticatedArgument;
  }

  get usesDangerousRequestParameters() {
    return this.usesUnauthenticatedRequestParameters && !this.hasAuthenticatedBucketParameters;
  }
}
