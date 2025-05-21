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
    descriptor_name: string,
    sql: string,
    call: FromCall,
    q: SelectFromStatement,
    options?: QueryParseOptions
  ): TableValuedFunctionSqlParameterQuery {
    const query = new TableValuedFunctionSqlParameterQuery();

    query.errors.push(...checkUnsupportedFeatures(sql, q));

    if (!(call.function.name in TABLE_VALUED_FUNCTIONS)) {
      query.errors.push(new SqlRuleError(`Table-valued function ${call.function.name} is not defined.`, sql, call));
      return query;
    }

    const callTable = call.alias?.name ?? call.function.name;
    const callExpression = call.args[0];

    const tools = new SqlTools({
      table: callTable,
      parameter_tables: ['token_parameters', 'user_parameters', callTable],
      supports_parameter_expressions: true,
      sql
    });
    const where = q.where;

    const filter = tools.compileParameterValueExtractor(where);
    const callClause = tools.compileParameterValueExtractor(callExpression);
    const columns = q.columns ?? [];
    const bucket_parameters = columns.map((column) => tools.getOutputName(column));

    query.sql = sql;
    query.descriptor_name = descriptor_name;
    query.bucket_parameters = bucket_parameters;
    query.columns = columns;
    query.tools = tools;
    query.function = TABLE_VALUED_FUNCTIONS[call.function.name]!;
    query.callTableName = callTable;
    if (!isClauseError(callClause)) {
      query.callClause = callClause;
    }
    if (!isClauseError(filter)) {
      query.filter = filter;
    }

    for (let column of columns) {
      if (column.alias != null) {
        tools.checkSpecificNameCase(column.alias);
      }
      const name = tools.getSpecificOutputName(column);
      if (tools.isBucketPriorityParameter(name)) {
        query.priority = tools.extractBucketPriority(column.expr);
        continue;
      }

      const extractor = tools.compileParameterValueExtractor(column.expr);
      if (isClauseError(extractor)) {
        // Error logged already
        continue;
      }
      query.parameter_extractors[name] = extractor;
    }

    query.errors.push(...tools.errors);

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

  sql?: string;
  columns?: SelectedColumn[];
  parameter_extractors: Record<string, ParameterValueClause> = {};
  priority?: BucketPriority;
  descriptor_name?: string;
  /** _Output_ bucket parameters */
  bucket_parameters?: string[];
  id?: string;
  tools?: SqlTools;

  filter?: ParameterValueClause;
  callClause?: ParameterValueClause;
  function?: TableValuedFunction;
  callTableName?: string;

  errors: SqlRuleError[] = [];

  getStaticBucketDescriptions(parameters: RequestParameters): BucketDescription[] {
    if (this.filter == null || this.callClause == null) {
      // Error in filter clause
      return [];
    }

    const valueString = this.callClause.lookupParameterValue(parameters);
    const rows = this.function!.call([valueString]);
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
      raw_token_payload: parameters.raw_token_payload,
      raw_user_parameters: parameters.raw_user_parameters,
      user_id: parameters.user_id,
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
    for (let name of this.bucket_parameters!) {
      const value = this.parameter_extractors[name].lookupParameterValue(mergedParams);
      if (isJsonValue(value)) {
        result[`bucket.${name}`] = value;
      } else {
        throw new Error(`Invalid parameter value: ${value}`);
      }
    }

    return {
      bucket: getBucketId(this.descriptor_name!, this.bucket_parameters!, result),
      priority: this.priority ?? DEFAULT_BUCKET_PRIORITY
    };
  }

  get hasAuthenticatedBucketParameters(): boolean {
    // select where request.jwt() ->> 'role' == 'authorized'
    // we do not count this as a sufficient check
    // const authenticatedFilter = this.filter!.usesAuthenticatedRequestParameters;

    // select request.user_id() as user_id
    const authenticatedExtractor =
      Object.values(this.parameter_extractors).find(
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
      Object.values(this.parameter_extractors).find(
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
