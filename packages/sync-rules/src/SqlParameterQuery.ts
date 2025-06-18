import { parse, SelectedColumn } from 'pgsql-ast-parser';
import { BucketDescription, BucketPriority, DEFAULT_BUCKET_PRIORITY } from './BucketDescription.js';
import { BucketParameterQuerier, ParameterLookup, ParameterLookupSource } from './BucketParameterQuerier.js';
import { SqlRuleError } from './errors.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlTools } from './sql_filters.js';
import { checkUnsupportedFeatures, isClauseError, isParameterValueClause } from './sql_support.js';
import { StaticSqlParameterQuery } from './StaticSqlParameterQuery.js';
import { TablePattern } from './TablePattern.js';
import { TableQuerySchema } from './TableQuerySchema.js';
import { TableValuedFunctionSqlParameterQuery } from './TableValuedFunctionSqlParameterQuery.js';
import {
  EvaluatedParameters,
  EvaluatedParametersResult,
  InputParameter,
  ParameterMatchClause,
  ParameterValueClause,
  QueryParseOptions,
  QuerySchema,
  RequestParameters,
  RowValueClause,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow
} from './types.js';
import { filterJsonRow, getBucketId, isJsonValue, isSelectStatement, normalizeParameterValue } from './utils.js';

export interface SqlParameterQueryOptions {
  sourceTable: TablePattern;
  table: string;
  sql: string;
  lookupExtractors: Record<string, RowValueClause>;
  parameterExtractors: Record<string, ParameterValueClause>;
  priority: BucketPriority;
  filter: ParameterMatchClause;
  descriptorName: string;
  inputParameters: InputParameter[];
  expandedInputParameter: InputParameter | undefined;
  bucketParameters: string[];
  queryId: string;
  tools: SqlTools;
  errors?: SqlRuleError[];
}

/**
 * Represents a parameter query, such as:
 *
 *  SELECT id as user_id FROM users WHERE users.user_id = token_parameters.user_id
 *  SELECT id as user_id, token_parameters.is_admin as is_admin FROM users WHERE users.user_id = token_parameters.user_id
 */
export class SqlParameterQuery {
  static fromSql(
    descriptorName: string,
    sql: string,
    options: QueryParseOptions,
    queryId: string
  ): SqlParameterQuery | StaticSqlParameterQuery | TableValuedFunctionSqlParameterQuery {
    const parsed = parse(sql, { locationTracking: true });
    const schema = options?.schema;

    if (parsed.length > 1) {
      throw new SqlRuleError('Only a single SELECT statement is supported', sql, parsed[1]?._location);
    }
    const q = parsed[0];

    if (!isSelectStatement(q)) {
      throw new SqlRuleError('Only SELECT statements are supported', sql, q._location);
    }

    if (q.from == null) {
      // E.g. SELECT token_parameters.user_id as user_id WHERE token_parameters.is_admin
      return StaticSqlParameterQuery.fromSql(descriptorName, sql, q, options, queryId);
    }

    let errors: SqlRuleError[] = [];

    errors.push(...checkUnsupportedFeatures(sql, q));

    if (q.from.length != 1) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    } else if (q.from[0].type == 'call') {
      const from = q.from[0];
      return TableValuedFunctionSqlParameterQuery.fromSql(descriptorName, sql, from, q, options, queryId);
    } else if (q.from[0].type == 'statement') {
      throw new SqlRuleError('Subqueries are not supported yet', sql, q.from?.[0]._location);
    }

    const tableRef = q.from[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }
    const alias: string = q.from?.[0].name.alias ?? tableRef.name;
    if (tableRef.name != alias) {
      errors.push(new SqlRuleError('Table aliases not supported in parameter queries', sql, q.from?.[0]._location));
    }
    const sourceTable = new TablePattern(tableRef.schema ?? options.defaultSchema, tableRef.name);
    let querySchema: QuerySchema | undefined = undefined;
    if (schema) {
      const tables = schema.getTables(sourceTable);
      if (tables.length == 0) {
        const e = new SqlRuleError(
          `Table ${sourceTable.schema}.${sourceTable.tablePattern} not found`,
          sql,
          q.from?.[0]?._location
        );
        e.type = 'warning';

        errors.push(e);
      } else {
        querySchema = new TableQuerySchema(tables, alias);
      }
    }

    const tools = new SqlTools({
      table: alias,
      parameterTables: ['token_parameters', 'user_parameters'],
      sql,
      supportsExpandingParameters: true,
      supportsParameterExpressions: true,
      schema: querySchema
    });
    tools.checkSpecificNameCase(tableRef);
    const where = q.where;
    const filter = tools.compileWhereClause(where);

    const bucketParameters = (q.columns ?? [])
      .map((column) => tools.getOutputName(column))
      .filter((c) => !tools.isBucketPriorityParameter(c));

    let priority: BucketPriority | undefined = options.priority;
    let lookupExtractors: Record<string, RowValueClause> = {};
    let parameterExtractors: Record<string, ParameterValueClause> = {};

    for (let column of q.columns ?? []) {
      const name = tools.getSpecificOutputName(column);
      if (column.alias != null) {
        tools.checkSpecificNameCase(column.alias);
      }
      if (tools.isBucketPriorityParameter(name)) {
        if (priority !== undefined) {
          errors.push(new SqlRuleError('Cannot set priority multiple times.', sql));
          continue;
        }

        priority = tools.extractBucketPriority(column.expr);
      } else if (tools.isTableRef(column.expr)) {
        const extractor = tools.compileRowValueExtractor(column.expr);
        if (isClauseError(extractor)) {
          // Error logged already
          continue;
        }
        lookupExtractors[name] = extractor;
      } else {
        const extractor = tools.compileParameterValueExtractor(column.expr);
        if (isClauseError(extractor)) {
          // Error logged already
          continue;
        }
        parameterExtractors[name] = extractor;
      }
    }
    errors.push(...tools.errors);

    const expandedParams = filter.inputParameters.filter((param) => param.expands);
    if (expandedParams.length > 1) {
      errors.push(new SqlRuleError('Cannot have multiple array input parameters', sql));
    }

    const parameterQuery = new SqlParameterQuery({
      sourceTable,
      table: alias,
      sql,
      lookupExtractors,
      parameterExtractors,
      priority: priority ?? DEFAULT_BUCKET_PRIORITY,
      filter,
      descriptorName,
      inputParameters: filter.inputParameters,
      expandedInputParameter: expandedParams[0],
      bucketParameters,
      queryId,
      tools,
      errors
    });

    if (parameterQuery.usesDangerousRequestParameters && !options.accept_potentially_dangerous_queries) {
      let err = new SqlRuleError(
        "Potentially dangerous query based on parameters set by the client. The client can send any value for these parameters so it's not a good place to do authorization.",
        sql
      );
      err.type = 'warning';
      parameterQuery.errors.push(err);
    }
    return parameterQuery;
  }

  /**
   * The table the parameter query queries from.
   *
   * Currently, no wildcards are supported here.
   */
  readonly sourceTable: TablePattern;

  /**
   * The table name or alias, as referred to in the SQL query.
   * Not used directly outside the query.
   *
   * Currently, this always matches sourceTable.name.
   */
  readonly table: string;

  /**
   * The source SQL query, for debugging purposes.
   */
  readonly sql: string;

  /**
   * Example: SELECT *user.id* FROM users WHERE ...
   *
   * These are applied onto the replicated parameter table rows, returning lookup values.
   */
  readonly lookupExtractors: Record<string, RowValueClause>;

  /**
   * Example: SELECT *token_parameters.user_id*.
   *
   * These are applied onto the request parameters.
   */
  readonly parameterExtractors: Record<string, ParameterValueClause>;

  readonly priority: BucketPriority;

  /**
   * This is the entire where clause.
   *
   * This can convert a parameter row into a set of parameter values, that would make the where clause match.
   * Those are then persisted to lookup later.
   */
  readonly filter: ParameterMatchClause;

  /**
   * Bucket definition name.
   */
  readonly descriptorName: string;

  /**
   * _Input_ token / user parameters - the parameters passed into the parameter query.
   *
   * These "pre-process" the parameters.
   */
  readonly inputParameters: InputParameter[];

  /**
   * If specified, an input parameter that expands to an array. Currently, only one parameter
   * may is allowed to expand to an array
   */
  readonly expandedInputParameter: InputParameter | undefined;

  /**
   * _Output_ bucket parameters, excluding the `bucket.` prefix.
   *
   * Each one of these will be present in either lookupExtractors or parameterExtractors.
   */
  readonly bucketParameters: string[];

  /**
   * Unique identifier for this query within a bucket definition.
   *
   * Typically auto-generated based on query order.
   *
   * This is used when persisting lookup values.
   */
  readonly queryId: string;
  readonly tools: SqlTools;

  readonly errors: SqlRuleError[];

  constructor(options: SqlParameterQueryOptions) {
    this.sourceTable = options.sourceTable;
    this.table = options.table;
    this.sql = options.sql;
    this.lookupExtractors = options.lookupExtractors;
    this.parameterExtractors = options.parameterExtractors;
    this.priority = options.priority;
    this.filter = options.filter;
    this.descriptorName = options.descriptorName;
    this.inputParameters = options.inputParameters;
    this.expandedInputParameter = options.expandedInputParameter;
    this.bucketParameters = options.bucketParameters;
    this.queryId = options.queryId;
    this.tools = options.tools;
    this.errors = options.errors ?? [];
  }

  applies(table: SourceTableInterface) {
    return this.sourceTable.matches(table);
  }

  /**
   * Given a replicated row, results an array of bucket parameter rows to persist.
   */
  evaluateParameterRow(row: SqliteRow): EvaluatedParametersResult[] {
    const tables = {
      [this.table]: row
    };
    try {
      const filterParameters = this.filter.filterRow(tables);
      let result: EvaluatedParametersResult[] = [];
      for (let filterParamSet of filterParameters) {
        let lookup: SqliteJsonValue[] = [this.descriptorName, this.queryId];
        lookup.push(
          ...this.inputParameters.map((param) => {
            return normalizeParameterValue(param.filteredRowToLookupValue(filterParamSet));
          })
        );

        const data = this.transformRows(row);

        const role: EvaluatedParameters = {
          bucketParameters: data.map((row) => filterJsonRow(row)),
          lookup: new ParameterLookup(lookup)
        };
        result.push(role);
      }
      return result;
    } catch (e) {
      return [{ error: e.message ?? `Evaluating parameter query failed` }];
    }
  }

  private transformRows(row: SqliteRow): SqliteRow[] {
    const tables = { [this.table]: row };
    let result: SqliteRow = {};
    for (let key in this.lookupExtractors) {
      const extractor = this.lookupExtractors[key];
      result[key] = extractor.evaluate(tables);
    }
    return [result];
  }

  /**
   * Given partial parameter rows, turn into bucket ids.
   *
   * Internal function, but exposed for tests.
   */
  resolveBucketDescriptions(bucketParameters: SqliteJsonRow[], parameters: RequestParameters): BucketDescription[] {
    // Filters have already been applied and gotten us the set of bucketParameters - don't attempt to filter again.
    // We _do_ need to evaluate the output columns here, using a combination of precomputed bucketParameters,
    // and values from token parameters.

    return bucketParameters
      .map((lookup) => {
        let result: Record<string, SqliteJsonValue> = {};
        for (let name of this.bucketParameters) {
          if (name in this.lookupExtractors) {
            result[`bucket.${name}`] = lookup[name];
          } else {
            const value = this.parameterExtractors[name].lookupParameterValue(parameters);
            if (!isJsonValue(value)) {
              // Not valid - exclude.
              // Should we error instead?
              return null;
            } else {
              result[`bucket.${name}`] = value;
            }
          }
        }

        return {
          definition: this.descriptorName,
          bucket: getBucketId(this.descriptorName, this.bucketParameters, result),
          priority: this.priority
        };
      })
      .filter((lookup) => lookup != null);
  }

  /**
   * Given sync parameters, get lookups we need to perform on the database.
   *
   * Each lookup is [bucket definition name, parameter query index, ...lookup values]
   */
  getLookups(parameters: RequestParameters): ParameterLookup[] {
    if (!this.expandedInputParameter) {
      let lookup: SqliteJsonValue[] = [this.descriptorName, this.queryId];

      let valid = true;
      lookup.push(
        ...this.inputParameters.map((param): SqliteJsonValue => {
          // Scalar value
          const value = param.parametersToLookupValue(parameters);

          if (isJsonValue(value)) {
            return normalizeParameterValue(value);
          } else {
            valid = false;
            return null;
          }
        })
      );
      if (!valid) {
        return [];
      }
      return [new ParameterLookup(lookup)];
    } else {
      const arrayString = this.expandedInputParameter.parametersToLookupValue(parameters);

      if (arrayString == null || typeof arrayString != 'string') {
        return [];
      }
      let values: SqliteJsonValue[];
      try {
        values = JSON.parse(arrayString);
        if (!Array.isArray(values)) {
          return [];
        }
      } catch (e) {
        return [];
      }

      return values
        .map((expandedValue) => {
          let lookup: SqliteJsonValue[] = [this.descriptorName, this.queryId];
          let valid = true;
          const normalizedExpandedValue = normalizeParameterValue(expandedValue);
          lookup.push(
            ...this.inputParameters.map((param): SqliteJsonValue => {
              if (param == this.expandedInputParameter) {
                // Expand array value
                return normalizedExpandedValue;
              } else {
                // Scalar value
                const value = param.parametersToLookupValue(parameters);

                if (isJsonValue(value)) {
                  return normalizeParameterValue(value);
                } else {
                  valid = false;
                  return null;
                }
              }
            })
          );
          if (!valid) {
            return null;
          }

          return new ParameterLookup(lookup);
        })
        .filter((lookup) => lookup != null) as ParameterLookup[];
    }
  }

  getBucketParameterQuerier(requestParameters: RequestParameters): BucketParameterQuerier {
    const lookups = this.getLookups(requestParameters);
    if (lookups.length == 0) {
      // This typically happens when the query is pre-filtered using a where clause
      // on the parameters, and does not depend on the database state.
      return {
        staticBuckets: [],
        hasDynamicBuckets: false,
        parameterQueryLookups: [],
        queryDynamicBucketDescriptions: async () => []
      };
    }

    return {
      staticBuckets: [],
      hasDynamicBuckets: true,
      parameterQueryLookups: lookups,
      queryDynamicBucketDescriptions: async (source: ParameterLookupSource) => {
        const bucketParameters = await source.getParameterSets(lookups);
        return this.resolveBucketDescriptions(bucketParameters, requestParameters);
      }
    };
  }

  get hasAuthenticatedBucketParameters(): boolean {
    // select request.user_id() as user_id where ...
    const authenticatedExtractor =
      Object.values(this.parameterExtractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesAuthenticatedRequestParameters
      ) != null;
    return authenticatedExtractor;
  }

  get hasAuthenticatedMatchClause(): boolean {
    // select ... where user_id = request.user_id()
    const authenticatedInputParameter = this.filter.usesAuthenticatedRequestParameters;
    return authenticatedInputParameter;
  }

  get usesUnauthenticatedRequestParameters(): boolean {
    // select ... where request.parameters() ->> 'include_comments'
    const unauthenticatedInputParameter = this.filter.usesUnauthenticatedRequestParameters;

    // select request.parameters() ->> 'project_id'
    const unauthenticatedExtractor =
      Object.values(this.parameterExtractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesUnauthenticatedRequestParameters
      ) != null;

    return unauthenticatedInputParameter || unauthenticatedExtractor;
  }

  /**
   * Safe:
   * SELECT id as user_id FROM users WHERE users.user_id = request.user_id()
   * SELECT request.jwt() ->> 'org_id' as org_id, id as project_id FROM projects WHERE id = request.parameters() ->> 'project_id'
   * SELECT id as project_id FROM projects WHERE org_id = request.jwt() ->> 'org_id' AND id = request.parameters() ->> 'project_id'
   * SELECT id as category_id FROM categories
   *
   * Dangerous:
   * SELECT id as project_id FROM projects WHERE id = request.parameters() ->> 'project_id'
   * SELECT id as project_id FROM projects WHERE id = request.parameters() ->> 'project_id' AND request.jwt() ->> 'role' = 'authenticated'
   * SELECT id as category_id, request.parameters() ->> 'project_id' as project_id FROM categories
   * SELECT id as category_id FROM categories WHERE request.parameters() ->> 'include_categories'
   */
  get usesDangerousRequestParameters() {
    return (
      this.usesUnauthenticatedRequestParameters &&
      !this.hasAuthenticatedBucketParameters &&
      !this.hasAuthenticatedMatchClause
    );
  }
}
