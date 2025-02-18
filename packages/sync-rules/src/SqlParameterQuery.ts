import { parse, SelectedColumn } from 'pgsql-ast-parser';
import { BucketDescription, BucketPriority, defaultBucketPriority } from './BucketDescription.js';
import { BucketParameterQuerier, ParameterLookupSource } from './BucketParameterQuerier.js';
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
import { filterJsonRow, getBucketId, isJsonValue, isSelectStatement } from './utils.js';

/**
 * Represents a parameter query, such as:
 *
 *  SELECT id as user_id FROM users WHERE users.user_id = token_parameters.user_id
 *  SELECT id as user_id, token_parameters.is_admin as is_admin FROM users WHERE users.user_id = token_parameters.user_id
 */
export class SqlParameterQuery {
  static fromSql(
    descriptor_name: string,
    sql: string,
    options: QueryParseOptions
  ): SqlParameterQuery | StaticSqlParameterQuery {
    const parsed = parse(sql, { locationTracking: true });
    const rows = new SqlParameterQuery();
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
      return StaticSqlParameterQuery.fromSql(descriptor_name, sql, q, options);
    }

    rows.errors.push(...checkUnsupportedFeatures(sql, q));

    if (q.from.length != 1) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    } else if (q.from[0].type == 'call') {
      const from = q.from[0];
      return TableValuedFunctionSqlParameterQuery.fromSql(descriptor_name, sql, from, q, options);
    } else if (q.from[0].type == 'statement') {
      throw new SqlRuleError('Subqueries are not supported yet', sql, q.from?.[0]._location);
    }

    const tableRef = q.from[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }
    const alias: string = q.from?.[0].name.alias ?? tableRef.name;
    if (tableRef.name != alias) {
      rows.errors.push(
        new SqlRuleError('Table aliases not supported in parameter queries', sql, q.from?.[0]._location)
      );
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

        rows.errors.push(e);
      } else {
        querySchema = new TableQuerySchema(tables, alias);
      }
    }

    const tools = new SqlTools({
      table: alias,
      parameter_tables: ['token_parameters', 'user_parameters'],
      sql,
      supports_expanding_parameters: true,
      supports_parameter_expressions: true,
      schema: querySchema
    });
    tools.checkSpecificNameCase(tableRef);
    const where = q.where;
    const filter = tools.compileWhereClause(where);

    const bucket_parameters = (q.columns ?? [])
      .map((column) => tools.getOutputName(column))
      .filter((c) => !tools.isBucketPriorityParameter(c));
    rows.sourceTable = sourceTable;
    rows.table = alias;
    rows.sql = sql;
    rows.filter = filter;
    rows.descriptor_name = descriptor_name;
    rows.bucket_parameters = bucket_parameters;
    rows.input_parameters = filter.inputParameters!;
    rows.priority = options.priority;
    const expandedParams = rows.input_parameters!.filter((param) => param.expands);
    if (expandedParams.length > 1) {
      rows.errors.push(new SqlRuleError('Cannot have multiple array input parameters', sql));
    }
    rows.expanded_input_parameter = expandedParams[0];
    rows.columns = q.columns ?? [];
    rows.static_columns = [];
    rows.lookup_columns = [];

    for (let column of q.columns ?? []) {
      const name = tools.getSpecificOutputName(column);
      if (column.alias != null) {
        tools.checkSpecificNameCase(column.alias);
      }
      if (tools.isBucketPriorityParameter(name)) {
        if (rows.priority !== undefined) {
          rows.errors.push(new SqlRuleError('Cannot set priority multiple times.', sql));
          continue;
        }

        rows.priority = tools.extractBucketPriority(column.expr);
      } else if (tools.isTableRef(column.expr)) {
        rows.lookup_columns.push(column);
        const extractor = tools.compileRowValueExtractor(column.expr);
        if (isClauseError(extractor)) {
          // Error logged already
          continue;
        }
        rows.lookup_extractors[name] = extractor;
      } else {
        rows.static_columns.push(column);
        const extractor = tools.compileParameterValueExtractor(column.expr);
        if (isClauseError(extractor)) {
          // Error logged already
          continue;
        }
        rows.parameter_extractors[name] = extractor;
      }
    }
    rows.tools = tools;
    rows.errors.push(...tools.errors);

    if (rows.usesDangerousRequestParameters && !options.accept_potentially_dangerous_queries) {
      let err = new SqlRuleError(
        "Potentially dangerous query based on parameters set by the client. The client can send any value for these parameters so it's not a good place to do authorization.",
        sql
      );
      err.type = 'warning';
      rows.errors.push(err);
    }
    return rows;
  }

  sourceTable?: TablePattern;
  table?: string;
  sql?: string;
  columns?: SelectedColumn[];
  lookup_columns?: SelectedColumn[];
  static_columns?: SelectedColumn[];

  /**
   * Example: SELECT *user.id* FROM users WHERE ...
   */
  lookup_extractors: Record<string, RowValueClause> = {};

  /**
   * Example: SELECT *token_parameters.user_id*
   */
  parameter_extractors: Record<string, ParameterValueClause> = {};
  priority?: BucketPriority;

  filter?: ParameterMatchClause;

  /**
   * Bucket definition name.
   */
  descriptor_name?: string;

  /** _Input_ token / user parameters */
  input_parameters?: InputParameter[];

  /** If specified, an input parameter that expands to an array. */
  expanded_input_parameter?: InputParameter;

  /**
   * _Output_ bucket parameters.
   *
   * Each one of these will be present in either lookup_extractors or static_extractors.
   */
  bucket_parameters?: string[];

  id?: string;
  tools?: SqlTools;

  errors: SqlRuleError[] = [];

  constructor() {}

  applies(table: SourceTableInterface) {
    return this.sourceTable!.matches(table);
  }

  evaluateParameterRow(row: SqliteRow): EvaluatedParametersResult[] {
    const tables = {
      [this.table!]: row
    };
    try {
      const filterParameters = this.filter!.filterRow(tables);
      let result: EvaluatedParametersResult[] = [];
      for (let filterParamSet of filterParameters) {
        let lookup: SqliteJsonValue[] = [this.descriptor_name!, this.id!];
        lookup.push(
          ...this.input_parameters!.map((param) => {
            return param.filteredRowToLookupValue(filterParamSet);
          })
        );

        const data = this.transformRows(row);

        const role: EvaluatedParameters = {
          bucket_parameters: data.map((row) => filterJsonRow(row)),
          lookup: lookup
        };
        result.push(role);
      }
      return result;
    } catch (e) {
      return [{ error: e.message ?? `Evaluating parameter query failed` }];
    }
  }

  transformRows(row: SqliteRow): SqliteRow[] {
    const tables = { [this.table!]: row };
    let result: SqliteRow = {};
    for (let key in this.lookup_extractors) {
      const extractor = this.lookup_extractors[key];
      result[key] = extractor.evaluate(tables);
    }
    return [result];
  }

  /**
   * Given partial parameter rows, turn into bucket ids.
   */
  resolveBucketDescriptions(bucketParameters: SqliteJsonRow[], parameters: RequestParameters): BucketDescription[] {
    // Filters have already been applied and gotten us the set of bucketParameters - don't attempt to filter again.
    // We _do_ need to evaluate the output columns here, using a combination of precomputed bucketParameters,
    // and values from token parameters.

    return bucketParameters
      .map((lookup) => {
        let result: Record<string, SqliteJsonValue> = {};
        for (let name of this.bucket_parameters!) {
          if (name in this.lookup_extractors) {
            result[`bucket.${name}`] = lookup[name];
          } else {
            const value = this.parameter_extractors[name].lookupParameterValue(parameters);
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
          bucket: getBucketId(this.descriptor_name!, this.bucket_parameters!, result),
          priority: this.priority ?? defaultBucketPriority
        };
      })
      .filter((lookup) => lookup != null);
  }

  /**
   * Given sync parameters, get lookups we need to perform on the database.
   *
   * Each lookup is [bucket definition name, parameter query index, ...lookup values]
   */
  getLookups(parameters: RequestParameters): SqliteJsonValue[][] {
    if (!this.expanded_input_parameter) {
      let lookup: SqliteJsonValue[] = [this.descriptor_name!, this.id!];

      let valid = true;
      lookup.push(
        ...this.input_parameters!.map((param): SqliteJsonValue => {
          // Scalar value
          const value = param.parametersToLookupValue(parameters);

          if (isJsonValue(value)) {
            return value;
          } else {
            valid = false;
            return null;
          }
        })
      );
      if (!valid) {
        return [];
      }
      return [lookup];
    } else {
      const arrayString = this.expanded_input_parameter.parametersToLookupValue(parameters);

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
          let lookup: SqliteJsonValue[] = [this.descriptor_name!, this.id!];
          let valid = true;
          lookup.push(
            ...this.input_parameters!.map((param): SqliteJsonValue => {
              if (param == this.expanded_input_parameter) {
                // Expand array value
                return expandedValue;
              } else {
                // Scalar value
                const value = param.parametersToLookupValue(parameters);

                if (isJsonValue(value)) {
                  return value;
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

          return lookup;
        })
        .filter((lookup) => lookup != null) as SqliteJsonValue[][];
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
        dynamicBucketDefinitions: new Set<string>(),
        queryDynamicBucketDescriptions: async () => []
      };
    }

    return {
      staticBuckets: [],
      hasDynamicBuckets: true,
      dynamicBucketDefinitions: new Set<string>([this.descriptor_name!]),
      queryDynamicBucketDescriptions: async (source: ParameterLookupSource) => {
        const bucketParameters = await source.getParameterSets(lookups);
        return this.resolveBucketDescriptions(bucketParameters, requestParameters);
      }
    };
  }

  get hasAuthenticatedBucketParameters(): boolean {
    // select request.user_id() as user_id where ...
    const authenticatedExtractor =
      Object.values(this.parameter_extractors).find(
        (clause) => isParameterValueClause(clause) && clause.usesAuthenticatedRequestParameters
      ) != null;
    return authenticatedExtractor;
  }

  get hasAuthenticatedMatchClause(): boolean {
    // select ... where user_id = request.user_id()
    this.filter?.inputParameters.find;
    const authenticatedInputParameter = this.filter!.usesAuthenticatedRequestParameters;
    return authenticatedInputParameter;
  }

  get usesUnauthenticatedRequestParameters(): boolean {
    // select ... where request.parameters() ->> 'include_comments'
    const unauthenticatedInputParameter = this.filter!.usesUnauthenticatedRequestParameters;

    // select request.parameters() ->> 'project_id'
    const unauthenticatedExtractor =
      Object.values(this.parameter_extractors).find(
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
