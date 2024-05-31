import { parse, SelectedColumn } from 'pgsql-ast-parser';
import {
  EvaluatedParameters,
  EvaluatedParametersResult,
  ParameterMatchClause,
  QueryBucketIdOptions,
  QuerySchema,
  SourceSchema,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow,
  StaticRowValueClause,
  SyncParameters
} from './types.js';
import { SqlRuleError } from './errors.js';
import { SqlTools } from './sql_filters.js';
import { StaticSqlParameterQuery } from './StaticSqlParameterQuery.js';
import { filterJsonRow, getBucketId, isJsonValue, isSelectStatement } from './utils.js';
import { TablePattern } from './TablePattern.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { checkUnsupportedFeatures, isClauseError } from './sql_support.js';
import { TableQuerySchema } from './TableQuerySchema.js';

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
    schema?: SourceSchema
  ): SqlParameterQuery | StaticSqlParameterQuery {
    const parsed = parse(sql, { locationTracking: true });
    const rows = new SqlParameterQuery();

    if (parsed.length > 1) {
      throw new SqlRuleError('Only a single SELECT statement is supported', sql, parsed[1]?._location);
    }
    const q = parsed[0];

    if (!isSelectStatement(q)) {
      throw new SqlRuleError('Only SELECT statements are supported', sql, q._location);
    }

    if (q.from == null) {
      // E.g. SELECT token_parameters.user_id as user_id WHERE token_parameters.is_admin
      return StaticSqlParameterQuery.fromSql(descriptor_name, sql, q);
    }

    rows.errors.push(...checkUnsupportedFeatures(sql, q));

    if (q.from.length != 1 || q.from[0].type != 'table') {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }

    const tableRef = q.from?.[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }
    const alias: string = q.from?.[0].name.alias ?? tableRef.name;
    if (tableRef.name != alias) {
      rows.errors.push(
        new SqlRuleError('Table aliases not supported in parameter queries', sql, q.from?.[0]._location)
      );
    }
    const sourceTable = new TablePattern(tableRef.schema, tableRef.name);
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
      schema: querySchema
    });
    const where = q.where;
    const filter = tools.compileWhereClause(where);

    const bucket_parameters = (q.columns ?? []).map((column) => tools.getOutputName(column));
    rows.sourceTable = sourceTable;
    rows.table = alias;
    rows.sql = sql;
    rows.filter = filter;
    rows.descriptor_name = descriptor_name;
    rows.bucket_parameters = bucket_parameters;
    rows.input_parameters = filter.bucketParameters!;
    const expandedParams = rows.input_parameters.filter((param) => param.endsWith('[*]'));
    if (expandedParams.length > 1) {
      rows.errors.push(new SqlRuleError('Cannot have multiple array input parameters', sql));
    }
    rows.expanded_input_parameter = expandedParams[0];
    rows.columns = q.columns ?? [];
    rows.static_columns = [];
    rows.lookup_columns = [];
    rows.static_tools = new SqlTools({
      // This is used for values not on the parameter query table - these operate directly on
      // token_parameters or user_parameters.
      table: undefined,
      value_tables: ['token_parameters', 'user_parameters'],
      parameter_tables: [],
      sql
    });

    for (let column of q.columns ?? []) {
      const name = tools.getSpecificOutputName(column);
      if (tools.isTableRef(column.expr)) {
        rows.lookup_columns.push(column);
        const extractor = tools.compileStaticExtractor(column.expr);
        if (isClauseError(extractor)) {
          // Error logged already
          continue;
        }
        rows.lookup_extractors[name] = extractor;
      } else {
        rows.static_columns.push(column);
        const extractor = rows.static_tools.compileStaticExtractor(column.expr);
        if (isClauseError(extractor)) {
          // Error logged already
          continue;
        }
        rows.static_extractors[name] = extractor;
      }
    }
    rows.tools = tools;
    rows.errors.push(...tools.errors);
    rows.errors.push(...rows.static_tools.errors);
    return rows;
  }

  sourceTable?: TablePattern;
  table?: string;
  sql?: string;
  columns?: SelectedColumn[];
  lookup_columns?: SelectedColumn[];
  static_columns?: SelectedColumn[];

  lookup_extractors: Record<string, StaticRowValueClause> = {};
  static_extractors: Record<string, StaticRowValueClause> = {};

  filter?: ParameterMatchClause;
  descriptor_name?: string;
  /** _Input_ token / user parameters */
  input_parameters?: string[];

  expanded_input_parameter?: string;

  /** _Output_ bucket parameters */
  bucket_parameters?: string[];

  id?: string;
  tools?: SqlTools;
  static_tools?: SqlTools;

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
      const filterParameters = this.filter!.filter(tables);
      let result: EvaluatedParametersResult[] = [];
      for (let filterParamSet of filterParameters) {
        let lookup: SqliteJsonValue[] = [this.descriptor_name!, this.id!];
        lookup.push(
          ...this.input_parameters!.map((param) => {
            return filterParamSet[param];
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

  resolveBucketIds(bucketParameters: SqliteJsonRow[], parameters: SyncParameters): string[] {
    const tables = { token_parameters: parameters.token_parameters, user_parameters: parameters.user_parameters };

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
            const value = this.static_extractors[name].evaluate(tables);
            if (!isJsonValue(value)) {
              // Not valid - exclude.
              // Should we error instead?
              return null;
            } else {
              result[`bucket.${name}`] = value;
            }
          }
        }

        return getBucketId(this.descriptor_name!, this.bucket_parameters!, result);
      })
      .filter((lookup) => lookup != null) as string[];
  }

  lookupParam(param: string, parameters: SyncParameters) {
    const [table, column] = param.split('.');
    const pt: SqliteJsonRow | undefined = (parameters as any)[table];
    return pt?.[column] ?? null;
  }

  getLookups(parameters: SyncParameters): SqliteJsonValue[][] {
    if (!this.expanded_input_parameter) {
      let lookup: SqliteJsonValue[] = [this.descriptor_name!, this.id!];

      lookup.push(
        ...this.input_parameters!.map((param): SqliteJsonValue => {
          // Scalar value
          return this.lookupParam(param, parameters);
        })
      );
      return [lookup];
    } else {
      const arrayString = this.lookupParam(
        this.expanded_input_parameter.substring(0, this.expanded_input_parameter.length - 3),
        parameters
      );
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

      return values.map((expandedValue): SqliteJsonValue[] => {
        let lookup: SqliteJsonValue[] = [this.descriptor_name!, this.id!];

        lookup.push(
          ...this.input_parameters!.map((param): SqliteJsonValue => {
            if (param == this.expanded_input_parameter) {
              // Expand array value
              return expandedValue;
            } else {
              // Scalar value
              return this.lookupParam(param, parameters);
            }
          })
        );

        return lookup;
      });
    }
  }

  async queryBucketIds(options: QueryBucketIdOptions): Promise<string[]> {
    let lookups = this.getLookups(options.parameters);
    if (lookups.length == 0) {
      return [];
    }

    const parameters = await options.getParameterSets(lookups);
    return this.resolveBucketIds(parameters, options.parameters);
  }
}
