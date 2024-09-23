import { JSONBig } from '@powersync/service-jsonbig';
import { parse } from 'pgsql-ast-parser';
import { BaseSqlDataQuery } from './BaseSqlDataQuery.js';
import { SqlRuleError } from './errors.js';
import { SqlTools } from './sql_filters.js';
import { checkUnsupportedFeatures, isClauseError } from './sql_support.js';
import { TablePattern } from './TablePattern.js';
import { TableQuerySchema } from './TableQuerySchema.js';
import { ParameterMatchClause, QuerySchema, SourceSchema } from './types.js';
import { isSelectStatement } from './utils.js';

export class SqlDataQuery extends BaseSqlDataQuery {
  filter?: ParameterMatchClause;

  static fromSql(descriptor_name: string, bucket_parameters: string[], sql: string, schema?: SourceSchema) {
    const parsed = parse(sql, { locationTracking: true });
    const rows = new SqlDataQuery();

    if (parsed.length > 1) {
      throw new SqlRuleError('Only a single SELECT statement is supported', sql, parsed[1]?._location);
    }
    const q = parsed[0];
    if (!isSelectStatement(q)) {
      throw new SqlRuleError('Only SELECT statements are supported', sql, q._location);
    }

    rows.errors.push(...checkUnsupportedFeatures(sql, q));

    if (q.from == null || q.from.length != 1 || q.from[0].type != 'table') {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }

    const tableRef = q.from?.[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }
    const alias: string = tableRef.alias ?? tableRef.name;

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

    const where = q.where;
    const tools = new SqlTools({
      table: alias,
      parameter_tables: ['bucket'],
      value_tables: [alias],
      sql,
      schema: querySchema
    });
    const filter = tools.compileWhereClause(where);

    const inputParameterNames = filter.inputParameters!.map((p) => p.key);
    const bucketParameterNames = bucket_parameters.map((p) => `bucket.${p}`);
    const allParams = new Set<string>([...inputParameterNames, ...bucketParameterNames]);
    if (
      (!filter.error && allParams.size != filter.inputParameters!.length) ||
      allParams.size != bucket_parameters.length
    ) {
      rows.errors.push(
        new SqlRuleError(
          `Query must cover all bucket parameters. Expected: ${JSONBig.stringify(
            bucketParameterNames
          )} Got: ${JSONBig.stringify(inputParameterNames)}`,
          sql,
          q._location
        )
      );
    }

    rows.sourceTable = sourceTable;
    rows.table = alias;
    rows.sql = sql;
    rows.filter = filter;
    rows.descriptor_name = descriptor_name;
    rows.bucket_parameters = bucket_parameters;
    rows.columns = q.columns ?? [];
    rows.tools = tools;

    let hasId = false;
    let hasWildcard = false;

    for (let column of q.columns ?? []) {
      const name = tools.getOutputName(column);
      if (name != '*') {
        const clause = tools.compileRowValueExtractor(column.expr);
        if (isClauseError(clause)) {
          // Error logged already
          continue;
        }
        rows.extractors.push({
          extract: (tables, output) => {
            output[name] = clause.evaluate(tables);
          },
          getTypes(schema, into) {
            into[name] = { name, type: clause.getType(schema) };
          }
        });
      } else {
        rows.extractors.push({
          extract: (tables, output) => {
            const row = tables[alias];
            for (let key in row) {
              if (key.startsWith('_')) {
                continue;
              }
              output[key] ??= row[key];
            }
          },
          getTypes(schema, into) {
            for (let column of schema.getColumns(alias)) {
              into[column.name] ??= column;
            }
          }
        });
      }
      if (name == 'id') {
        hasId = true;
      } else if (name == '*') {
        hasWildcard = true;
        if (querySchema == null) {
          // Not performing schema-based validation - assume there is an id
          hasId = true;
        } else {
          const idType = querySchema.getType(alias, 'id');
          if (!idType.isNone()) {
            hasId = true;
          }
        }
      }
    }
    if (!hasId) {
      const error = new SqlRuleError(`Query must return an "id" column`, sql, q.columns?.[0]._location);
      if (hasWildcard) {
        // Schema-based validations are always warnings
        error.type = 'warning';
      }
      rows.errors.push(error);
    }
    rows.errors.push(...tools.errors);
    return rows;
  }
}
