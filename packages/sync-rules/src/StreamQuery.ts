import { parse } from 'pgsql-ast-parser';
import { ParameterValueClause, QuerySchema, StreamParseOptions } from './types.js';
import { SqlRuleError } from './errors.js';
import { isSelectStatement } from './utils.js';
import { checkUnsupportedFeatures, isClauseError } from './sql_support.js';
import { SqlDataQuery, SqlDataQueryOptions } from './SqlDataQuery.js';
import { RowValueExtractor } from './BaseSqlDataQuery.js';
import { TablePattern } from './TablePattern.js';
import { TableQuerySchema } from './TableQuerySchema.js';
import { SqlTools } from './sql_filters.js';
import { ExpressionType } from './ExpressionType.js';
import { SqlParameterQuery } from './SqlParameterQuery.js';
import { StaticSqlParameterQuery } from './StaticSqlParameterQuery.js';
import { DEFAULT_BUCKET_PRIORITY } from './BucketDescription.js';

/**
 * Represents a query backing a stream definition.
 *
 * Streams are a new way to define sync rules that don't require separate data and
 * parameter queries. However, since most of the sync service is built around that
 * distiction at the moment, stream queries are implemented by desugaring a unified
 * query into its individual components.
 */
export class StreamQuery {
  inferredParameters: (SqlParameterQuery | StaticSqlParameterQuery)[];
  data: SqlDataQuery;

  static fromSql(descriptorName: string, sql: string, options: StreamParseOptions): [StreamQuery, SqlRuleError[]] {
    const [query, ...illegalRest] = parse(sql, { locationTracking: true });
    const schema = options.schema;
    const parameters: (SqlParameterQuery | StaticSqlParameterQuery)[] = [];
    const errors: SqlRuleError[] = [];

    // TODO: Share more of this code with SqlDataQuery
    if (illegalRest.length > 0) {
      throw new SqlRuleError('Only a single SELECT statement is supported', sql, illegalRest[0]?._location);
    }

    if (!isSelectStatement(query)) {
      throw new SqlRuleError('Only SELECT statements are supported', sql, query._location);
    }

    if (query.from == null || query.from.length != 1 || query.from[0].type != 'table') {
      throw new SqlRuleError('Must SELECT from a single table', sql, query.from?.[0]._location);
    }

    errors.push(...checkUnsupportedFeatures(sql, query));

    const tableRef = query.from?.[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', sql, query.from?.[0]._location);
    }
    const alias: string = tableRef.alias ?? tableRef.name;

    const sourceTable = new TablePattern(tableRef.schema ?? options.defaultSchema, tableRef.name);
    let querySchema: QuerySchema | undefined = undefined;
    if (schema) {
      const tables = schema.getTables(sourceTable);
      if (tables.length == 0) {
        const e = new SqlRuleError(
          `Table ${sourceTable.schema}.${sourceTable.tablePattern} not found`,
          sql,
          query.from?.[0]?._location
        );
        e.type = 'warning';

        errors.push(e);
      } else {
        querySchema = new TableQuerySchema(tables, alias);
      }
    }

    const where = query.where;
    const tools = new SqlTools({
      table: alias,
      parameterTables: [],
      valueTables: [alias],
      sql,
      schema: querySchema,
      supportsStreamInputs: true,
      supportsParameterExpressions: true
    });
    tools.checkSpecificNameCase(tableRef);
    const filter = tools.compileWhereClause(where);
    const inputParameterNames = filter.inputParameters.map((p) => `bucket.${p.key}`);

    // Build parameter queries based on inferred bucket parameters
    if (tools.inferredParameters.length) {
      const extractors: Record<string, ParameterValueClause> = {};
      for (const inferred of tools.inferredParameters) {
        extractors[inferred.name] = inferred.clause;
      }

      parameters.push(
        new StaticSqlParameterQuery({
          sql,
          queryId: 'static',
          descriptorName,
          parameterExtractors: extractors,
          bucketParameters: tools.inferredParameters.map((p) => p.name),
          filter: undefined, // TODO
          priority: DEFAULT_BUCKET_PRIORITY // Ignored here
        })
      );
    }

    let hasId = false;
    let hasWildcard = false;
    let extractors: RowValueExtractor[] = [];

    for (let column of query.columns ?? []) {
      const name = tools.getOutputName(column);
      if (name != '*') {
        const clause = tools.compileRowValueExtractor(column.expr);
        if (isClauseError(clause)) {
          // Error logged already
          continue;
        }
        extractors.push({
          extract: (tables, output) => {
            output[name] = clause.evaluate(tables);
          },
          getTypes(schema, into) {
            const def = clause.getColumnDefinition(schema);

            into[name] = { name, type: def?.type ?? ExpressionType.NONE, originalType: def?.originalType };
          }
        });
      } else {
        extractors.push({
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
          const idType = querySchema.getColumn(alias, 'id')?.type ?? ExpressionType.NONE;
          if (!idType.isNone()) {
            hasId = true;
          }
        }
      }
    }
    if (!hasId) {
      const error = new SqlRuleError(`Query must return an "id" column`, sql, query.columns?.[0]._location);
      if (hasWildcard) {
        // Schema-based validations are always warnings
        error.type = 'warning';
      }
      errors.push(error);
    }

    errors.push(...tools.errors);

    const data: SqlDataQueryOptions = {
      sourceTable,
      table: alias,
      sql,
      filter,
      columns: query.columns ?? [],
      descriptorName,
      bucketParameters: inputParameterNames,
      tools,
      extractors
    };
    return [new StreamQuery(parameters, data), errors];
  }

  private constructor(parameters: (SqlParameterQuery | StaticSqlParameterQuery)[], data: SqlDataQueryOptions) {
    this.inferredParameters = parameters;
    this.data = new SqlDataQuery(data);
  }
}
