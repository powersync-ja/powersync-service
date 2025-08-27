import { JSONBig } from '@powersync/service-jsonbig';
import { parse } from 'pgsql-ast-parser';
import { BaseSqlDataQuery, BaseSqlDataQueryOptions, RowValueExtractor } from './BaseSqlDataQuery.js';
import { SqlRuleError } from './errors.js';
import { ExpressionType } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlTools } from './sql_filters.js';
import { checkUnsupportedFeatures, isClauseError } from './sql_support.js';
import { SyncRulesOptions } from './SqlSyncRules.js';
import { TablePattern } from './TablePattern.js';
import { TableQuerySchema } from './TableQuerySchema.js';
import { BucketIdTransformer, EvaluationResult, ParameterMatchClause, QuerySchema, SqliteRow } from './types.js';
import { getBucketId, isSelectStatement } from './utils.js';
import { CompatibilityContext } from './compatibility.js';

export interface SqlDataQueryOptions extends BaseSqlDataQueryOptions {
  filter: ParameterMatchClause;
}

export class SqlDataQuery extends BaseSqlDataQuery {
  static fromSql(
    descriptorName: string,
    bucketParameters: string[],
    sql: string,
    options: SyncRulesOptions,
    compatibility: CompatibilityContext
  ) {
    const parsed = parse(sql, { locationTracking: true });
    const schema = options.schema;

    let errors: SqlRuleError[] = [];

    if (parsed.length > 1) {
      throw new SqlRuleError('Only a single SELECT statement is supported', sql, parsed[1]?._location);
    }
    const q = parsed[0];
    if (!isSelectStatement(q)) {
      throw new SqlRuleError('Only SELECT statements are supported', sql, q._location);
    }

    errors.push(...checkUnsupportedFeatures(sql, q));

    if (q.from == null || q.from.length != 1 || q.from[0].type != 'table') {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
    }

    const tableRef = q.from?.[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', sql, q.from?.[0]._location);
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
          q.from?.[0]?._location
        );
        e.type = 'warning';

        errors.push(e);
      } else {
        querySchema = new TableQuerySchema(tables, alias);
      }
    }

    const where = q.where;
    const tools = new SqlTools({
      table: alias,
      parameterTables: ['bucket'],
      valueTables: [alias],
      compatibilityContext: compatibility,
      sql,
      schema: querySchema
    });
    tools.checkSpecificNameCase(tableRef);
    const filter = tools.compileWhereClause(where);

    const inputParameterNames = filter.inputParameters.map((p) => p.key);
    const bucketParameterNames = bucketParameters.map((p) => `bucket.${p}`);
    const allParams = new Set<string>([...inputParameterNames, ...bucketParameterNames]);
    if (
      (!filter.error && allParams.size != filter.inputParameters.length) ||
      allParams.size != bucketParameters.length
    ) {
      errors.push(
        new SqlRuleError(
          `Query must cover all bucket parameters. Expected: ${JSONBig.stringify(
            bucketParameterNames
          )} Got: ${JSONBig.stringify(inputParameterNames)}`,
          sql,
          q._location
        )
      );
    }

    let hasId = false;
    let hasWildcard = false;
    let extractors: RowValueExtractor[] = [];

    for (let column of q.columns ?? []) {
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
      const error = new SqlRuleError(`Query must return an "id" column`, sql, q.columns?.[0]._location);
      if (hasWildcard) {
        // Schema-based validations are always warnings
        error.type = 'warning';
      }
      errors.push(error);
    }

    errors.push(...tools.errors);

    return new SqlDataQuery({
      sourceTable,
      table: alias,
      sql,
      filter,
      columns: q.columns ?? [],
      descriptorName,
      bucketParameters,
      tools,
      errors,
      extractors
    });
  }

  /**
   * The query WHERE clause.
   *
   * For a given row, this returns a set of bucket parameter values that could cause the filter to match.
   *
   * We use this to determine the buckets that a data row belong to.
   */
  readonly filter: ParameterMatchClause;

  constructor(options: SqlDataQueryOptions) {
    super(options);
    this.filter = options.filter;
  }

  evaluateRow(
    table: SourceTableInterface,
    row: SqliteRow,
    bucketIdTransformer: BucketIdTransformer
  ): EvaluationResult[] {
    return this.evaluateRowWithOptions({
      table,
      row,
      bucketIds: (tables) => {
        const bucketParameters = this.filter.filterRow(tables);
        return bucketParameters.map((params) =>
          getBucketId(this.descriptorName, this.bucketParameters, params, bucketIdTransformer)
        );
      }
    });
  }
}
