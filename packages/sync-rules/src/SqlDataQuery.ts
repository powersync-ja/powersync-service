import { JSONBig } from '@powersync/service-jsonbig';
import { parse } from 'pgsql-ast-parser';
import { BaseSqlDataQuery, BaseSqlDataQueryOptions, RowValueExtractor } from './BaseSqlDataQuery.js';
import { SqlRuleError } from './errors.js';
import { ExpressionType } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlTools } from './sql_filters.js';
import { castAsText } from './sql_functions.js';
import { checkUnsupportedFeatures, isClauseError } from './sql_support.js';
import { SyncRulesOptions } from './SqlSyncRules.js';
import { TablePattern } from './TablePattern.js';
import { TableQuerySchema } from './TableQuerySchema.js';
import { EvaluationResult, ParameterMatchClause, QuerySchema, SqliteRow } from './types.js';
import { getBucketId, isSelectStatement } from './utils.js';

export interface SqlDataQueryOptions extends BaseSqlDataQueryOptions {
  filter: ParameterMatchClause;
}

export class SqlDataQuery extends BaseSqlDataQuery {
  readonly filter: ParameterMatchClause;

  static fromSql(
    descriptor_name: string,
    bucket_parameters: string[],
    sql: string,
    options: SyncRulesOptions,
    ruleId?: string
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
      parameter_tables: ['bucket'],
      value_tables: [alias],
      sql,
      schema: querySchema
    });
    tools.checkSpecificNameCase(tableRef);
    const filter = tools.compileWhereClause(where);

    const inputParameterNames = filter.inputParameters.map((p) => p.key);
    const bucketParameterNames = bucket_parameters.map((p) => `bucket.${p}`);
    const allParams = new Set<string>([...inputParameterNames, ...bucketParameterNames]);
    if (
      (!filter.error && allParams.size != filter.inputParameters.length) ||
      allParams.size != bucket_parameters.length
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
      descriptor_name,
      bucket_parameters,
      tools,
      ruleId: ruleId ?? '',
      errors,
      extractors
    });
  }

  constructor(options: SqlDataQueryOptions) {
    super(options);
    this.filter = options.filter;
  }

  evaluateRow(table: SourceTableInterface, row: SqliteRow): EvaluationResult[] {
    try {
      const tables = { [this.table]: this.addSpecialParameters(table, row) };
      const bucketParameters = this.filter.filterRow(tables);
      const bucketIds = bucketParameters.map((params) =>
        getBucketId(this.descriptor_name, this.bucket_parameters, params)
      );

      const data = this.transformRow(tables);
      let id = data.id;
      if (typeof id != 'string') {
        // While an explicit cast would be better, this covers against very common
        // issues when initially testing out sync, for example when the id column is an
        // auto-incrementing integer.
        // If there is no id column, we use a blank id. This will result in the user syncing
        // a single arbitrary row for this table - better than just not being able to sync
        // anything.
        id = castAsText(id) ?? '';
      }
      const outputTable = this.getOutputName(table.table);

      return bucketIds.map((bucketId) => {
        return {
          bucket: bucketId,
          table: outputTable,
          id: id,
          data,
          ruleId: this.ruleId
        } as EvaluationResult;
      });
    } catch (e) {
      return [{ error: e.message ?? `Evaluating data query failed` }];
    }
  }
}
