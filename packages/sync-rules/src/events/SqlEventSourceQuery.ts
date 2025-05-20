import { parse } from 'pgsql-ast-parser';
import { BaseSqlDataQuery, BaseSqlDataQueryOptions, RowValueExtractor } from '../BaseSqlDataQuery.js';
import { SqlRuleError } from '../errors.js';
import { ExpressionType } from '../ExpressionType.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { SqlTools } from '../sql_filters.js';
import { checkUnsupportedFeatures, isClauseError } from '../sql_support.js';
import { SyncRulesOptions } from '../SqlSyncRules.js';
import { TablePattern } from '../TablePattern.js';
import { TableQuerySchema } from '../TableQuerySchema.js';
import { EvaluationError, QuerySchema, SqliteJsonRow, SqliteRow } from '../types.js';
import { isSelectStatement } from '../utils.js';

export type EvaluatedEventSourceRow = {
  data: SqliteJsonRow;
  ruleId?: string;
};

export type EvaluatedEventRowWithErrors = {
  result?: EvaluatedEventSourceRow;
  errors: EvaluationError[];
};

/**
 * Defines how a Replicated Row is mapped to source parameters for events.
 */
export class SqlEventSourceQuery extends BaseSqlDataQuery {
  static fromSql(descriptor_name: string, sql: string, options: SyncRulesOptions, ruleId: string) {
    const parsed = parse(sql, { locationTracking: true });
    const schema = options.schema;

    if (parsed.length > 1) {
      throw new SqlRuleError('Only a single SELECT statement is supported', sql, parsed[1]?._location);
    }
    const q = parsed[0];
    if (!isSelectStatement(q)) {
      throw new SqlRuleError('Only SELECT statements are supported', sql, q._location);
    }

    let errors: SqlRuleError[] = [];

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

    const tools = new SqlTools({
      table: alias,
      parameter_tables: [],
      value_tables: [alias],
      sql,
      schema: querySchema
    });

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
    }
    errors.push(...tools.errors);

    return new SqlEventSourceQuery({
      sourceTable,
      table: alias,
      sql,
      descriptor_name,
      columns: q.columns ?? [],
      extractors: extractors,
      tools,
      bucket_parameters: [],
      ruleId: ruleId,
      errors: errors
    });
  }

  constructor(options: BaseSqlDataQueryOptions) {
    super(options);
  }

  evaluateRowWithErrors(table: SourceTableInterface, row: SqliteRow): EvaluatedEventRowWithErrors {
    try {
      const tables = { [this.table!]: this.addSpecialParameters(table, row) };

      const data = this.transformRow(tables);
      return {
        result: {
          data,
          ruleId: this.ruleId
        },
        errors: []
      };
    } catch (e) {
      return { errors: [e.message ?? `Evaluating data query failed`] };
    }
  }
}
