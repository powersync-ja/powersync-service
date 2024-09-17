import { parse, SelectedColumn } from 'pgsql-ast-parser';
import { SqlRuleError } from '../errors.js';
import { ColumnDefinition, ExpressionType } from '../ExpressionType.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { SqlTools } from '../sql_filters.js';
import { checkUnsupportedFeatures, isClauseError } from '../sql_support.js';
import { RowValueExtractor } from '../SqlDataQuery.js';
import { TablePattern } from '../TablePattern.js';
import { TableQuerySchema } from '../TableQuerySchema.js';
import {
  EvaluationError,
  ParameterMatchClause,
  QueryParameters,
  QuerySchema,
  SourceSchema,
  SourceSchemaTable,
  SqliteJsonRow,
  SqliteRow
} from '../types.js';
import { filterJsonRow, isSelectStatement } from '../utils.js';

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
 * TODO cleanup duplication
 */
export class SqlEventSourceQuery {
  static fromSql(descriptor_name: string, sql: string, schema?: SourceSchema) {
    const parsed = parse(sql, { locationTracking: true });
    const rows = new SqlEventSourceQuery();

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

    rows.sourceTable = sourceTable;
    rows.table = alias;
    rows.sql = sql;
    rows.filter = filter;
    rows.descriptor_name = descriptor_name;
    rows.columns = q.columns ?? [];
    rows.tools = tools;

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
    }
    rows.errors.push(...tools.errors);
    return rows;
  }

  sourceTable?: TablePattern;
  table?: string;
  sql?: string;
  columns?: SelectedColumn[];
  extractors: RowValueExtractor[] = [];
  filter?: ParameterMatchClause;
  descriptor_name?: string;
  tools?: SqlTools;

  ruleId?: string;

  errors: SqlRuleError[] = [];

  constructor() {}

  applies(table: SourceTableInterface) {
    return this.sourceTable?.matches(table);
  }

  addSpecialParameters(table: SourceTableInterface, row: SqliteRow) {
    if (this.sourceTable!.isWildcard) {
      return {
        ...row,
        _table_suffix: this.sourceTable!.suffix(table.table)
      };
    } else {
      return row;
    }
  }

  isUnaliasedWildcard() {
    return this.sourceTable!.isWildcard && this.table == this.sourceTable!.tablePattern;
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

  private transformRow(tables: QueryParameters): SqliteJsonRow {
    let result: SqliteRow = {};
    for (let extractor of this.extractors) {
      extractor.extract(tables, result);
    }
    return filterJsonRow(result);
  }

  columnOutputNames(): string[] {
    return this.columns!.map((c) => {
      return this.tools!.getOutputName(c);
    });
  }

  getColumnOutputs(schema: SourceSchema): { name: string; columns: ColumnDefinition[] }[] {
    let result: { name: string; columns: ColumnDefinition[] }[] = [];

    if (this.isUnaliasedWildcard()) {
      // Separate results
      for (let schemaTable of schema.getTables(this.sourceTable!)) {
        let output: Record<string, ColumnDefinition> = {};

        this.getColumnOutputsFor(schemaTable, output);

        result.push({
          name: schemaTable.table,
          columns: Object.values(output)
        });
      }
    } else {
      // Merged results
      let output: Record<string, ColumnDefinition> = {};
      for (let schemaTable of schema.getTables(this.sourceTable!)) {
        this.getColumnOutputsFor(schemaTable, output);
      }
      result.push({
        name: this.table!,
        columns: Object.values(output)
      });
    }

    return result;
  }

  private getColumnOutputsFor(schemaTable: SourceSchemaTable, output: Record<string, ColumnDefinition>) {
    const querySchema: QuerySchema = {
      getType: (table, column) => {
        if (table == this.table!) {
          return schemaTable.getType(column) ?? ExpressionType.NONE;
        } else {
          // TODO: bucket parameters?
          return ExpressionType.NONE;
        }
      },
      getColumns: (table) => {
        if (table == this.table!) {
          return schemaTable.getColumns();
        } else {
          return [];
        }
      }
    };
    for (let extractor of this.extractors) {
      extractor.getTypes(querySchema, output);
    }
  }
}
