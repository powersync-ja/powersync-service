import { SelectedColumn } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { ColumnDefinition } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlTools } from './sql_filters.js';
import { TablePattern } from './TablePattern.js';
import { QueryParameters, QuerySchema, SourceSchema, SourceSchemaTable, SqliteJsonRow, SqliteRow } from './types.js';
import { filterJsonRow } from './utils.js';

export interface RowValueExtractor {
  extract(tables: QueryParameters, into: SqliteRow): void;
  getTypes(schema: QuerySchema, into: Record<string, ColumnDefinition>): void;
}

export interface BaseSqlDataQueryOptions {
  sourceTable: TablePattern;
  table: string;
  sql: string;
  columns: SelectedColumn[];
  extractors: RowValueExtractor[];
  descriptorName: string;
  bucketParameters: string[];
  tools: SqlTools;

  ruleId: string;

  errors?: SqlRuleError[];
}

export class BaseSqlDataQuery {
  /**
   * Source table or table pattern.
   */
  readonly sourceTable: TablePattern;

  /**
   * The table name or alias used in the query.
   *
   * This is used for the output table name.
   */
  readonly table: string;

  /**
   * The source SQL query, for debugging purposes.
   */
  readonly sql: string;

  /**
   * Query columns, for debugging purposes.
   */
  readonly columns: SelectedColumn[];

  /**
   * Extracts input row into output row. This is the column list in the SELECT part of the query.
   *
   * This may include plain column names, wildcards, and basic expressions.
   */
  readonly extractors: RowValueExtractor[] = [];

  /**
   * Bucket definition name.
   */
  readonly descriptorName: string;
  /**
   * Bucket parameter names, without the `bucket.` prefix.
   *
   * These are received from the associated parameter query (if any), and must match the filters
   * used in the data query.
   */
  readonly bucketParameters: string[];
  /**
   * Used to generate debugging info.
   */
  private readonly tools: SqlTools;

  readonly ruleId: string;

  readonly errors: SqlRuleError[];

  constructor(options: BaseSqlDataQueryOptions) {
    this.sourceTable = options.sourceTable;
    this.table = options.table;
    this.sql = options.sql;
    this.columns = options.columns;
    this.extractors = options.extractors;
    this.descriptorName = options.descriptorName;
    this.bucketParameters = options.bucketParameters;
    this.tools = options.tools;
    this.ruleId = options.ruleId;
    this.errors = options.errors ?? [];
  }

  applies(table: SourceTableInterface) {
    return this.sourceTable.matches(table);
  }

  addSpecialParameters(table: SourceTableInterface, row: SqliteRow) {
    if (this.sourceTable.isWildcard) {
      return {
        ...row,
        _table_suffix: this.sourceTable.suffix(table.table)
      };
    } else {
      return row;
    }
  }

  getOutputName(sourceTable: string) {
    if (this.isUnaliasedWildcard()) {
      // Wildcard without alias - use source
      return sourceTable;
    } else {
      return this.table;
    }
  }

  isUnaliasedWildcard() {
    return this.sourceTable.isWildcard && this.table == this.sourceTable.tablePattern;
  }

  columnOutputNames(): string[] {
    return this.columns.map((c) => {
      return this.tools.getOutputName(c);
    });
  }

  getColumnOutputs(schema: SourceSchema): { name: string; columns: ColumnDefinition[] }[] {
    let result: { name: string; columns: ColumnDefinition[] }[] = [];

    if (this.isUnaliasedWildcard()) {
      // Separate results
      for (let schemaTable of schema.getTables(this.sourceTable)) {
        let output: Record<string, ColumnDefinition> = {};

        this.getColumnOutputsFor(schemaTable, output);

        result.push({
          name: this.getOutputName(schemaTable.table),
          columns: Object.values(output)
        });
      }
    } else {
      // Merged results
      let output: Record<string, ColumnDefinition> = {};
      for (let schemaTable of schema.getTables(this.sourceTable)) {
        this.getColumnOutputsFor(schemaTable, output);
      }
      result.push({
        name: this.table,
        columns: Object.values(output)
      });
    }

    return result;
  }

  protected transformRow(tables: QueryParameters): SqliteJsonRow {
    let result: SqliteRow = {};
    for (let extractor of this.extractors) {
      extractor.extract(tables, result);
    }
    return filterJsonRow(result);
  }

  protected getColumnOutputsFor(schemaTable: SourceSchemaTable, output: Record<string, ColumnDefinition>) {
    const querySchema: QuerySchema = {
      getColumn: (table, column) => {
        if (table == this.table) {
          return schemaTable.getColumn(column);
        } else {
          // TODO: bucket parameters?
          return undefined;
        }
      },
      getColumns: (table) => {
        if (table == this.table) {
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
