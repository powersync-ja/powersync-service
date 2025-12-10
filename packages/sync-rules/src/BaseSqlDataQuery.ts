import { SelectedColumn } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { ColumnDefinition } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { AvailableTable, SqlTools } from './sql_filters.js';
import { TablePattern } from './TablePattern.js';
import {
  BucketIdTransformer,
  EvaluationResult,
  QueryParameters,
  QuerySchema,
  SourceSchema,
  SourceSchemaTable,
  SqliteJsonRow,
  SqliteRow
} from './types.js';
import { filterJsonRow } from './utils.js';
import { castAsText } from './sql_functions.js';

export interface RowValueExtractor {
  extract(tables: QueryParameters, into: SqliteRow): void;
  getTypes(schema: QuerySchema, into: Record<string, ColumnDefinition>): void;
}

export interface EvaluateRowOptions {
  table: SourceTableInterface;
  row: SqliteRow;
  bucketIds: (params: QueryParameters) => string[];
}

export interface BaseSqlDataQueryOptions {
  sourceTable: TablePattern;
  table: AvailableTable;
  sql: string;
  columns: SelectedColumn[];
  extractors: RowValueExtractor[];
  bucketParameters: string[];
  tools: SqlTools;
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
  readonly table: AvailableTable;

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
  readonly extractors: RowValueExtractor[];

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

  readonly errors: SqlRuleError[];

  constructor(options: BaseSqlDataQueryOptions) {
    this.sourceTable = options.sourceTable;
    this.table = options.table;
    this.sql = options.sql;
    this.columns = options.columns;
    this.extractors = options.extractors;
    this.bucketParameters = options.bucketParameters;
    this.tools = options.tools;
    this.errors = options.errors ?? [];
  }

  applies(table: SourceTableInterface) {
    return this.sourceTable.matches(table);
  }

  addSpecialParameters(table: SourceTableInterface, row: SqliteRow) {
    if (this.sourceTable.isWildcard) {
      return {
        ...row,
        _table_suffix: this.sourceTable.suffix(table.name)
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
      return this.table.sqlName;
    }
  }

  isUnaliasedWildcard() {
    return this.sourceTable.isWildcard && !this.table.isAliased;
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
          name: this.getOutputName(schemaTable.name),
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
        name: this.table.sqlName,
        columns: Object.values(output)
      });
    }

    return result;
  }

  resolveResultSets(schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>) {
    const outTables = this.getColumnOutputs(schema);
    for (let table of outTables) {
      tables[table.name] ??= {};
      for (let column of table.columns) {
        if (column.name != 'id') {
          tables[table.name][column.name] ??= column;
        }
      }
    }
  }

  evaluateRowWithOptions(options: EvaluateRowOptions): EvaluationResult[] {
    try {
      const { table, row, bucketIds } = options;

      const tables = { [this.table.nameInSchema]: this.addSpecialParameters(table, row) };
      const resolvedBucketIds = bucketIds(tables);
      if (resolvedBucketIds.length == 0) {
        // Short-circuit: No need to transform the row if there are no matching buckets.
        return [];
      }

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
      const outputTable = this.getOutputName(table.name);

      return resolvedBucketIds.map((bucketId) => {
        return {
          bucket: bucketId,
          table: outputTable,
          id: id,
          data
        } as EvaluationResult;
      });
    } catch (e) {
      return [{ error: e.message ?? `Evaluating data query failed` }];
    }
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
        if (table == this.table.nameInSchema) {
          return schemaTable.getColumn(column);
        } else {
          // TODO: bucket parameters?
          return undefined;
        }
      },
      getColumns: (table) => {
        if (table == this.table.nameInSchema) {
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
