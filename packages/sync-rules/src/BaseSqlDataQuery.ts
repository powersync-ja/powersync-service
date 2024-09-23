import { SelectedColumn } from 'pgsql-ast-parser';
import { SqlRuleError } from './errors.js';
import { ColumnDefinition, ExpressionType } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { SqlTools } from './sql_filters.js';
import { castAsText } from './sql_functions.js';
import { TablePattern } from './TablePattern.js';
import {
  EvaluationResult,
  QueryParameters,
  QuerySchema,
  SourceSchema,
  SourceSchemaTable,
  SqliteJsonRow,
  SqliteRow
} from './types.js';
import { filterJsonRow, getBucketId } from './utils.js';

export interface RowValueExtractor {
  extract(tables: QueryParameters, into: SqliteRow): void;
  getTypes(schema: QuerySchema, into: Record<string, ColumnDefinition>): void;
}

export abstract class BaseSqlDataQuery {
  sourceTable?: TablePattern;
  table?: string;
  sql?: string;
  columns?: SelectedColumn[];
  extractors: RowValueExtractor[] = [];
  descriptor_name?: string;
  bucket_parameters?: string[];
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

  getOutputName(sourceTable: string) {
    if (this.isUnaliasedWildcard()) {
      // Wildcard without alias - use source
      return sourceTable;
    } else {
      return this.table!;
    }
  }

  isUnaliasedWildcard() {
    return this.sourceTable!.isWildcard && this.table == this.sourceTable!.tablePattern;
  }

  evaluateRow(table: SourceTableInterface, row: SqliteRow): EvaluationResult[] {
    try {
      const tables = { [this.table!]: this.addSpecialParameters(table, row) };
      const bucketParameters = this.filter!.filterRow(tables);
      const bucketIds = bucketParameters.map((params) =>
        getBucketId(this.descriptor_name!, this.bucket_parameters!, params)
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
          name: this.getOutputName(schemaTable.table),
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

  protected transformRow(tables: QueryParameters): SqliteJsonRow {
    let result: SqliteRow = {};
    for (let extractor of this.extractors) {
      extractor.extract(tables, result);
    }
    return filterJsonRow(result);
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
