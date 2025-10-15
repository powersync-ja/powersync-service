import { ColumnDefinition } from './ExpressionType.js';
import { AvailableTable } from './sql_filters.js';
import { QuerySchema, SourceSchemaTable } from './types.js';

/**
 * Exposes a list of {@link SourceSchemaTable}s as a {@link QuerySchema} by only exposing the subset of the schema
 * referenced in a `FROM` clause.
 */
export class TableQuerySchema implements QuerySchema {
  constructor(
    private tables: SourceSchemaTable[],
    private alias: AvailableTable
  ) {}

  getColumn(table: string, column: string): ColumnDefinition | undefined {
    if (table != this.alias.schemaName) {
      return undefined;
    }
    for (let table of this.tables) {
      const t = table.getColumn(column);
      if (t != null) {
        return t;
      }
    }
    return undefined;
  }

  getColumns(table: string): ColumnDefinition[] {
    if (table != this.alias.schemaName) {
      return [];
    }
    let columns: Record<string, ColumnDefinition> = {};

    for (let table of this.tables) {
      for (let col of table.getColumns()) {
        if (!(col.name in columns)) {
          columns[col.name] = col;
        }
      }
    }
    return Object.values(columns);
  }
}
