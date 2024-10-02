import { ColumnDefinition } from './ExpressionType.js';
import { QuerySchema, SourceSchemaTable } from './types.js';

export class TableQuerySchema implements QuerySchema {
  constructor(private tables: SourceSchemaTable[], private alias: string) {}

  getColumn(table: string, column: string): ColumnDefinition | undefined {
    if (table != this.alias) {
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
    if (table != this.alias) {
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
