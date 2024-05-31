import { ColumnDefinition, ExpressionType } from './ExpressionType.js';
import { QuerySchema, SourceSchemaTable } from './types.js';

export class TableQuerySchema implements QuerySchema {
  constructor(private tables: SourceSchemaTable[], private alias: string) {}

  getType(table: string, column: string): ExpressionType {
    if (table != this.alias) {
      return ExpressionType.NONE;
    }
    for (let table of this.tables) {
      const t = table.getType(column);
      if (t != null) {
        return t;
      }
    }
    return ExpressionType.NONE;
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
