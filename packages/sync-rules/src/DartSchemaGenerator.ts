import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from './ExpressionType.js';
import { SchemaGenerator } from './SchemaGenerator.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

export class DartSchemaGenerator extends SchemaGenerator {
  readonly key = 'dart';
  readonly label = 'Dart';
  readonly mediaType = 'text/x-dart';
  readonly fileName = 'schema.dart';

  generate(source: SqlSyncRules, schema: SourceSchema): string {
    const tables = super.getAllTables(source, schema);

    return `Schema([
  ${tables.map((table) => this.generateTable(table.name, table.columns)).join(',\n  ')}
]);
`;
  }

  private generateTable(name: string, columns: ColumnDefinition[]): string {
    return `Table('${name}', [
    ${columns.map((c) => this.generateColumn(c)).join(',\n    ')}
  ])`;
  }

  private generateColumn(column: ColumnDefinition) {
    const t = column.type;
    if (t.typeFlags & TYPE_TEXT) {
      return `Column.text('${column.name}')`;
    } else if (t.typeFlags & TYPE_REAL) {
      return `Column.real('${column.name}')`;
    } else if (t.typeFlags & TYPE_INTEGER) {
      return `Column.integer('${column.name}')`;
    } else {
      return `Column.text('${column.name}')`;
    }
  }
}
