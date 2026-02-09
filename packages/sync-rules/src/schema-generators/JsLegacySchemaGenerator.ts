import { BaseSyncConfig } from '../BaseSyncConfig.js';
import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { SchemaGenerator } from './SchemaGenerator.js';

export class JsLegacySchemaGenerator extends SchemaGenerator {
  readonly key = 'jsLegacy';
  readonly label = 'JavaScript (legacy syntax)';
  readonly mediaType = 'text/javascript';
  readonly fileName = 'schema.js';

  generate(source: BaseSyncConfig, schema: SourceSchema): string {
    const tables = super.getAllTables(source, schema);

    return `new Schema([
  ${tables.map((table) => this.generateTable(table.name, table.columns)).join(',\n  ')}
])
`;
  }

  private generateTable(name: string, columns: ColumnDefinition[]): string {
    return `new Table({
    name: '${name}',
    columns: [
      ${columns.map((c) => this.generateColumn(c)).join(',\n      ')}
    ]
  })`;
  }

  private generateColumn(column: ColumnDefinition) {
    const t = column.type;
    if (t.typeFlags & TYPE_TEXT) {
      return `new Column({ name: '${column.name}', type: ColumnType.TEXT })`;
    } else if (t.typeFlags & TYPE_REAL) {
      return `new Column({ name: '${column.name}', type: ColumnType.REAL })`;
    } else if (t.typeFlags & TYPE_INTEGER) {
      return `new Column({ name: '${column.name}', type: ColumnType.INTEGER })`;
    } else {
      return `new Column({ name: '${column.name}', type: ColumnType.TEXT })`;
    }
  }
}
