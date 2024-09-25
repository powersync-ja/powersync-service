import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from './ExpressionType.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

export class DartSchemaGenerator extends SchemaGenerator {
  readonly key = 'dart';
  readonly label = 'Dart';
  readonly mediaType = 'text/x-dart';
  readonly fileName = 'schema.dart';

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `Schema([
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join(',\n  ')}
]);
`;
  }

  private generateTable(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    const generated = columns.map((c, i) => {
      const last = i == columns.length - 1;
      const base = this.generateColumn(c);
      let withFormatting: string;
      if (last) {
        withFormatting = `    ${base}`;
      } else {
        withFormatting = `    ${base},`;
      }

      if (options?.includeTypeComments && c.originalType != null) {
        return `${withFormatting} // ${c.originalType}`;
      } else {
        return withFormatting;
      }
    });
    return `Table('${name}', [
${generated.join('\n')}
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
