import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition, ExpressionType } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

export class DartSchemaGenerator extends SchemaGenerator {
  readonly key = 'dart';
  readonly label = 'Dart';
  readonly mediaType = 'text/x-dart';
  readonly fileName = 'schema.dart';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
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
    return `Column.${this.columnType(column)}('${column.name}')`;
  }
}
