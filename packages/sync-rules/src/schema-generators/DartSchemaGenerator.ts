import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SyncConfig } from '../SyncConfig.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

export class DartSchemaGenerator extends SchemaGenerator {
  readonly key = 'dart';
  readonly label = 'Dart';
  readonly mediaType = 'text/x-dart';
  readonly fileName = 'schema.dart';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    let generatedCode = `Schema([
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join(',\n  ')}
]);
`;

    const optionalSyncStreams = this.getOptionalStreams(source, schema);
    if (optionalSyncStreams.length) {
      generatedCode += '\nextension type TypedSyncStreams(PowerSyncDatabase _db) {\n';

      for (const stream of optionalSyncStreams) {
        let dartParameters = Object.entries(stream.parameters)
          .map(([parameter, type]) => `required ${this.dartType(type)} ${toCamelCase(parameter)}`)
          .join(', ');
        if (dartParameters.length) {
          dartParameters = `{${dartParameters}}`;
        }

        const dartMap = Object.keys(stream.parameters)
          .map((parameter) => `'${parameter}': ${toCamelCase(parameter)},`)
          .join(', ');

        generatedCode += `  SyncStream ${toCamelCase(stream.name)}(${dartParameters}) => _db.syncStream('${stream.name}', {${dartMap}});\n`;
      }

      generatedCode += `}
`;
    }

    return generatedCode;
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

  private dartType({ type }: ColumnType): string {
    if (type.typeFlags & TYPE_TEXT) {
      return 'String';
    } else if (type.typeFlags & TYPE_REAL) {
      return 'double';
    } else if (type.typeFlags & TYPE_INTEGER) {
      return 'int';
    } else {
      return 'String';
    }
  }
}
