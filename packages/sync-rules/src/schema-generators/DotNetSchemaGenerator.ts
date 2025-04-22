import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SqlSyncRules } from '../SqlSyncRules.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

export class DotNetSchemaGenerator extends SchemaGenerator {
  readonly key = 'dotnet';
  readonly label = 'DotNet';
  readonly mediaType = 'text/x-csharp';
  readonly fileName = 'Schema.cs';

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `class AppSchema
{
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n  ')}

  public static Schema PowerSyncSchema = new Schema(new Dictionary<string, Table>
  {
    ${tables.map((table) => `{"${table.name}", ${this.toUpperCaseFirstLetter(table.name)}}`).join(',\n    ')}
  });
}`;
  }

  private toUpperCaseFirstLetter(str: string): string {
    return str.charAt(0).toUpperCase() + str.slice(1);
  }

  private generateTable(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    const generated = columns.map((c, i) => {
      const last = i === columns.length - 1;
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
    return `public static Table ${this.toUpperCaseFirstLetter(name)} = new Table(new Dictionary<string, ColumnType>
  {
  ${generated.join('\n  ')}
  });`;
  }

  private generateColumn(column: ColumnDefinition): string {
    return `{ "${column.name}", ${cSharpColumnType(column)} }`;
  }
}

const cSharpColumnType = (def: ColumnDefinition): string => {
  const t = def.type;
  if (t.typeFlags & TYPE_TEXT) {
    return 'ColumnType.TEXT';
  } else if (t.typeFlags & TYPE_REAL) {
    return 'ColumnType.REAL';
  } else if (t.typeFlags & TYPE_INTEGER) {
    return 'ColumnType.INTEGER';
  } else {
    return 'ColumnType.TEXT';
  }
};
