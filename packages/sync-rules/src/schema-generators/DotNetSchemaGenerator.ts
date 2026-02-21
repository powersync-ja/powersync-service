import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

export class DotNetSchemaGenerator extends SchemaGenerator {
  readonly key = 'dotnet';
  readonly label = '.Net';
  readonly mediaType = 'text/x-csharp';
  readonly fileName = 'Schema.cs';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `using PowerSync.Common.DB.Schema;

class AppSchema
{
${tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n')}

    public static Schema PowerSyncSchema = new Schema(${tables.map((table) => this.toUpperCaseFirstLetter(table.name)).join(', ')});
}`;
  }

  private toUpperCaseFirstLetter(str: string): string {
    return str.charAt(0).toUpperCase() + str.slice(1);
  }

  private generateTable(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    const includeComments = options?.includeTypeComments;
    const generated = columns.map((c) => {
      const base = this.generateColumn(c);
      let line = `            ${base},`;
      if (includeComments && c.originalType != null) {
        line += ` // ${c.originalType}`;
      }
      return line;
    });

    return `    public static Table ${this.toUpperCaseFirstLetter(name)} = new Table
    {
        Name = "${name}",
        Columns =
        {
${generated.join('\n')}
        },
    };`;
  }

  private generateColumn(column: ColumnDefinition): string {
    return `["${column.name}"] = ${cSharpColumnType(column)}`;
  }
}

const cSharpColumnType = (def: ColumnDefinition): string => {
  const t = def.type;
  if (t.typeFlags & TYPE_TEXT) {
    return 'ColumnType.Text';
  } else if (t.typeFlags & TYPE_REAL) {
    return 'ColumnType.Real';
  } else if (t.typeFlags & TYPE_INTEGER) {
    return 'ColumnType.Integer';
  } else {
    return 'ColumnType.Text';
  }
};
