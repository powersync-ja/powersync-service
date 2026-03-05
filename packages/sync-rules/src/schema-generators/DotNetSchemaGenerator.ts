import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

export class DotNetSchemaGenerator extends SchemaGenerator {
  readonly key = 'dotnet';
  readonly label = '.Net';
  readonly mediaType = 'text/x-csharp';
  readonly fileName = 'Schema.cs';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    let generated = `using PowerSync.Common.DB.Schema;

class AppSchema
{
${tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n')}

    public static Schema PowerSyncSchema = new Schema(${tables.map((table) => this.toUpperCaseFirstLetter(table.name)).join(', ')});
}`;
    const streamHelper = this.generateStreamHelper(source, schema);
    generated += `\n${streamHelper}\n`;
    return generated;
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

  private generateStreamHelper(source: SyncConfig, schema: SourceSchema): string | undefined {
    const optionalSyncStreams = this.getOptionalStreams(source, schema);
    if (optionalSyncStreams.length) {
      let generatedCode = `
public readonly ref struct TypedSyncStreams(PowerSyncDatabase db)
{
    private PowerSyncDatabase db { get; } = db;
`;

      for (const stream of optionalSyncStreams) {
        const entries = Object.entries(stream.parameters);
        let methodParameters = entries
          .map(([parameter, type]) => `${this.cSharpType(type)} ${toCamelCase(parameter)}`)
          .join(', ');

        let parameterDictionary: string;
        if (entries.length) {
          parameterDictionary =
            entries.map(([parameter]) => `\n            { "${parameter}", ${toCamelCase(parameter)} }`).join(',') +
            '\n        ';
        } else {
          parameterDictionary = '';
        }

        generatedCode += `    public ISyncStream ${toCamelCase(stream.name, true)}(${methodParameters})
    {
        var parameters = new Dictionary<string, object>() {${parameterDictionary}};
        return db.SyncStream("${stream.name}", parameters);
    }
`;
      }

      generatedCode += `}`;
      return generatedCode;
    }
  }

  private cSharpType({ type }: ColumnType): string {
    if (type.typeFlags & TYPE_TEXT) {
      return 'string';
    } else if (type.typeFlags & TYPE_REAL) {
      return 'double';
    } else if (type.typeFlags & TYPE_INTEGER) {
      return 'int';
    } else {
      return 'string';
    }
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
