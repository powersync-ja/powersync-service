import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, OptionalStream, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

export class DotNetSchemaGenerator extends SchemaGenerator {
  readonly key = 'dotnet';
  readonly label = '.NET';
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

    const optionalStreams = this.getOptionalStreams(source, schema);
    const streamHelper = generateCSharpStreamHelper(optionalStreams);
    generated += `\n${streamHelper}\n`;

    return generated;
  }

  private toUpperCaseFirstLetter(str: string): string {
    return str.charAt(0).toUpperCase() + (str.length > 1 ? str.slice(1) : '');
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

export class DotNetClassSchemaGenerator extends SchemaGenerator {
  readonly key = 'dotnet-class';
  readonly label = '.NET (class-based)';
  readonly mediaType = 'text/x-csharp';
  readonly fileName = 'Schema.cs';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);
    let generated = `using PowerSync.Common.DB.Schema;
using PowerSync.Common.DB.Schema.Attributes;

${tables.map((table) => this.generateTableClass(table.name, table.columns, options)).join('\n\n')}

public class AppSchema
{
    public static Schema PowerSyncSchema = new Schema(${tables.map((t) => `typeof(${this.getTableClassName(t.name)})`).join(', ')});
}
`;

    const optionalStreams = this.getOptionalStreams(source, schema);
    const streamHelper = generateCSharpStreamHelper(optionalStreams);
    if (streamHelper) {
      generated += `${streamHelper}\n`;
    }

    return generated;
  }

  generateTableClass(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    return `[Table("${name}")]
public class ${this.getTableClassName(name)}
{
    // An "id" property is required when using a class-based schema.
    [Column("id")]
    public string Id { get; set; }

${columns.map((c) => this.generateProperty(c, options)).join('\n\n')}
}`;
  }

  generateProperty(column: ColumnDefinition, options?: GenerateSchemaOptions): string {
    const columnAttr = `    [Column("${column.name}")]`;
    const definition = `    public ${cSharpType(column)} ${toCamelCase(column.name, true)} { get; set; }`;

    if (options?.includeTypeComments && column.originalType) {
      return `${columnAttr}\n${definition} // ${column.originalType}`;
    } else {
      return `${columnAttr}\n${definition}`;
    }
  }

  protected getTableClassName(name: string): string {
    return `${toCamelCase(name, true)}Item`;
  }
}

const generateCSharpStreamHelper = (optionalSyncStreams: OptionalStream[]): string | undefined => {
  if (!optionalSyncStreams.length) return;

  let generatedCode = `
public readonly ref struct TypedSyncStreams(PowerSyncDatabase db)
{
    private PowerSyncDatabase db { get; } = db;
`;

  for (const stream of optionalSyncStreams) {
    const entries = Object.entries(stream.parameters);
    let methodParameters = entries
      .map(([parameter, type]) => `${cSharpType(type)} ${toCamelCase(parameter)}`)
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
};

const cSharpType = ({ type }: ColumnType): string => {
  if (type.typeFlags & TYPE_TEXT) {
    return 'string';
  } else if (type.typeFlags & TYPE_REAL) {
    return 'double';
  } else if (type.typeFlags & TYPE_INTEGER) {
    return 'int';
  } else {
    return 'string';
  }
};

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
