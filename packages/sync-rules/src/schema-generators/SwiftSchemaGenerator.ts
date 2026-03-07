import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SyncConfig } from '../SyncConfig.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

export class SwiftSchemaGenerator extends SchemaGenerator {
  readonly key = 'swift';
  readonly label = 'Swift';
  readonly mediaType = 'text/x-swift';
  readonly fileName = 'schema.swift';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);
    const streamsHelper = this.generateStreamHelper(source, schema);

    return `import PowerSync

let schema = Schema(
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join(',\n  ')}
)
${streamsHelper ?? ''}
`;
  }

  private generateTable(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    const generated = columns.map((c, i) => {
      const last = i === columns.length - 1;
      const base = this.generateColumn(c);
      let withFormatting: string;
      if (last) {
        withFormatting = `        ${base}`;
      } else {
        withFormatting = `        ${base},`;
      }

      if (options?.includeTypeComments && c.originalType != null) {
        return `${withFormatting} // ${c.originalType}`;
      } else {
        return withFormatting;
      }
    });
    return `Table(
    name: "${name}",
    columns: [
${generated.join('\n')}
    ]
  )`;
  }

  private generateColumn(column: ColumnDefinition): string {
    return `.${this.columnType(column)}("${column.name}")`;
  }

  private generateStreamHelper(source: SyncConfig, schema: SourceSchema): string | undefined {
    const optionalSyncStreams = this.getOptionalStreams(source, schema);
    if (optionalSyncStreams.length) {
      let generatedCode = `
struct TypedSyncStreams {
    private var db: PowerSyncDatabaseProtocol
    init(_ db: PowerSyncDatabaseProtocol) {
        self.db = db
    }
`;

      for (const stream of optionalSyncStreams) {
        const entries = Object.entries(stream.parameters);
        let swiftParameters = entries
          .map(([parameter, type]) => `${toCamelCase(parameter)}: ${this.swiftType(type)}`)
          .join(', ');

        let parameterMap: string;
        if (entries.length) {
          parameterMap = '[\n';
          for (const [parameter, type] of entries) {
            parameterMap += `            "${parameter}": ${this.swiftJsonParamType(type)}(${toCamelCase(parameter)})\n`;
          }
          parameterMap += '        ]';
        } else {
          parameterMap = '[:]';
        }

        generatedCode += `    func ${toCamelCase(stream.name)}(${swiftParameters}) -> SyncStream {
        return db.syncStream(name: "${stream.name}", params: ${parameterMap})
    }
`;
      }

      generatedCode += `}`;
      return generatedCode;
    }
  }

  private swiftType({ type }: ColumnType): string {
    if (type.typeFlags & TYPE_TEXT) {
      return 'String';
    } else if (type.typeFlags & TYPE_REAL) {
      return 'Double';
    } else if (type.typeFlags & TYPE_INTEGER) {
      return 'Int';
    } else {
      return 'String';
    }
  }

  private swiftJsonParamType({ type }: ColumnType): string {
    if (type.typeFlags & TYPE_INTEGER) {
      return 'JsonValue.int';
    } else if (type.typeFlags & TYPE_REAL) {
      return 'JsonValue.double';
    } else {
      return 'JsonValue.string';
    }
  }
}
