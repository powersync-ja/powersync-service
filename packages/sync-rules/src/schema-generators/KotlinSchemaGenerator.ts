import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SyncConfig } from '../SyncConfig.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

export class KotlinSchemaGenerator extends SchemaGenerator {
  readonly key = 'kotlin';
  readonly label = 'Kotlin';
  readonly mediaType = 'text/x-kotlin';
  readonly fileName = 'schema.kt';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    let imports = `import com.powersync.db.schema.Column
import com.powersync.db.schema.Schema
import com.powersync.db.schema.Table
`;

    const generatedSchema = `val schema = Schema(
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join(',\n  ')}
)
`;

    const streams = this.generateStreamHelperForKotlin(source, schema);
    if (streams != null) {
      imports += `import com.powersync.PowerSyncDatabase
import com.powersync.sync.SyncStream
import com.powersync.utils.JsonParam
import kotlin.jvm.JvmInline
`;
      return `${imports}\n${generatedSchema}\n${streams}`;
    }

    return `${imports}\n${generatedSchema}`;
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
    name = "${name}",
    columns = listOf(
${generated.join('\n')}
    )
  )`;
  }

  private generateColumn(column: ColumnDefinition): string {
    return `Column.${this.columnType(column)}("${column.name}")`;
  }

  private generateStreamHelperForKotlin(source: SyncConfig, schema: SourceSchema): string | undefined {
    const optionalSyncStreams = this.getOptionalStreams(source, schema);
    if (optionalSyncStreams.length) {
      let generatedCode = `@JvmInline
value class TypedSyncStreams(private val db: PowerSyncDatabase) {
`;

      for (const stream of optionalSyncStreams) {
        const entries = Object.entries(stream.parameters);
        let kotlinParameters = entries
          .map(([parameter, type]) => `${toCamelCase(parameter)}: ${this.kotlinType(type)}`)
          .join(', ');

        let parameterMap: string;
        if (entries.length) {
          parameterMap = 'buildMap {\n';
          for (const [parameter, type] of entries) {
            parameterMap += `    put("${parameter}", ${this.kotlinJsonParamType(type)}(${toCamelCase(parameter)}))\n`;
          }
          parameterMap += '  }';
        } else {
          parameterMap = 'emptyMap()';
        }

        generatedCode += `  fun ${toCamelCase(stream.name)}(${kotlinParameters}): SyncStream = db.syncStream("${stream.name}", ${parameterMap})\n`;
      }

      generatedCode += `}
`;
      return generatedCode;
    }
  }

  private kotlinType({ type }: ColumnType): string {
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

  private kotlinJsonParamType({ type }: ColumnType): string {
    if (type.typeFlags & (TYPE_INTEGER | TYPE_REAL)) {
      return 'JsonParam.Number';
    } else {
      return 'JsonParam.String';
    }
  }
}
