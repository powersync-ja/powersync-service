import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

export class KotlinSchemaGenerator extends SchemaGenerator {
  readonly key = 'kotlin';
  readonly label = 'Kotlin';
  readonly mediaType = 'text/x-kotlin';
  readonly fileName = 'schema.kt';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `import com.powersync.db.schema.Column
import com.powersync.db.schema.Schema
import com.powersync.db.schema.Table

val schema = Schema(
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join(',\n  ')}
)`;
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
}
