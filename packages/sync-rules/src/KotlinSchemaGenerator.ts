import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from './ExpressionType.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

export class KotlinSchemaGenerator extends SchemaGenerator {
  readonly key = 'kotlin';
  readonly label = 'Kotlin';
  readonly mediaType = 'text/x-kotlin';
  readonly fileName = 'schema.kt';

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `val schema = Schema(
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
    return `Column.${kotlinColumnType(column)}("${column.name}")`;
  }
}

const kotlinColumnType = (def: ColumnDefinition): string => {
  const t = def.type;
  if (t.typeFlags & TYPE_TEXT) {
    return 'text';
  } else if (t.typeFlags & TYPE_REAL) {
    return 'real';
  } else if (t.typeFlags & TYPE_INTEGER) {
    return 'integer';
  } else {
    return 'text';
  }
};
