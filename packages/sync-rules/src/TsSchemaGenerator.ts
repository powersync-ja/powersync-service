import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from './ExpressionType.js';
import { SchemaGenerator } from './SchemaGenerator.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

export class TsSchemaGenerator extends SchemaGenerator {
  readonly key = 'ts';
  readonly label = 'TypeScript';
  readonly mediaType = 'application/typescript';
  readonly fileName = 'schema.ts';

  generate(source: SqlSyncRules, schema: SourceSchema): string {
    const tables = super.getAllTables(source, schema);

    return `import { column, Schema, TableV2 } from '@powersync/web';
// OR: import { column, Schema, TableV2 } from '@powersync/react-native';

${tables.map((table) => this.generateTable(table.name, table.columns)).join('\n\n')}

export const AppSchema = new Schema({
  ${tables.map((table) => table.name).join(',\n  ')}
});

export type Database = (typeof AppSchema)['types'];
`;
  }

  private generateTable(name: string, columns: ColumnDefinition[]): string {
    return `const ${name} = new TableV2(
  {
    // id column (text) is automatically included
    ${columns.map((c) => this.generateColumn(c)).join(',\n    ')}
  },
  { indexes: {} }
);`;
  }

  private generateColumn(column: ColumnDefinition) {
    const t = column.type;
    if (t.typeFlags & TYPE_TEXT) {
      return `${column.name}: column.text`;
    } else if (t.typeFlags & TYPE_REAL) {
      return `${column.name}: column.real`;
    } else if (t.typeFlags & TYPE_INTEGER) {
      return `${column.name}: column.integer`;
    } else {
      return `${column.name}: column.text`;
    }
  }
}
