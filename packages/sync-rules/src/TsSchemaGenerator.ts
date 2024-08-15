import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from './ExpressionType.js';
import { SchemaGenerator } from './SchemaGenerator.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

export interface TsSchemaGeneratorOptions {
  language?: 'ts' | 'js';
  imports?: 'web' | 'react-native' | 'auto';
}
export class TsSchemaGenerator extends SchemaGenerator {
  readonly key = 'ts';
  readonly label = 'TypeScript';
  readonly mediaType = 'application/typescript';
  readonly fileName = 'schema.ts';

  constructor(public readonly options: TsSchemaGeneratorOptions = {}) {
    super();
  }

  generate(source: SqlSyncRules, schema: SourceSchema): string {
    const tables = super.getAllTables(source, schema);

    return `${this.generateImports()}

${tables.map((table) => this.generateTable(table.name, table.columns)).join('\n\n')}

export const AppSchema = new Schema({
  ${tables.map((table) => table.name).join(',\n  ')}
});

${this.generateTypeExports()}`;
  }

  private generateTypeExports() {
    const lang = this.options.language ?? 'ts';
    if (lang == 'ts') {
      return `export type Database = (typeof AppSchema)['types'];\n`;
    } else {
      return ``;
    }
  }

  private generateImports() {
    const importStyle = this.options.imports ?? 'auto';
    if (importStyle == 'web') {
      return `import { column, Schema, TableV2 } from '@powersync/web';`;
    } else if (importStyle == 'react-native') {
      return `import { column, Schema, TableV2 } from '@powersync/react-native';`;
    } else {
      return `import { column, Schema, TableV2 } from '@powersync/web';
// OR: import { column, Schema, TableV2 } from '@powersync/react-native';`;
    }
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
