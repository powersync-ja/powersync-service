import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from './ExpressionType.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

export interface TsSchemaGeneratorOptions {
  language?: TsSchemaLanguage;
  imports?: TsSchemaImports;
}

export enum TsSchemaLanguage {
  ts = 'ts',
  /** Excludes types from the generated schema. */
  js = 'js'
}

export enum TsSchemaImports {
  web = 'web',
  reactNative = 'reactNative',
  /**
   * Emits imports for `@powersync/web`, with comments for `@powersync/react-native`.
   */
  auto = 'auto'
}

export class TsSchemaGenerator extends SchemaGenerator {
  readonly key: string;
  readonly fileName: string;
  readonly mediaType: string;
  readonly label: string;

  readonly language: TsSchemaLanguage;

  constructor(public readonly options: TsSchemaGeneratorOptions = {}) {
    super();

    this.language = options.language ?? TsSchemaLanguage.ts;
    this.key = this.language;
    if (this.language == TsSchemaLanguage.ts) {
      this.fileName = 'schema.ts';
      this.mediaType = 'text/typescript';
      this.label = 'TypeScript';
    } else {
      this.fileName = 'schema.js';
      this.mediaType = 'text/javascript';
      this.label = 'JavaScript';
    }
  }

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `${this.generateImports()}

${tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n')}

export const AppSchema = new Schema({
  ${tables.map((table) => table.name).join(',\n  ')}
});

${this.generateTypeExports()}`;
  }

  private generateTypeExports() {
    if (this.language == TsSchemaLanguage.ts) {
      return `export type Database = (typeof AppSchema)['types'];\n`;
    } else {
      return ``;
    }
  }

  private generateImports() {
    const importStyle = this.options.imports ?? 'auto';
    if (importStyle == TsSchemaImports.web) {
      return `import { column, Schema, Table } from '@powersync/web';`;
    } else if (importStyle == TsSchemaImports.reactNative) {
      return `import { column, Schema, Table } from '@powersync/react-native';`;
    } else {
      return `import { column, Schema, Table } from '@powersync/web';
// OR: import { column, Schema, Table } from '@powersync/react-native';`;
    }
  }

  private generateTable(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    const generated = columns.map((c, i) => {
      const last = i == columns.length - 1;
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

    return `const ${name} = new Table(
  {
    // id column (text) is automatically included
${generated.join('\n')}
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
