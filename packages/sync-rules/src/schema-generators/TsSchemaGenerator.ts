import { SyncConfig } from '../SyncConfig.js';
import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

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

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);
    const streamsHelper = this.generateStreamHelper(source, schema);

    return `${this.generateImports(streamsHelper != null)}

${tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n')}

export const AppSchema = new Schema({
  ${tables.map((table) => table.name).join(',\n  ')}
});

${this.generateTypeExports()}
${streamsHelper ?? ''}`;
  }

  private generateTypeExports() {
    if (this.language == TsSchemaLanguage.ts) {
      return `export type Database = (typeof AppSchema)['types'];\n`;
    } else {
      return ``;
    }
  }

  private generateImports(includeSyncStreams: boolean) {
    const importStyle = this.options.imports ?? 'auto';
    const importedNames = ['column', 'Schema', 'Table'];
    if (includeSyncStreams) {
      importedNames.push(...['PowerSyncDatabase', 'SyncStream']);
    }
    const importStart = `import { ${importedNames.join(', ')} } from`;

    if (importStyle == TsSchemaImports.web) {
      return `${importStart} '@powersync/web';`;
    } else if (importStyle == TsSchemaImports.reactNative) {
      return `${importStart} '@powersync/react-native';`;
    } else {
      return `${importStart} '@powersync/web';
// OR: ${importStart} '@powersync/react-native';`;
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

  private generateStreamHelper(source: SyncConfig, schema: SourceSchema): string | undefined {
    const optionalSyncStreams = this.getOptionalStreams(source, schema);
    if (optionalSyncStreams.length) {
      let generatedCode = `export const typedStreams = Object.freeze({
`;

      const methods = optionalSyncStreams.map((stream) => {
        const allParams = Object.entries(stream.parameters);

        let args = 'db: PowerSyncDatabase';
        if (allParams.length) {
          const paramsType = allParams.map(([name, type]) => `${name}: ${this.valueType(type)}`).join(', ');
          args += `, params: {${paramsType}}`;
        }

        return `  ${toCamelCase(stream.name)}(${args}): SyncStream {
    return db.syncStream('${stream.name}', ${allParams.length ? 'params' : '{}'});
  }`;
      });

      generatedCode += methods.join(',\n');
      generatedCode += `\n});\n`;
      return generatedCode;
    }
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

  private valueType({ type }: ColumnType): string {
    if (type.typeFlags & (TYPE_INTEGER | TYPE_REAL)) {
      return 'number';
    } else {
      return 'string';
    }
  }
}
