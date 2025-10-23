import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SqlSyncRules } from '../SqlSyncRules.js';
import { SourceSchema } from '../types.js';

export interface GenerateSchemaOptions {
  includeTypeComments?: boolean;
}

export abstract class SchemaGenerator {
  protected getAllTables(source: SqlSyncRules, schema: SourceSchema) {
    let tables: Record<string, Record<string, ColumnDefinition>> = {};

    for (let descriptor of source.bucketSources) {
      descriptor.resolveResultSets(schema, tables);
    }

    return Object.entries(tables).map(([name, columns]) => {
      return {
        name: name,
        columns: Object.values(columns)
      };
    });
  }

  abstract readonly key: string;
  abstract readonly label: string;
  abstract readonly mediaType: string;
  abstract readonly fileName: string;

  abstract generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string;

  /**
   * @param def The column definition to generate the type for.
   * @returns The SDK column type for the given column definition.
   */
  columnType(def: ColumnDefinition): 'text' | 'real' | 'integer' {
    const { type } = def;
    if (type.typeFlags & TYPE_TEXT) {
      return 'text';
    } else if (type.typeFlags & TYPE_REAL) {
      return 'real';
    } else if (type.typeFlags & TYPE_INTEGER) {
      return 'integer';
    } else {
      return 'text';
    }
  }
}
