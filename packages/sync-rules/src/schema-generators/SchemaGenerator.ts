import { ColumnDefinition, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SqlSyncRules } from '../SqlSyncRules.js';
import { SourceSchema } from '../types.js';

export interface GenerateSchemaOptions {
  includeTypeComments?: boolean;
}

export abstract class SchemaGenerator {
  protected getAllTables(source: SqlSyncRules, schema: SourceSchema) {
    let tables: Record<string, Record<string, ColumnDefinition>> = {};

    for (let descriptor of source.bucket_descriptors) {
      for (let query of descriptor.data_queries) {
        const outTables = query.getColumnOutputs(schema);
        for (let table of outTables) {
          tables[table.name] ??= {};
          for (let column of table.columns) {
            if (column.name != 'id') {
              tables[table.name][column.name] ??= column;
            }
          }
        }
      }
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
  columnType(def: ColumnDefinition): string {
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
