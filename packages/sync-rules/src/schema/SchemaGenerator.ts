import { ColumnDefinition } from './ExpressionType.js';
import { SqlSyncRules } from './SqlSyncRules.js';
import { SourceSchema } from './types.js';

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
}
