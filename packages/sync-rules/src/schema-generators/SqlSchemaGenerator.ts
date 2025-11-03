import { SqlSyncRules } from '../SqlSyncRules.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

/**
 * Generates a schema as `CREATE TABLE` statements, useful for libraries like drift or SQLDelight which can generate
 * typed rows or generate queries based on that.
 */
export class SqlSchemaGenerator extends SchemaGenerator {
  readonly key = 'sql';
  readonly mediaType = 'application/sql';

  constructor(
    readonly label: string,
    readonly fileName: string
  ) {
    super();
  }

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    let buffer =
      '-- Note: These definitions are only used to generate typed code. PowerSync manages the database schema.\n';
    const tables = super.getAllTables(source, schema);

    for (const table of tables) {
      buffer += `CREATE TABLE ${table.name}(\n`;

      buffer += '  id TEXT NOT NULL PRIMARY KEY';
      for (const column of table.columns) {
        const type = this.columnType(column).toUpperCase();
        buffer += `,\n  ${column.name} ${type}`;
      }

      buffer += '\n);\n';
    }

    return buffer;
  }
}
