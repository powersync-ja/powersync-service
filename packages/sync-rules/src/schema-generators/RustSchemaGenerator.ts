import { ColumnDefinition } from '../ExpressionType.js';
import { SyncConfig } from '../SyncConfig.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

/**
 * Generates the client-side schema for the PowerSync Rust SDK, in the shape its reference docs use:
 * an `app_schema()` function that pushes one `Table::create(...)` per synced table.
 *
 * Typed sync stream helpers are not generated for Rust yet.
 */
export class RustSchemaGenerator extends SchemaGenerator {
  readonly key = 'rust';
  readonly label = 'Rust';
  readonly mediaType = 'text/x-rust';
  readonly fileName = 'schema.rs';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);
    const pushes = tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n');

    return `use powersync::schema::{Column, Schema, Table};

pub fn app_schema() -> Schema {
    let mut schema = Schema::default();

${pushes}

    schema
}
`;
  }

  private generateTable(name: string, columns: ColumnDefinition[], options?: GenerateSchemaOptions): string {
    const lines = columns.map((column) => {
      const line = `            ${this.generateColumn(column)},`;
      if (options?.includeTypeComments && column.originalType != null) {
        return `${line} // ${column.originalType}`;
      }
      return line;
    });
    const columnsVec = lines.length > 0 ? `vec![\n${lines.join('\n')}\n        ]` : 'vec![]';

    return `    schema.tables.push(Table::create(
        ${rustString(name)},
        ${columnsVec},
        |_| {},
    ));`;
  }

  private generateColumn(column: ColumnDefinition): string {
    return `Column::${this.columnType(column)}(${rustString(column.name)})`;
  }
}

const RUST_STRING_ESCAPES: Record<string, string> = {
  '\\': '\\\\',
  '"': '\\"',
  '\n': '\\n',
  '\r': '\\r',
  '\t': '\\t'
};

/**
 * Wraps a name in a Rust string literal, escaping characters that would break or distort it.
 */
function rustString(value: string): string {
  return `"${value.replace(/[\\"\n\r\t]/g, (c) => RUST_STRING_ESCAPES[c])}"`;
}
