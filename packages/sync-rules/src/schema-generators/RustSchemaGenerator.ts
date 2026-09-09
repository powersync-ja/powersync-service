import { ColumnDefinition, ColumnType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SyncConfig } from '../SyncConfig.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, OptionalStream, SchemaGenerator, toCamelCase } from './SchemaGenerator.js';

/**
 * Generates the client-side schema for the PowerSync Rust SDK, in the shape its reference docs use:
 * an `app_schema()` function that pushes one `Table::create(...)` per synced table.
 *
 * When the sync configuration has optional streams, a `TypedSyncStreams` helper is generated with
 * one method per stream.
 */
export class RustSchemaGenerator extends SchemaGenerator {
  readonly key = 'rust';
  readonly label = 'Rust';
  readonly mediaType = 'text/rust';
  readonly fileName = 'schema.rs';

  generate(source: SyncConfig, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);
    const streams = this.getOptionalStreams(source, schema);
    const pushes = tables.map((table) => this.generateTable(table.name, table.columns, options)).join('\n\n');

    // Import order matches what rustfmt produces.
    let imports = 'use powersync::schema::{Column, Schema, Table};';
    if (streams.length) {
      imports += '\nuse powersync::{PowerSyncDatabase, SyncStream};';
      if (streams.some((stream) => Object.keys(stream.parameters).length > 0)) {
        imports += '\nuse serde_json::json;';
      }
    }

    const streamHelper = streams.length ? `\n${this.generateStreamHelper(streams)}` : '';

    return `${imports}

pub fn app_schema() -> Schema {
    let mut schema = Schema::default();

${pushes}

    schema
}
${streamHelper}`;
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

  private generateStreamHelper(streams: OptionalStream[]): string {
    const methods = streams.map((stream) => this.generateStreamMethod(stream)).join('\n\n');

    // The field must be pub so that the helper can be constructed from outside the generated module.
    return `pub struct TypedSyncStreams<'a>(pub &'a PowerSyncDatabase);

impl<'a> TypedSyncStreams<'a> {
${methods}
}
`;
  }

  private generateStreamMethod(stream: OptionalStream): string {
    const entries = Object.entries(stream.parameters);
    const name = rustString(stream.name);

    if (entries.length == 0) {
      return `    pub fn ${toSnakeCase(stream.name)}(&self) -> SyncStream<'a> {
        self.0.sync_stream(${name}, None)
    }`;
    }

    const args = entries.map(([parameter, type]) => `${toSnakeCase(parameter)}: ${this.rustType(type)}`).join(', ');
    const jsonEntries = entries.map(([parameter]) => `${rustString(parameter)}: ${toSnakeCase(parameter)}`).join(', ');

    return `    pub fn ${toSnakeCase(stream.name)}(&self, ${args}) -> SyncStream<'a> {
        let encoded_params = json!({${jsonEntries}});

        self.0.sync_stream(${name}, Some(&encoded_params))
    }`;
  }

  private rustType({ type }: ColumnType): string {
    if (type.typeFlags & TYPE_TEXT) {
      // The SDK clones parameters internally, so &str is more convenient for callers than String.
      return '&str';
    } else if (type.typeFlags & TYPE_REAL) {
      return 'f64';
    } else if (type.typeFlags & TYPE_INTEGER) {
      return 'i64';
    } else {
      return '&str';
    }
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

/**
 * Converts a name to a snake_case Rust identifier, e.g. `myStream` and `my-stream` become `my_stream`.
 */
function toSnakeCase(source: string): string {
  return toCamelCase(source)
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase();
}
