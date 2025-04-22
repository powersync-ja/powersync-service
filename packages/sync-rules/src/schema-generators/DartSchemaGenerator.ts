import { ColumnDefinition, ExpressionType, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '../ExpressionType.js';
import { SqlSyncRules } from '../SqlSyncRules.js';
import { SourceSchema } from '../types.js';
import { GenerateSchemaOptions, SchemaGenerator } from './SchemaGenerator.js';

export class DartSchemaGenerator extends SchemaGenerator {
  readonly key = 'dart';
  readonly label = 'Dart';
  readonly mediaType = 'text/x-dart';
  readonly fileName = 'schema.dart';

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const tables = super.getAllTables(source, schema);

    return `Schema([
  ${tables.map((table) => this.generateTable(table.name, table.columns, options)).join(',\n  ')}
]);
`;
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
    return `Table('${name}', [
${generated.join('\n')}
  ])`;
  }

  private generateColumn(column: ColumnDefinition) {
    return `Column.${dartColumnType(column)}('${column.name}')`;
  }
}

export class DartFlutterFlowSchemaGenerator extends SchemaGenerator {
  readonly key = 'dart-flutterflow';
  readonly label = 'FlutterFlow';
  readonly mediaType = 'application/json';
  readonly fileName = 'schema.json';

  generate(source: SqlSyncRules, schema: SourceSchema, options?: GenerateSchemaOptions): string {
    const serializedTables = this.getAllTables(source, schema).map((e) => this.generateTable(e.name, e.columns));
    // Not all FlutterFlow apps will use the attachments queue table, but it needs to be part of the app schema if used
    // and does no harm otherwise. So, we just include it by default.
    serializedTables.push(
      this.generateTable(
        'attachments_queue',
        [
          {
            name: 'filename',
            type: ExpressionType.TEXT
          },
          {
            name: 'local_uri',
            type: ExpressionType.TEXT
          },
          {
            name: 'timestamp',
            type: ExpressionType.INTEGER
          },
          {
            name: 'size',
            type: ExpressionType.INTEGER
          },
          {
            name: 'media_type',
            type: ExpressionType.TEXT
          },
          {
            name: 'state',
            type: ExpressionType.INTEGER
          }
        ],
        true
      )
    );

    return JSON.stringify({ tables: serializedTables });
  }

  private generateTable(name: string, columns: ColumnDefinition[], localOnly = false): object {
    return {
      name,
      view_name: null,
      local_only: localOnly,
      insert_only: false,
      columns: columns.map(this.generateColumn),
      indexes: []
    };
  }

  private generateColumn(definition: ColumnDefinition): object {
    return {
      name: definition.name,
      type: dartColumnType(definition)
    };
  }
}

const dartColumnType = (def: ColumnDefinition) => {
  const t = def.type;
  if (t.typeFlags & TYPE_TEXT) {
    return 'text';
  } else if (t.typeFlags & TYPE_REAL) {
    return 'real';
  } else if (t.typeFlags & TYPE_INTEGER) {
    return 'integer';
  } else {
    return 'text';
  }
};
