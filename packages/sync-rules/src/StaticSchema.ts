import { ColumnDefinition, ExpressionType, expressionTypeFromPostgresType, SqliteType } from './ExpressionType.js';
import { SourceTableInterface } from './SourceTableInterface.js';
import { TablePattern } from './TablePattern.js';
import { SourceSchema, SourceSchemaTable } from './types.js';

export interface SourceSchemaDefinition {
  name: string;
  tables: SourceTableDefinition[];
}

export interface SourceTableDefinition {
  name: string;
  columns: SourceColumnDefinition[];
}

export interface SourceColumnDefinition {
  /**
   * Column name.
   */
  name: string;

  /**
   * Option 1: SQLite type flags - see ExpressionType.typeFlags.
   * Option 2: SQLite type name in lowercase - 'text' | 'integer' | 'real' | 'numeric' | 'blob' | 'null'
   */
  sqlite_type?: number | SqliteType;

  /**
   * Type name from the source database, e.g. "character varying(255)[]"
   */
  original_type?: string;

  /**
   * Postgres type, kept for backwards-compatibility.
   *
   * @deprecated - use original_type instead
   */
  pg_type?: string;
}

export interface SourceConnectionDefinition {
  tag: string;
  schemas: SourceSchemaDefinition[];
}

class SourceTableDetails implements SourceTableInterface, SourceSchemaTable {
  readonly connectionTag: string;
  readonly schema: string;
  readonly table: string;
  private readonly columns: Record<string, ColumnDefinition>;

  constructor(connection: SourceConnectionDefinition, schema: SourceSchemaDefinition, table: SourceTableDefinition) {
    this.connectionTag = connection.tag;
    this.schema = schema.name;
    this.table = table.name;
    this.columns = Object.fromEntries(
      table.columns.map((column) => {
        return [column.name, mapColumn(column)];
      })
    );
  }

  getType(column: string): ExpressionType | undefined {
    return this.columns[column]?.type;
  }

  getColumns(): ColumnDefinition[] {
    return Object.values(this.columns);
  }
}

export class StaticSchema implements SourceSchema {
  private tables: SourceTableDetails[];

  constructor(connections: SourceConnectionDefinition[]) {
    this.tables = [];
    for (let connection of connections) {
      for (let schema of connection.schemas) {
        for (let table of schema.tables) {
          this.tables.push(new SourceTableDetails(connection, schema, table));
        }
      }
    }
  }

  getTables(sourceTable: TablePattern): SourceSchemaTable[] {
    const filtered = this.tables.filter((t) => sourceTable.matches(t));
    return filtered;
  }
}

function mapColumn(column: SourceColumnDefinition): ColumnDefinition {
  return {
    name: column.name,
    type: mapColumnType(column)
  };
}

function mapColumnType(column: SourceColumnDefinition): ExpressionType {
  if (typeof column.sqlite_type == 'number') {
    return ExpressionType.of(column.sqlite_type);
  } else if (typeof column.sqlite_type == 'string') {
    return ExpressionType.fromTypeText(column.sqlite_type);
  } else if (column.pg_type != null) {
    // We still handle these types for backwards-compatibility of old schemas
    return expressionTypeFromPostgresType(column.pg_type);
  } else {
    throw new Error(`Cannot determine SQLite type of ${JSON.stringify(column)}`);
  }
}
