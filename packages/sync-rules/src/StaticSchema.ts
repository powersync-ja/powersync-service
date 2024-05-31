import { ColumnDefinition, ExpressionType } from './ExpressionType.js';
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
  name: string;
  /**
   * Postgres type.
   */
  pg_type: string;
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
    type: mapType(column.pg_type)
  };
}

function mapType(type: string | undefined): ExpressionType {
  if (type?.endsWith('[]')) {
    return ExpressionType.TEXT;
  }
  switch (type) {
    case 'bool':
      return ExpressionType.INTEGER;
    case 'bytea':
      return ExpressionType.BLOB;
    case 'int2':
    case 'int4':
    case 'int8':
    case 'oid':
      return ExpressionType.INTEGER;
    case 'float4':
    case 'float8':
      return ExpressionType.REAL;
    default:
      return ExpressionType.TEXT;
  }
}
