import { SourceTableInterface } from './SourceTableInterface.js';

export const DEFAULT_TAG = 'default';

/**
 * Some pattern matching SourceTables.
 */
export class TablePattern {
  public readonly connectionTag: string;

  public readonly schema: string;
  public readonly tablePattern: string;

  constructor(schema: string, tablePattern: string) {
    const splitSchema = schema.split('.');
    if (splitSchema.length > 2) {
      throw new Error(`Invalid schema: ${schema}`);
    }
    if (splitSchema.length == 2) {
      this.connectionTag = splitSchema[0];
      this.schema = splitSchema[1];
    } else {
      this.connectionTag = DEFAULT_TAG;
      this.schema = schema;
    }
    this.tablePattern = tablePattern;
  }

  /**
   * Unique lookup key for this pattern. For in-memory use only - no gaurantee of stability across restarts.
   */
  get key(): string {
    return JSON.stringify([this.connectionTag, this.schema, this.tablePattern]);
  }

  equals(other: TablePattern): boolean {
    return this.key == other.key;
  }

  get isWildcard() {
    return this.tablePattern.endsWith('%');
  }

  get tablePrefix() {
    if (!this.isWildcard) {
      throw new Error('Not a wildcard table');
    }
    return this.tablePattern.substring(0, this.tablePattern.length - 1);
  }

  get name() {
    if (this.isWildcard) {
      throw new Error('Cannot get name for wildcard table');
    }
    return this.tablePattern;
  }

  matches(table: SourceTableInterface) {
    if (this.connectionTag != table.connectionTag || this.schema != table.schema) {
      return false;
    }
    if (this.isWildcard) {
      return table.name.startsWith(this.tablePrefix);
    } else {
      return this.tablePattern == table.name;
    }
  }

  suffix(table: string) {
    if (!this.isWildcard) {
      return '';
    }
    return table.substring(this.tablePrefix.length);
  }
}
