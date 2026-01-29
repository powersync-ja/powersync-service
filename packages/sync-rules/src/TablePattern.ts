import { Equatable, StableHasher } from './compiler/equality.js';
import { SourceTableInterface } from './SourceTableInterface.js';

export const DEFAULT_TAG = 'default';

/**
 * Some pattern matching SourceTables.
 */
export class TablePattern implements Equatable {
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

  buildHash(hasher: StableHasher): void {
    hasher.addString(this.connectionTag);
    hasher.addString(this.schema);
    hasher.addString(this.tablePattern);
  }

  equals(other: unknown): boolean {
    return (
      other instanceof TablePattern &&
      other.connectionTag == this.connectionTag &&
      other.schema == this.schema &&
      other.tablePattern == this.tablePattern
    );
  }
}
