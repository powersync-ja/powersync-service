import { JSONBig } from '@powersync/service-jsonbig';
import { SourceTableInterface } from './SourceTableInterface.js';
import { ParameterMatchClause, StaticFilter } from './types.js';

export const DEFAULT_TAG = 'default';

/**
 * Some pattern matching SourceTables.
 */
export class TablePattern {
  public readonly connectionTag: string;

  public readonly schema: string;
  public readonly tablePattern: string;

  public readonly filter?: StaticFilter;

  constructor(schema: string, tablePattern: string, filter?: StaticFilter) {
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
    this.filter = filter;
  }

  /**
   * Unique lookup key for this pattern. For in-memory use only - no gaurantee of stability across restarts.
   */
  get key(): string {
    return JSONBig.stringify([this.connectionTag, this.schema, this.tablePattern, this.filter]);
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

  withFilter(filter: StaticFilter | undefined): TablePattern {
    return new TablePattern(
      this.connectionTag == DEFAULT_TAG ? this.schema : `${this.connectionTag}.${this.schema}`,
      this.tablePattern,
      filter
    );
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
