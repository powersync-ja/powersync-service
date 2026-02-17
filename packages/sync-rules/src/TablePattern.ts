import { Equatable, StableHasher } from './compiler/equality.js';
import { SourceTableInterface } from './SourceTableInterface.js';

export const DEFAULT_TAG = 'default';

/**
 * A variant of {@link TablePattern} that doesn't require a schema.
 *
 * While we'll always have a default schema when parsing sync configurations, sync plans also need to be stored in a
 * serialized form. There is no guarantee that the default schema used to compile a plan is the same as the one used
 * when loading it, so we can't apply a default value and store it.
 *
 * This class doesn't apply a default schema when constructed.
 */
export class ImplicitSchemaTablePattern implements Equatable {
  public readonly connectionTag: string | null;
  public readonly schema: string | null;

  constructor(
    schema: string | null,
    public readonly tablePattern: string
  ) {
    if (schema) {
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
    } else {
      this.connectionTag = null;
      this.schema = null;
    }
  }

  /**
   * Unique lookup key for this pattern. For in-memory use only - no gaurantee of stability across restarts.
   */
  get key(): string {
    return JSON.stringify([this.connectionTag, this.schema, this.tablePattern]);
  }

  get isWildcard() {
    return this.tablePattern.endsWith('%');
  }

  get name() {
    if (this.isWildcard) {
      throw new Error('Cannot get name for wildcard table');
    }
    return this.tablePattern;
  }

  toTablePattern(defaultSchema: string): TablePattern {
    return new TablePattern(this.schema ?? defaultSchema, this.tablePattern);
  }

  buildHash(hasher: StableHasher): void {
    if (this.connectionTag) {
      hasher.addString(this.connectionTag);
    }
    if (this.schema) {
      hasher.addString(this.schema);
    }

    hasher.addString(this.tablePattern);
  }

  equals(other: unknown): boolean {
    return (
      other instanceof ImplicitSchemaTablePattern &&
      other.connectionTag == this.connectionTag &&
      other.schema == this.schema &&
      other.tablePattern == this.tablePattern
    );
  }
}

/**
 * Some pattern matching SourceTables.
 */
export class TablePattern extends ImplicitSchemaTablePattern {
  declare public readonly connectionTag: string;
  declare public readonly schema: string;

  constructor(schema: string, tablePattern: string) {
    super(schema, tablePattern);
  }

  get tablePrefix() {
    if (!this.isWildcard) {
      throw new Error('Not a wildcard table');
    }
    return this.tablePattern.substring(0, this.tablePattern.length - 1);
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
