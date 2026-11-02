import { Equatable, StableHasher } from './compiler/equality.js';
import { SourceTableRef } from './SourceTableRef.js';

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
    public readonly tablePattern: string,
    connectionTag?: string | null
  ) {
    if (connectionTag != null) {
      // A connection tag identifies a database on that connection, so a schema must be provided with it.
      if (schema == null) throw new Error('A schema is required when a connection tag is set.');
      this.connectionTag = connectionTag;
      this.schema = schema;
    } else if (schema) {
      const splitSchema = schema.split('.');
      if (splitSchema.length > 2) throw new Error(`Invalid schema: ${schema}`);
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

  get isWildcard() {
    return this.tablePattern.endsWith('%');
  }

  get isSchemaWildcard() {
    return this.schema?.endsWith('%') ?? false;
  }

  get name() {
    if (this.isWildcard) {
      throw new Error('Cannot get name for wildcard table');
    }
    return this.tablePattern;
  }

  toTablePattern(defaultSchema: string): TablePattern {
    // Preserve existing hydration behavior: explicit connection tags are stored but resolve to the default tag.
    // FIXME: When multiple connections are supported, honor explicit tags consistently across SQL, source-table options,
    // and hydration while preserving the interpretation of existing saved plans.
    // Pass a tag for explicit schemas so quoted dots stay literal; undefined lets runtime defaults retain qualification.
    return new TablePattern(
      this.schema ?? defaultSchema,
      this.tablePattern,
      this.schema == null ? undefined : DEFAULT_TAG
    );
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

  constructor(schema: string, tablePattern: string, connectionTag?: string) {
    super(schema, tablePattern, connectionTag);
  }

  /**
   * Unique key for this table pattern, used for caching.
   *
   * Do not use for persisted values.
   */
  key() {
    return JSON.stringify([this.connectionTag, this.schema, this.tablePattern]);
  }

  get tablePrefix() {
    if (!this.isWildcard) {
      throw new Error('Not a wildcard table');
    }
    return this.tablePattern.substring(0, this.tablePattern.length - 1);
  }

  get schemaPrefix() {
    if (!this.isSchemaWildcard) {
      throw new Error('Not a wildcard schema');
    }
    return this.schema.substring(0, this.schema.length - 1);
  }

  matches(table: SourceTableRef) {
    if (this.connectionTag != table.connectionTag) {
      return false;
    }
    if (this.isSchemaWildcard) {
      if (!table.schema.startsWith(this.schemaPrefix)) {
        return false;
      }
    } else if (this.schema != table.schema) {
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
