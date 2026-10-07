import type { JsonObject } from './json.js';
import { MONGO_FILTER_VALIDATOR, type MongoTableFilter } from './mongo/MongoFilterExpression.js';
import { DEFAULT_TAG, ImplicitSchemaTablePattern, TablePattern } from './TablePattern.js';

const SOURCE_TABLE_NAME_ERROR =
  'Source table patterns must use <table>, <database>.<table>, or <connection>.<database>.<table>. Double-quote names containing dots or quotes.';

/**
 * Options for one entry in `config.source_table_options`. MongoDB filtering is parsed in every edition; execution support is validated by the source adapter.
 */
export interface SourceTableConfig {
  mongodb_filter_expression?: MongoTableFilter;
}

/**
 * Source-table options keyed by an authored table pattern.
 */
export type SourceTableConfigMap<TTableConfig extends SourceTableConfig = SourceTableConfig> = Readonly<
  Partial<Record<string, TTableConfig>>
>;

/**
 * Generate the source-table options schema with MongoDB pre-filtering expressions.
 */
export function createSourceTableConfigSchema(): JsonObject {
  return {
    type: 'object',
    propertyNames: {
      minLength: 1,
      pattern: '^(?:[^."]+|"(?:[^"]|"")+")(?:\\.(?:[^."]+|"(?:[^"]|"")+")){0,2}$'
    },
    additionalProperties: {
      type: 'object',
      properties: {
        mongodb_filter_expression: {
          anyOf: [{ $ref: '#/definitions/mongodb_filter_expression' }, { const: 'disabled' }]
        }
      },
      additionalProperties: false
    }
  };
}

/**
 * Parse a source-table key using the same right-to-left qualification as Sync Stream table references.
 */
export function parseSourceTableConfigKey(name: string): ImplicitSchemaTablePattern {
  const parts = parseQualifiedIdentifiers(name);
  if (parts.length > 3) throw new Error(SOURCE_TABLE_NAME_ERROR);

  if (parts.length == 1) return new ImplicitSchemaTablePattern(null, parts[0]);
  if (parts.length == 2) return new TablePattern(parts[0], parts[1], DEFAULT_TAG);
  return new TablePattern(parts[1], parts[2], parts[0]);
}

/**
 * Find authored keys that identify the same pattern after SQL identifier normalization.
 * Invalid keys are left to the source-table schema validator.
 */
export function findDuplicateSourceTableConfigKeys(keys: Iterable<string>): Array<[string, string]> {
  const seen = new Map<string, string>();
  const duplicates: Array<[string, string]> = [];
  for (const key of keys) {
    let pattern: ImplicitSchemaTablePattern;
    try {
      pattern = parseSourceTableConfigKey(key);
    } catch {
      continue;
    }
    const identity = JSON.stringify([pattern.connectionTag, pattern.schema, pattern.tablePattern]);
    const previous = seen.get(identity);
    if (previous !== undefined) {
      duplicates.push([previous, key]);
    } else {
      seen.set(identity, key);
    }
  }
  return duplicates;
}

/**
 * Split a source-table key on unquoted dots and normalize each identifier.
 */
function parseQualifiedIdentifiers(name: string): string[] {
  /*
   * Match <table>, <database>.<table>, or <connection>.<database>.<table> from right to left.
   * Each component is either unquoted without dots or quotes, or quoted to allow dots and escaped quotes within it.
   * A quote inside a quoted component is represented by two consecutive quotes, following SQL identifier syntax.
   */
  const qualifiedIdentifier =
    /^(?:(?:(?<connection>"(?:[^"]|"")+"|[^."]+)\.)?(?<database>"(?:[^"]|"")+"|[^."]+)\.)?(?<table>"(?:[^"]|"")+"|[^."]+)$/;
  const match = qualifiedIdentifier.exec(name);
  if (match == null) throw new Error(SOURCE_TABLE_NAME_ERROR);

  // Nesting the optional groups aligns one-, two-, and three-part names from the right.
  return [match.groups!.connection, match.groups!.database, match.groups!.table]
    .filter((part): part is string => part != null)
    .map((part) => {
      // Quoted names preserve case and may contain dots or escaped quotes. Unquoted names follow SQL case folding.
      return part.startsWith('"') ? part.slice(1, -1).replaceAll('""', '"') : part.toLowerCase();
    });
}

/**
 * Validate and copy source-table options while preserving declaration order.
 */
export function normalizeSourceTableConfig(value: unknown): SourceTableConfigMap {
  if (value === undefined) return {};
  assertObject(value, 'Source table configuration must be a JSON object.');
  const duplicate = findDuplicateSourceTableConfigKeys(Object.keys(value))[0];
  if (duplicate != null) {
    const [previous, key] = duplicate;
    throw new Error(
      `Source-table keys ${JSON.stringify(previous)} and ${JSON.stringify(key)} resolve to the same pattern.`
    );
  }
  return Object.fromEntries(
    Object.entries(value).map(([table, options]) => {
      if (!table) throw new Error('A source table name must not be empty.');
      parseSourceTableConfigKey(table);
      assertObject(options, 'Source table options must be JSON objects.');
      for (const key of Object.keys(options)) {
        if (key !== 'mongodb_filter_expression') throw new Error(`Unknown source-table option: ${key}`);
      }
      const filter = options.mongodb_filter_expression;
      if (filter !== undefined && filter !== 'disabled') {
        const result = MONGO_FILTER_VALIDATOR.safeParse(filter);
        if (!result.success) {
          throw new Error(
            `Invalid MongoDB pre-filtering expression for source table ${JSON.stringify(table)}: ${result.error.message}`
          );
        }
      }
      return [table, structuredClone(options) as SourceTableConfig];
    })
  );
}

/**
 * Conservative equality: declaration order and expression order are significant.
 */
export function sourceTableConfigsEqual(left: SourceTableConfigMap, right: SourceTableConfigMap): boolean {
  try {
    return JSON.stringify(normalizeSourceTableConfig(left)) == JSON.stringify(normalizeSourceTableConfig(right));
  } catch {
    // Saved options may no longer validate after an upgrade. Do not reuse their replication stream.
    return false;
  }
}

function assertObject(value: unknown, message: string): asserts value is JsonObject {
  if (value == null || typeof value != 'object' || Array.isArray(value)) throw new Error(message);
}

/**
 * Configured expressions require execution support even when current collections opt out.
 */
export function hasMongoFilterExpressions(config: SourceTableConfigMap, connectionTag: string): boolean {
  return Object.entries(config).some(
    ([name, options]) =>
      (parseSourceTableConfigKey(name).connectionTag ?? DEFAULT_TAG) === connectionTag &&
      options?.mongodb_filter_expression !== undefined &&
      options.mongodb_filter_expression !== 'disabled'
  );
}
