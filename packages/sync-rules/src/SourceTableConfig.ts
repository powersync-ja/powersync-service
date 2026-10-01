import * as t from 'ts-codec';
import type { JsonObject } from './json.js';
import { ImplicitSchemaTablePattern } from './TablePattern.js';

/**
 * Options for one entry in `config.source_tables`. Core has no source-specific options.
 */
export const SOURCE_TABLE_CONFIG = t.object({});
export type SourceTableConfig = t.Decoded<typeof SOURCE_TABLE_CONFIG>;

/**
 * Source-table options keyed by an authored table pattern.
 */
export type SourceTableConfigMap<TTableConfig extends SourceTableConfig = SourceTableConfig> = Readonly<
  Partial<Record<string, TTableConfig>>
>;

/**
 * Generate the common source-table map while making the empty base option explicit.
 */
export function createSourceTableConfigSchema(): JsonObject {
  return {
    type: 'object',
    propertyNames: {
      minLength: 1,
      pattern: '^[^.]+(?:\\.[^.]+){0,2}$',
      patternErrorMessage: 'Use <table>, <database>.<table>, or <connection>.<database>.<table> for source table names.'
    },
    additionalProperties: {
      type: 'object',
      properties: {},
      additionalProperties: false
    }
  };
}

/**
 * Parse a source-table key using the same right-to-left qualification as Sync Stream table references.
 */
export function parseSourceTableConfigKey(name: string): ImplicitSchemaTablePattern {
  const parts = name.split('.');
  if (parts.length > 3 || parts.some((part) => !part)) {
    throw new Error('Source table patterns must use <table>, <database>.<table>, or <connection>.<database>.<table>.');
  }
  const table = parts.pop()!;
  return new ImplicitSchemaTablePattern(parts.length == 0 ? null : parts.join('.'), table);
}

/**
 * Validate and copy portable source-table options while preserving declaration order.
 */
export function normalizeSourceTableConfig(value: unknown): SourceTableConfigMap {
  if (value === undefined) return {};
  assertJsonObject(value, 'Source table configuration must be a JSON object.');
  return Object.fromEntries(
    Object.entries(value).map(([table, options]) => {
      if (!table) throw new Error('A source table name must not be empty.');
      parseSourceTableConfigKey(table);
      assertJsonObject(options, 'Source table options must be JSON objects.');
      return [table, structuredClone(options) as SourceTableConfig];
    })
  );
}

/**
 * Conservative equality: declaration order and expression order are significant.
 */
export function sourceTableConfigsEqual(left: SourceTableConfigMap, right: SourceTableConfigMap): boolean {
  return JSON.stringify(normalizeSourceTableConfig(left)) == JSON.stringify(normalizeSourceTableConfig(right));
}

function assertJsonObject(value: unknown, message: string): asserts value is JsonObject {
  if (value == null || typeof value != 'object' || Array.isArray(value)) throw new Error(message);
  assertJsonValue(value, new Set());
}

function assertJsonValue(value: unknown, parents: Set<object>): void {
  if (value === null || typeof value == 'string' || typeof value == 'boolean') return;
  if (typeof value == 'number' && Number.isFinite(value)) return;
  if (typeof value != 'object' || parents.has(value!)) {
    throw new Error('Source table configuration must contain finite, acyclic JSON values.');
  }
  if (
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) !== Object.prototype &&
    Object.getPrototypeOf(value) !== null
  ) {
    throw new Error('Source table configuration must contain plain JSON objects.');
  }
  parents.add(value!);
  for (const child of Object.values(value!)) assertJsonValue(child, parents);
  parents.delete(value!);
}
