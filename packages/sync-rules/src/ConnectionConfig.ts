import * as t from 'ts-codec';
import type { JsonObject } from './json.js';

/** Options for one source table, such as `config.connections.default.tables.orders`. This repo has no table options yet. */
export const SourceTableConfig = t.object({});
export type SourceTableConfig = t.Decoded<typeof SourceTableConfig>;

/** Shared connection structure. A source module specializes the table codec to describe its additional options. */
export function connectionConfigCodec<T extends t.AnyCodec>(tableConfig: T) {
  return t.object({ type: t.string, tables: t.record(tableConfig).optional() });
}

export const ConnectionConfig = connectionConfigCodec(SourceTableConfig);
export type ConnectionConfig<TTableConfig extends SourceTableConfig = SourceTableConfig> = {
  readonly type: string;
  readonly tables?: Readonly<Partial<Record<string, TTableConfig>>>;
};

/** Connection tags are map keys; table names and patterns remain relative to their connection's default schema. */
export type ConnectionConfigMap = Readonly<Partial<Record<string, ConnectionConfig>>>;

/** Generate the common shape while making the empty base table option explicit, regardless of generator defaults. */
export function createConnectionConfigSchema(): JsonObject {
  const connection = t.generateJSONSchema(ConnectionConfig, { allowAdditional: false }) as JsonObject;
  const properties = connection.properties as JsonObject;
  properties.type = { type: 'string', minLength: 1 };
  properties.tables = {
    type: 'object',
    propertyNames: { minLength: 1 },
    additionalProperties: {
      type: 'object',
      properties: {},
      additionalProperties: false,
      maxProperties: 0
    }
  };
  // The empty connection spelling means no options. Nonempty connections require their discriminator.
  return {
    type: 'object',
    propertyNames: { minLength: 1 },
    additionalProperties: {
      anyOf: [{ type: 'object', additionalProperties: false, maxProperties: 0 }, connection]
    }
  };
}

/**
 * Copy schema-validated connection options without dropping module-owned fields through the empty base table codec.
 * Sort connection tags only: table declaration order can determine wildcard precedence, and literal object order matters.
 */
export function normalizeConnectionConfig(value: unknown): ConnectionConfigMap {
  if (value === undefined) return {};
  assertJsonObject(value);
  const entries = Object.entries(value).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  return Object.fromEntries(
    entries.flatMap(([tag, config]) => {
      assertJsonObject(config);
      if (!tag) throw new Error('A connection tag must not be empty.');
      if (Object.keys(config).length == 0) return [];
      if (typeof config.type != 'string' || !config.type) throw new Error('A connection type must not be empty.');
      if (config.tables !== undefined) {
        assertJsonObject(config.tables);
        for (const [table, options] of Object.entries(config.tables)) {
          if (!table) throw new Error('A source table name must not be empty.');
          assertJsonObject(options);
        }
      }
      return [[tag, structuredClone(config) as ConnectionConfig]];
    })
  );
}

/** Conservative equality: an expression rewrite may require a new snapshot even if MongoDB evaluates it identically. */
export function connectionConfigsEqual(left: ConnectionConfigMap, right: ConnectionConfigMap): boolean {
  return JSON.stringify(normalizeConnectionConfig(left)) == JSON.stringify(normalizeConnectionConfig(right));
}

function assertJsonObject(value: unknown): asserts value is JsonObject {
  if (value == null || typeof value != 'object' || Array.isArray(value)) {
    throw new Error('Connection and table configuration must be JSON objects.');
  }
  assertJsonValue(value, new Set());
}

function assertJsonValue(value: unknown, parents: Set<object>): void {
  if (value === null || typeof value == 'string' || typeof value == 'boolean') return;
  if (typeof value == 'number' && Number.isFinite(value)) return;
  if (typeof value != 'object' || parents.has(value!)) {
    throw new Error('Connection configuration must contain finite, acyclic JSON values.');
  }
  if (
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) !== Object.prototype &&
    Object.getPrototypeOf(value) !== null
  ) {
    throw new Error('Connection configuration must contain plain JSON objects.');
  }
  parents.add(value!);
  for (const child of Object.values(value!)) assertJsonValue(child, parents);
  parents.delete(value!);
}
