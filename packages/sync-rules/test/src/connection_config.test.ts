import * as t from 'ts-codec';
import { describe, expect, test, vi } from 'vitest';
import {
  AdditionalSyncConfigParser,
  ConnectionConfig,
  connectionConfigCodec,
  connectionConfigsEqual,
  deserializeSyncPlan,
  normalizeConnectionConfig,
  PrecompiledSyncConfig,
  serializeSyncPlan,
  SOURCE_TABLE_CONFIG,
  SqlSyncRules,
  SyncRulesErrors
} from '../../src/index.js';
import { compileSyncRulesSchemaValidator, createSyncRulesSchema } from '../../src/json_schema.js';

const STREAMS = /* yaml */ ` streams:
    orders:
      query: SELECT * FROM orders `;

// A fixture module extends the shared table structure. No source-specific syntax is built into the base codec.
const TABLE_OPTIONS = t.object({ sample: t.number.optional() });
type TableOptions = t.Decoded<typeof TABLE_OPTIONS>;
const EXTENDED_CONNECTION = connectionConfigCodec(TABLE_OPTIONS);

const ADDITIONAL_PARSER: AdditionalSyncConfigParser = {
  id: 'example.tables',
  extendJsonSchema({ schema }) {
    const map = (schema.properties as any).config.properties.connections;
    map.additionalProperties = {
      if: { type: 'object', required: ['type'], properties: { type: { const: 'example' } } },
      then: t.generateJSONSchema(EXTENDED_CONNECTION, { allowAdditional: false }),
      else: map.additionalProperties
    };
  },
  parse({ config, context }) {
    for (const [tag, options] of Object.entries((config as any).config?.connections ?? {})) {
      if ((options as any).type != 'example') continue;
      const { plan } = context.parsedConfig as PrecompiledSyncConfig;
      plan.moduleData = { ...plan.moduleData, ['example.tables']: null };
      const connection: ConnectionConfig<TableOptions> = EXTENDED_CONNECTION.decode(options as never);
      context.parsedConfig.connectionConfig = { ...context.parsedConfig.connectionConfig, [tag]: connection };
      for (const [table, options] of Object.entries(connection.tables ?? {})) {
        if ((options?.sample ?? 0) < 0)
          context.reportDiagnostic({
            level: 'fatal',
            message: 'Sample must not be negative.',
            location: context.sourceLocations.getLocation(['config', 'connections', tag, 'tables', table, 'sample'])
          });
      }
    }
  }
};

function withStreams(config = 'config: { edition: 3 }'): string {
  return `${config.trim()}\n${STREAMS.trim()}\n`;
}

function parse(config: string, parsers: AdditionalSyncConfigParser[] = []) {
  return SqlSyncRules.fromYaml(withStreams(config), { defaultSchema: 'app', parsers });
}

describe('connection configuration', () => {
  test('accepts connections alongside other config settings and rejects unknown config keys', () => {
    const yaml = /* yaml */ `
      { config: { edition: 3, storage_version: 2, connections: { default: { type: example } } }, streams: {} }
    `;
    const { config } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', parsers: [ADDITIONAL_PARSER] });
    expect(config.storageVersion).toBe(2);
    expect(config.connectionConfig).toEqual({ default: { type: 'example' } });
    expect(() =>
      SqlSyncRules.fromYaml(yaml.replace('storage_version', 'unknown_setting'), {
        defaultSchema: 'app',
        parsers: [ADDITIONAL_PARSER]
      })
    ).toThrow("Unknown key 'unknown_setting'.");
  });

  test.each(['connection_config', 'connections'])('rejects the root-level %s spelling', (key) => {
    expect(() =>
      SqlSyncRules.fromYaml(`${withStreams()}${key}: { default: { type: example } }`, {
        defaultSchema: 'app'
      })
    ).toThrow(`Unknown key '${key}'.`);
  });

  test('declares base connection/table structure without selecting more source tables', () => {
    const { config } = parse(
      /* yaml */ ` config:
          edition: 3
          connections:
            default:
              type: example
              tables:
                orders: {}
                another: {} `,
      [ADDITIONAL_PARSER]
    );
    expect(config.connectionConfig).toEqual({ default: { type: 'example', tables: { orders: {}, another: {} } } });
    expect(config.getSourceTables().map((table) => table.name)).toEqual(['orders']);
    expect(
      parse(/* yaml */ ` config:
          edition: 3
          connections:
            unused: {} `).config.connectionConfig
    ).toEqual({});
    expect(SqlSyncRules.fromYaml(withStreams(), { defaultSchema: 'app' }).config.connectionConfig).toEqual({});
  });

  test.each([
    ['  "": {}', 'must NOT have fewer than 1 characters'],
    ['  default: null', 'must be object'],
    ['  default: []', 'must be object'],
    ['  default: { type: "" }', 'must NOT have fewer than 1 characters'],
    ['  default: { tables: {} }', "must have required property 'type'"],
    ['  default: { type: example, unknown: true }', 'must NOT have additional properties'],
    ['  default: { type: example, tables: { "": {} } }', 'must NOT have fewer than 1 characters'],
    ['  default: { type: example, tables: { orders: null } }', 'must be object'],
    ['  default: { type: example, tables: { orders: { filter: {} } } }', 'must NOT have additional properties']
  ])('rejects invalid/unsupported options: %s', (connection, message) => {
    expect(() => parse(`config: { edition: 3, connections: { ${connection} } }`)).toThrow(message);
  });

  test('keeps the generated base table schema empty even when additional properties would be permitted', () => {
    // Some generators allow extra properties by default. The explicit cardinality constraint must survive that.
    const schema = createSyncRulesSchema();
    const entry = (schema.properties as any).config.properties.connections.additionalProperties.anyOf[1];
    expect(entry.properties.tables.additionalProperties).toMatchObject({
      additionalProperties: false,
      maxProperties: 0
    });
    entry.properties.tables.additionalProperties.additionalProperties = true;
    const validate = compileSyncRulesSchemaValidator(schema);
    const config = {
      config: { edition: 3, connections: { default: { type: 'example', tables: { orders: {} } } } },
      streams: {}
    };
    expect(validate(config)).toBe(true);
    (config.config.connections.default.tables.orders as any).filter = {};
    expect(validate(config)).toBe(false);
    expect(t.generateJSONSchema(SOURCE_TABLE_CONFIG)).toHaveProperty('type', 'object');
  });

  test('specializes tables only for the module type and preserves values through a saved-plan round trip', () => {
    const yaml = /* yaml */ ` config:
        edition: 3
        connections:
          default:
            type: example
            tables:
              orders%: { sample: 10 }
              orders: {} `;
    const result = parse(yaml, [ADDITIONAL_PARSER]);
    const plan = serializeSyncPlan((result.config as PrecompiledSyncConfig).plan);
    expect(plan.version).toBe(3);
    expect(plan.moduleData).toEqual({ 'example.tables': null });
    expect(plan.connectionConfig).toEqual(result.config.connectionConfig);
    expect(deserializeSyncPlan(plan).connectionConfig).toEqual(result.config.connectionConfig);
    expect(Object.keys(plan.connectionConfig!.default!.tables!)).toEqual(['orders%', 'orders']);
    expect(() => parse(yaml)).toThrow('must NOT have additional properties');
    expect(() => parse(yaml.replace('type: example', 'type: other'), [ADDITIONAL_PARSER])).toThrow(
      'must NOT have additional properties'
    );
    expect(() => parse(yaml.replace('sample: 10', 'unknown: 10'), [ADDITIONAL_PARSER])).toThrow(
      'must NOT have additional properties'
    );
  });

  test('reports schema and hook errors at the authored value, including escaped JSON-pointer segments', () => {
    const body = /* yaml */ ` config:
        edition: 3
        connections:
          default:
            type: example
            tables:
              'orders/a~b': { sample: -1 } `;
    try {
      parse(body, [ADDITIONAL_PARSER]);
      expect.fail('Expected validation to fail');
    } catch (error) {
      expect(error).toBeInstanceOf(SyncRulesErrors);
      const diagnostic = (error as SyncRulesErrors).errors.find(
        (error) => error.message == 'Sample must not be negative.'
      )!;
      expect(withStreams(body).slice(diagnostic.location.start, diagnostic.location.end).trim()).toBe('-1');
    }
    const invalid = body.replace('sample: -1', 'unknown: true');
    try {
      parse(invalid, [ADDITIONAL_PARSER]);
      expect.fail('Expected validation to fail');
    } catch (error) {
      const diagnostic = (error as SyncRulesErrors).errors.find((error) =>
        error.message.includes('additional properties')
      )!;
      expect(withStreams(invalid).slice(diagnostic.location.start, diagnostic.location.end).trim()).toBe('unknown');
    }
  });

  test('runs generic hooks in order with context even when connection configuration is absent', () => {
    const seen: string[] = [];
    const first: AdditionalSyncConfigParser = {
      id: 'example.core-validation',
      parse({ config, context }) {
        expect(config).toHaveProperty('streams.orders');
        expect(context.defaultSchema).toBe('app');
        expect(context.sourceTables[0].schema).toBe('app');
        expect(context.parsedConfig.connectionConfig).toEqual({});
        seen.push('first');
        context.reportDiagnostic({ level: 'warning', message: 'A generic warning.' });
      }
    };
    const second = {
      id: 'example.other',
      parse: vi.fn(() => {
        seen.push('second');
      })
    };
    const result = SqlSyncRules.fromYaml(withStreams(), { defaultSchema: 'app', parsers: [first, second] });
    expect(seen).toEqual(['first', 'second']);
    expect(result.errors[0]).toMatchObject({ type: 'warning', message: 'A generic warning.' });
  });

  test('rejects additional root fields even when a module extends the schema', () => {
    const parser: AdditionalSyncConfigParser = {
      id: 'example.root',
      extendJsonSchema({ schema }) {
        (schema.properties as any).extra = { type: 'boolean' };
      },
      parse: vi.fn()
    };
    expect(() =>
      SqlSyncRules.fromYaml(`${withStreams()}extra: true`, { defaultSchema: 'app', parsers: [parser] })
    ).toThrow("Unknown key 'extra'.");
    expect(() =>
      SqlSyncRules.fromYaml(`${withStreams()}unknown: true`, { defaultSchema: 'app', parsers: [parser] })
    ).toThrow("Unknown key 'unknown'.");
    expect(() => SqlSyncRules.fromYaml(`${withStreams()}extra: true`, { defaultSchema: 'app' })).toThrow(
      "Unknown key 'extra'."
    );
  });

  test.each(['schema', 'hook'])('never exposes a partial config on %s failure in diagnostic mode', (kind) => {
    const parser = { ...ADDITIONAL_PARSER };
    if (kind == 'hook')
      parser.parse = ({ context }) => context.reportDiagnostic({ level: 'fatal', message: 'Rejected.' });
    expect(() =>
      SqlSyncRules.fromYaml(
        withStreams(
          `config: { edition: 3, connections: { default: { type: example, tables: { orders: { sample: ${kind == 'schema' ? 'wrong' : '1'} } } } } }`
        ),
        {
          defaultSchema: 'app',
          throwOnError: false,
          parsers: [parser]
        }
      )
    ).toThrow(kind == 'schema' ? 'must be number' : 'Rejected.');
  });

  test('preserves legacy plan formats and rejects unknown or incorrectly labelled configured plans', () => {
    const config = SqlSyncRules.fromYaml(withStreams(), { defaultSchema: 'app' }).config as PrecompiledSyncConfig;
    const plan = serializeSyncPlan(config.plan);
    expect(plan.version).toBe(1);
    expect(plan).not.toHaveProperty('connectionConfig');
    expect(() => deserializeSyncPlan({ ...plan, version: 0 })).toThrow(
      'Unknown sync plan version passed to deserializeSyncPlan()'
    );
    expect(() => deserializeSyncPlan({ ...plan, version: 4 })).toThrow(
      'Encountered a sync plan with version 4, the maximum supported version is 3.'
    );
    expect(() => deserializeSyncPlan({ ...plan, connectionConfig: { default: { type: 'example' } } })).toThrow(
      'version 3'
    );
    expect(
      () =>
        parse(/* yaml */ ` config:
            edition: 3
            connections:
              default: {} `).config
    ).not.toThrow();
    expect(() =>
      SqlSyncRules.fromYaml(/* yaml */ `{ config: { edition: 2, connections: {} }, streams: {} }`, {
        defaultSchema: 'app'
      })
    ).toThrow('edition 3');
  });

  test('normalizes only connection-tag order and rejects nonportable runtime values', () => {
    const a = { z: { type: 'example' }, a: { type: 'example', tables: { 'orders%': {}, orders: {} } } };
    expect(connectionConfigsEqual(a, { a: a.a, z: a.z })).toBe(true);
    expect(connectionConfigsEqual(a, { ...a, a: { ...a.a, tables: { orders: {}, 'orders%': {} } } })).toBe(false);
    expect(() =>
      normalizeConnectionConfig({ default: { type: 'example', tables: { orders: { date: new Date() } } } })
    ).toThrow('plain JSON');
    expect(() =>
      normalizeConnectionConfig({ default: { type: 'example', tables: { orders: { amount: NaN } } } })
    ).toThrow('finite');
  });
});
