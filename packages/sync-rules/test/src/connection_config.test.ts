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
  SourceTableConfig,
  SqlSyncRules,
  SyncRulesErrors
} from '../../src/index.js';
import { compileSyncRulesSchemaValidator, createSyncRulesSchema } from '../../src/json_schema.js';

const streams = `config:\n  edition: 3\nstreams:\n  orders:\n    query: SELECT * FROM orders\n`;

// A fixture module extends the shared table structure. No source-specific syntax is built into the base codec.
const TableOptions = t.object({ sample: t.number.optional() });
type TableOptions = t.Decoded<typeof TableOptions>;
const ExtendedConnection = connectionConfigCodec(TableOptions);

function additionalParser(): AdditionalSyncConfigParser {
  return {
    id: 'example.tables',
    extendJsonSchema({ schema }) {
      const map = (schema.properties as any).config.properties.connections;
      map.additionalProperties = {
        if: { type: 'object', required: ['type'], properties: { type: { const: 'example' } } },
        then: t.generateJSONSchema(ExtendedConnection, { allowAdditional: false }),
        else: map.additionalProperties
      };
    },
    parse({ config, context }) {
      for (const [tag, options] of Object.entries((config as any).config?.connections ?? {})) {
        if ((options as any).type != 'example') continue;
        const connection: ConnectionConfig<TableOptions> = ExtendedConnection.decode(options as never);
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
}

function configYaml(connection: string): string {
  return streams.replace('streams:', `  connections:\n${connection.trimEnd().replace(/^/gm, '  ')}\nstreams:`);
}

function parse(connection: string, parsers: AdditionalSyncConfigParser[] = []) {
  return SqlSyncRules.fromYaml(configYaml(connection), { defaultSchema: 'app', parsers });
}

describe('connection configuration', () => {
  test('accepts connections alongside other config settings and rejects unknown config keys', () => {
    const yaml = 'config: { edition: 3, storage_version: 2, connections: { default: { type: example } } }\nstreams: {}';
    const { config } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' });
    expect(config.storageVersion).toBe(2);
    expect(config.connectionConfig).toEqual({ default: { type: 'example' } });
    expect(() =>
      SqlSyncRules.fromYaml(yaml.replace('storage_version', 'unknown_setting'), {
        defaultSchema: 'app',
        parsers: [additionalParser()]
      })
    ).toThrow();
  });

  test.each(['connection_config', 'connections'])('rejects the root-level %s spelling', (key) => {
    expect(() =>
      SqlSyncRules.fromYaml(`${streams}${key}: { default: { type: example } }`, {
        defaultSchema: 'app'
      })
    ).toThrow();
  });

  test('declares base connection/table structure without selecting more source tables', () => {
    const { config } = parse('  default:\n    type: example\n    tables:\n      orders: {}\n      another: {}\n');
    expect(config.connectionConfig).toEqual({ default: { type: 'example', tables: { orders: {}, another: {} } } });
    expect(config.getSourceTables().map((table) => table.name)).toEqual(['orders']);
    expect(parse('  unused: {}\n').config.connectionConfig).toEqual({});
    expect(SqlSyncRules.fromYaml(streams, { defaultSchema: 'app' }).config.connectionConfig).toEqual({});
  });

  test.each([
    '  "": {}',
    '  default: null',
    '  default: []',
    '  default: { type: "" }',
    '  default: { tables: {} }',
    '  default: { type: example, unknown: true }',
    '  default: { type: example, tables: { "": {} } }',
    '  default: { type: example, tables: { orders: null } }',
    '  default: { type: example, tables: { orders: { filter: {} } } }'
  ])('rejects invalid/unsupported options: %s', (connection) => {
    expect(() => parse(connection)).toThrow(SyncRulesErrors);
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
    expect(t.generateJSONSchema(SourceTableConfig)).toHaveProperty('type', 'object');
  });

  test('specializes tables only for the module type and preserves values through a saved-plan round trip', () => {
    const yaml = '  default:\n    type: example\n    tables:\n      orders%: { sample: 10 }\n      orders: {}\n';
    const result = parse(yaml, [additionalParser()]);
    const plan = serializeSyncPlan((result.config as PrecompiledSyncConfig).plan);
    expect(plan.version).toBe(4);
    expect(plan.connectionConfig).toEqual(result.config.connectionConfig);
    expect(deserializeSyncPlan(plan).connectionConfig).toEqual(result.config.connectionConfig);
    expect(Object.keys(plan.connectionConfig!.default!.tables!)).toEqual(['orders%', 'orders']);
    expect(() => parse(yaml)).toThrow();
    expect(() => parse(yaml.replace('type: example', 'type: other'), [additionalParser()])).toThrow();
    expect(() => parse(yaml.replace('sample: 10', 'unknown: 10'), [additionalParser()])).toThrow();
  });

  test('reports schema and hook errors at the authored value, including escaped JSON-pointer segments', () => {
    const body = '  default:\n    type: example\n    tables:\n      "orders/a~b": { sample: -1 }\n';
    try {
      parse(body, [additionalParser()]);
      expect.fail('Expected validation to fail');
    } catch (error) {
      expect(error).toBeInstanceOf(SyncRulesErrors);
      const diagnostic = (error as SyncRulesErrors).errors.find(
        (error) => error.message == 'Sample must not be negative.'
      )!;
      expect(configYaml(body).slice(diagnostic.location.start, diagnostic.location.end).trim()).toBe('-1');
    }
    const invalid = body.replace('sample: -1', 'unknown: true');
    try {
      parse(invalid, [additionalParser()]);
      expect.fail('Expected validation to fail');
    } catch (error) {
      const diagnostic = (error as SyncRulesErrors).errors.find((error) =>
        error.message.includes('additional properties')
      )!;
      expect(configYaml(invalid).slice(diagnostic.location.start, diagnostic.location.end).trim()).toBe('unknown');
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
    const result = SqlSyncRules.fromYaml(streams, { defaultSchema: 'app', parsers: [first, second] });
    expect(seen).toEqual(['first', 'second']);
    expect(result.errors[0]).toMatchObject({ type: 'warning', message: 'A generic warning.' });
  });

  test('can extend unrelated root fields without accepting undeclared ones', () => {
    const parser: AdditionalSyncConfigParser = {
      id: 'example.root',
      extendJsonSchema({ schema }) {
        (schema.properties as any).extra = { type: 'boolean' };
      },
      parse: vi.fn()
    };
    expect(SqlSyncRules.fromYaml(`${streams}extra: true`, { defaultSchema: 'app', parsers: [parser] }).errors).toEqual(
      []
    );
    expect(() =>
      SqlSyncRules.fromYaml(`${streams}unknown: true`, { defaultSchema: 'app', parsers: [parser] })
    ).toThrow();
    expect(() => SqlSyncRules.fromYaml(`${streams}extra: true`, { defaultSchema: 'app' })).toThrow();
  });

  test.each(['schema', 'hook'])('never exposes a partial config on %s failure in diagnostic mode', (kind) => {
    const parser = additionalParser();
    if (kind == 'hook')
      parser.parse = ({ context }) => context.reportDiagnostic({ level: 'fatal', message: 'Rejected.' });
    expect(() =>
      SqlSyncRules.fromYaml(
        configYaml(`  default: { type: example, tables: { orders: { sample: ${kind == 'schema' ? 'wrong' : '1'} } } }`),
        {
          defaultSchema: 'app',
          throwOnError: false,
          parsers: [parser]
        }
      )
    ).toThrow(SyncRulesErrors);
  });

  test('preserves legacy plan formats and rejects unknown or incorrectly labelled configured plans', () => {
    const config = SqlSyncRules.fromYaml(streams, { defaultSchema: 'app' }).config as PrecompiledSyncConfig;
    const plan = serializeSyncPlan(config.plan);
    expect(plan.version).toBe(1);
    expect(plan).not.toHaveProperty('connectionConfig');
    expect(() => deserializeSyncPlan({ ...plan, version: 3 })).toThrow();
    expect(() => deserializeSyncPlan({ ...plan, version: 5 })).toThrow();
    expect(() => deserializeSyncPlan({ ...plan, connectionConfig: { default: { type: 'example' } } })).toThrow(
      'version 4'
    );
    expect(() => parse('  default: {}\n').config).not.toThrow();
    expect(() =>
      SqlSyncRules.fromYaml('config: { edition: 2, connections: {} }\nstreams: {}', { defaultSchema: 'app' })
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
