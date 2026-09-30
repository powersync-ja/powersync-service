import * as t from 'ts-codec';
import { describe, expect, test, vi } from 'vitest';
import {
  AdditionalSyncConfigParser,
  deserializeSyncPlan,
  normalizeSourceTableConfig,
  parseSourceTableConfigKey,
  PrecompiledSyncConfig,
  serializeSyncPlan,
  sourceTableConfigsEqual,
  SqlSyncRules,
  SyncRulesErrors
} from '../../src/index.js';
import { compileSyncRulesSchemaValidator, createSyncRulesSchema } from '../../src/json_schema.js';

const STREAMS = /* yaml */ ` streams:
    orders:
      query: SELECT * FROM orders `;

const SAMPLE_OPTION = t.number;

const ADDITIONAL_PARSER: AdditionalSyncConfigParser = {
  id: 'example.tables',
  extendJsonSchema({ schema }) {
    const sourceTables = (schema.properties as any).config.properties.source_tables;
    sourceTables.additionalProperties.properties.sample = t.generateJSONSchema(SAMPLE_OPTION);
    delete sourceTables.additionalProperties.maxProperties;
  },
  parse({ config, context }) {
    const sourceTables = (config as any).config?.source_tables;
    if (sourceTables == null || typeof sourceTables != 'object' || Array.isArray(sourceTables)) return;
    for (const [table, rawOptions] of Object.entries(sourceTables)) {
      if (rawOptions == null || typeof rawOptions != 'object' || Array.isArray(rawOptions)) continue;
      if (!Object.hasOwn(rawOptions, 'sample')) continue;
      const sample = SAMPLE_OPTION.decode((rawOptions as any).sample);
      const { plan } = context.parsedConfig as PrecompiledSyncConfig;
      plan.moduleData = { ...plan.moduleData, [ADDITIONAL_PARSER.id]: null };
      context.parsedConfig.sourceTableConfig = {
        ...context.parsedConfig.sourceTableConfig,
        [table]: { ...context.parsedConfig.sourceTableConfig[table], sample }
      };
      if (sample < 0) {
        context.reportDiagnostic({
          level: 'fatal',
          message: 'Sample must not be negative.',
          location: context.sourceLocations.getLocation(['config', 'source_tables', table, 'sample'])
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

describe('source table configuration', () => {
  test('accepts source_tables alongside other settings and rejects old or misplaced spellings', () => {
    const yaml = /* yaml */ `
      { config: { edition: 3, storage_version: 2, source_tables: { orders: { sample: 2 } } }, streams: {} }
    `;
    const { config } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', parsers: [ADDITIONAL_PARSER] });
    expect(config.storageVersion).toBe(2);
    expect(config.sourceTableConfig).toEqual({ orders: { sample: 2 } });
    expect(() => parse('config: { edition: 3, connections: {} }')).toThrow("Unknown key 'connections'.");
    expect(() => SqlSyncRules.fromYaml(`${withStreams()}source_tables: {}`, { defaultSchema: 'app' })).toThrow(
      "Unknown key 'source_tables'."
    );
  });

  test('does not select additional replication sources', () => {
    const { config } = parse(
      /* yaml */ ` config:
          edition: 3
          source_tables:
            orders: { sample: 1 }
            another: {} `,
      [ADDITIONAL_PARSER]
    );
    expect(config.sourceTableConfig).toEqual({ orders: { sample: 1 } });
    expect(config.getSourceTables().map((table) => table.name)).toEqual(['orders']);
  });

  test.each([
    ['"": {}', 'fewer than 1 characters'],
    ['orders: null', 'must be object'],
    ['orders: []', 'must be object'],
    ['a.b.c.d: {}', 'Use <table>, <database>.<table>'],
    ['a..orders: {}', 'Use <table>, <database>.<table>'],
    ['orders: { unknown: true }', 'must NOT have additional properties'],
    ['orders: { sample: wrong }', 'must be number']
  ])('rejects invalid source-table options: %s', (entry, message) => {
    expect(() => parse(`config: { edition: 3, source_tables: { ${entry} } }`, [ADDITIONAL_PARSER])).toThrow(message);
  });

  test('uses right-to-left table qualification', () => {
    expect(parseSourceTableConfigKey('orders')).toMatchObject({
      connectionTag: null,
      schema: null,
      tablePattern: 'orders'
    });
    expect(parseSourceTableConfigKey('app.orders')).toMatchObject({
      connectionTag: 'default',
      schema: 'app',
      tablePattern: 'orders'
    });
    expect(parseSourceTableConfigKey('archive.app.orders')).toMatchObject({
      connectionTag: 'archive',
      schema: 'app',
      tablePattern: 'orders'
    });
  });

  test('composes fields from independent parsers', () => {
    const flagParser: AdditionalSyncConfigParser = {
      id: 'example.flags',
      extendJsonSchema({ schema }) {
        const options = (schema.properties as any).config.properties.source_tables.additionalProperties;
        options.properties.flag = { type: 'boolean' };
        delete options.maxProperties;
      },
      parse({ config, context }) {
        const sourceTables = (config as any).config?.source_tables ?? {};
        for (const [table, options] of Object.entries(sourceTables) as [string, any][]) {
          if (!Object.hasOwn(options, 'flag')) continue;
          context.parsedConfig.sourceTableConfig = {
            ...context.parsedConfig.sourceTableConfig,
            [table]: { ...context.parsedConfig.sourceTableConfig[table], flag: options.flag }
          };
        }
      }
    };
    const result = parse(
      'config: { edition: 3, source_tables: { orders: { sample: 1, flag: true } } }',
      [ADDITIONAL_PARSER, flagParser]
    );
    expect(result.config.sourceTableConfig).toEqual({ orders: { sample: 1, flag: true } });
  });

  test('preserves declaration order and options through the sync plan', () => {
    const result = parse(
      /* yaml */ ` config:
        edition: 3
        source_tables:
          orders%: { sample: 10 }
          orders: { sample: 1 } `,
      [ADDITIONAL_PARSER]
    );
    const plan = serializeSyncPlan((result.config as PrecompiledSyncConfig).plan);
    expect(plan.version).toBe(3);
    expect(plan.moduleData).toEqual({ 'example.tables': null });
    expect(plan.sourceTableConfig).toEqual(result.config.sourceTableConfig);
    expect(deserializeSyncPlan(plan).sourceTableConfig).toEqual(result.config.sourceTableConfig);
    expect(Object.keys(plan.sourceTableConfig!)).toEqual(['orders%', 'orders']);
  });

  test('reports hook errors at the authored option value', () => {
    const body = /* yaml */ ` config:
      edition: 3
      source_tables:
        orders: { sample: -1 } `;
    try {
      parse(body, [ADDITIONAL_PARSER]);
      expect.fail('Expected validation to fail');
    } catch (error) {
      expect(error).toBeInstanceOf(SyncRulesErrors);
      const diagnostic = (error as SyncRulesErrors).errors.find(
        (candidate) => candidate.message == 'Sample must not be negative.'
      )!;
      expect(withStreams(body).slice(diagnostic.location.start, diagnostic.location.end).trim()).toBe('-1');
    }
  });

  test('runs hooks without source-table configuration', () => {
    const parser: AdditionalSyncConfigParser = {
      id: 'example.context',
      parse: vi.fn(({ context }) => {
        expect(context.defaultSchema).toBe('app');
        expect(context.sourceTables[0].schema).toBe('app');
        expect(context.parsedConfig.sourceTableConfig).toEqual({});
      })
    };
    parse('config: { edition: 3 }', [parser]);
    expect(parser.parse).toHaveBeenCalledOnce();
  });

  test('requires edition 3 and plan version 3 for configured tables', () => {
    expect(() =>
      SqlSyncRules.fromYaml(/* yaml */ `{ config: { edition: 2, source_tables: {} }, streams: {} }`, {
        defaultSchema: 'app'
      })
    ).toThrow('edition 3');
    const config = SqlSyncRules.fromYaml(withStreams(), { defaultSchema: 'app' }).config as PrecompiledSyncConfig;
    const plan = serializeSyncPlan(config.plan);
    expect(plan.version).toBe(1);
    expect(plan).not.toHaveProperty('sourceTableConfig');
    expect(() => deserializeSyncPlan({ ...plan, sourceTableConfig: { orders: { sample: 1 } } })).toThrow(
      'version 3'
    );
  });

  test('normalizes portable values without reordering table declarations', () => {
    const config = { 'orders%': { sample: 1 }, orders: { sample: 2 } };
    expect(sourceTableConfigsEqual(config, { ...config })).toBe(true);
    expect(sourceTableConfigsEqual(config, { orders: config.orders, 'orders%': config['orders%'] })).toBe(false);
    expect(() => normalizeSourceTableConfig({ orders: { date: new Date() } })).toThrow('plain JSON');
    expect(() => normalizeSourceTableConfig({ orders: { amount: Number.NaN } })).toThrow('finite');
  });

  test('keeps the generated base table schema closed', () => {
    const schema = createSyncRulesSchema();
    const options = (schema.properties as any).config.properties.source_tables.additionalProperties;
    expect(options).toMatchObject({ additionalProperties: false, maxProperties: 0 });
    const validate = compileSyncRulesSchemaValidator(schema);
    expect(validate({ config: { edition: 3, source_tables: { orders: {} } }, streams: {} })).toBe(true);
    expect(validate({ config: { edition: 3, source_tables: { orders: { sample: 1 } } }, streams: {} })).toBe(false);
  });
});
