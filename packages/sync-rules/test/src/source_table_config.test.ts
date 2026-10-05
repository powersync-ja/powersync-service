import * as t from 'ts-codec';
import { describe, expect, test, vi } from 'vitest';
import {
  AdditionalSyncConfigParser,
  deserializeSyncPlan,
  ImplicitSchemaTablePattern,
  normalizeSourceTableConfig,
  parseSourceTableConfigKey,
  PrecompiledSyncConfig,
  serializeSyncPlan,
  sourceTableConfigsEqual,
  SqlSyncRules,
  SyncRulesErrors,
  TablePattern
} from '../../src/index.js';
import { compileSyncRulesSchemaValidator, createSyncRulesSchema } from '../../src/json_schema.js';

const STREAMS = /* yaml */ ` streams:
    orders:
      query: SELECT * FROM orders `;

const SAMPLE_OPTION = t.number;

const ADDITIONAL_PARSER: AdditionalSyncConfigParser = {
  id: 'example.tables',
  extendJsonSchema({ schema }) {
    const sourceTables = (schema.properties as any).config.properties.source_table_options;
    sourceTables.additionalProperties.properties.sample = t.generateJSONSchema(SAMPLE_OPTION);
  },
  parse({ config, context }) {
    const sourceTables = (config as any).config?.source_table_options;
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
          location: context.sourceLocations.getLocation(['config', 'source_table_options', table, 'sample'])
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
  test('accepts source_table_options alongside other settings and rejects old or misplaced spellings', () => {
    const yaml = /* yaml */ `
      { config: { edition: 3, storage_version: 2, source_table_options: { orders: { sample: 2 } } }, streams: {} }
    `;
    const { config } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', parsers: [ADDITIONAL_PARSER] });
    expect(config.storageVersion).toBe(2);
    expect(config.sourceTableConfig).toEqual({ orders: { sample: 2 } });
    expect(() => parse('config: { edition: 3, connections: {} }')).toThrow("Unknown key 'connections'.");
    expect(() => SqlSyncRules.fromYaml(`${withStreams()}source_table_options: {}`, { defaultSchema: 'app' })).toThrow(
      "Unknown key 'source_table_options'."
    );
  });

  test('does not select additional replication sources', () => {
    const { config } = parse(
      /* yaml */ ` config:
          edition: 3
          source_table_options:
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
    expect(() => parse(`config: { edition: 3, source_table_options: { ${entry} } }`, [ADDITIONAL_PARSER])).toThrow(
      message
    );
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
    expect(parseSourceTableConfigKey('Archive.App.Orders')).toMatchObject({
      connectionTag: 'archive',
      schema: 'app',
      tablePattern: 'orders'
    });
    expect(parseSourceTableConfigKey('%')).toMatchObject({
      connectionTag: null,
      schema: null,
      tablePattern: '%',
      isWildcard: true
    });
    expect(parseSourceTableConfigKey('1')).toMatchObject({ tablePattern: '1' });
    expect(parseSourceTableConfigKey('Order-Archive')).toMatchObject({ tablePattern: 'order-archive' });
    expect(new TablePattern('app.v2', 'orders', 'default')).toMatchObject({
      connectionTag: 'default',
      schema: 'app.v2',
      tablePattern: 'orders'
    });
    expect(new TablePattern('app', 'orders', 'archive')).toMatchObject({
      connectionTag: 'archive',
      schema: 'app',
      tablePattern: 'orders'
    });
    expect(new ImplicitSchemaTablePattern('archive.app', 'orders', null)).toMatchObject({
      connectionTag: 'archive',
      schema: 'app',
      tablePattern: 'orders'
    });
  });

  test.each([
    ['Users', 'users'],
    ['Users', '"users"'],
    ['Archive.App.Users', 'archive.app.users'],
    ['archive."app.v2".Users', 'archive."app.v2"."users"'],
    ['Users%', 'users%']
  ])('rejects source-table keys %s and %s that resolve to the same pattern', (first, second) => {
    const yaml = withStreams(`config: { edition: 3, source_table_options: { '${first}': {}, '${second}': {} } }`);
    const hook = { id: 'example.noop', parse: vi.fn() };
    expect(() => SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', parsers: [hook] })).toThrow(
      `Source-table keys ${JSON.stringify(first)} and ${JSON.stringify(second)} resolve to the same pattern.`
    );
    expect(hook.parse).not.toHaveBeenCalled();
    const { errors } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', throwOnError: false });
    expect(errors).toHaveLength(1);
    expect(errors[0].type).toBe('fatal');
    expect(yaml.slice(errors[0].location.start, errors[0].location.end)).toBe(`'${second}'`);
  });

  test.each([
    ['"Users"', 'users'],
    ['archive.app.users', 'other.app.users'],
    ['app.users', 'other.users'],
    ['"app.users"', 'app.users'],
    ['users%', 'users']
  ])('accepts distinct source-table patterns %s and %s', (first, second) => {
    const yaml = withStreams(`config: { edition: 3, source_table_options: { '${first}': {}, '${second}': {} } }`);
    expect(SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' }).errors).toEqual([]);
  });

  test('retains conservative equality of authored key spellings', () => {
    expect(sourceTableConfigsEqual({ Users: { sample: 1 } }, { users: { sample: 1 } })).toBe(false);
  });

  test('uses Sync Stream quoting for qualified identifiers', () => {
    expect(parseSourceTableConfigKey('"Orders"')).toMatchObject({
      connectionTag: null,
      schema: null,
      tablePattern: 'Orders'
    });
    expect(parseSourceTableConfigKey('"fs.files"')).toMatchObject({
      connectionTag: null,
      schema: null,
      tablePattern: 'fs.files'
    });
    const qualified = parseSourceTableConfigKey('archive.app."audit.events"');
    expect(qualified).toMatchObject({
      connectionTag: 'archive',
      schema: 'app',
      tablePattern: 'audit.events'
    });
    expect(parseSourceTableConfigKey('"Archive.Prod".app.orders')).toMatchObject({
      connectionTag: 'Archive.Prod',
      schema: 'app',
      tablePattern: 'orders'
    });
    expect(parseSourceTableConfigKey('archive."App.V2".orders')).toMatchObject({
      connectionTag: 'archive',
      schema: 'App.V2',
      tablePattern: 'orders'
    });
    expect(parseSourceTableConfigKey('conn.test."na""me"')).toMatchObject({
      connectionTag: 'conn',
      schema: 'test',
      tablePattern: 'na"me'
    });
    expect(() => parseSourceTableConfigKey('"unterminated')).toThrow('Double-quote names containing dots');
    expect(() => parseSourceTableConfigKey('""')).toThrow('Double-quote names containing dots');
    expect(() => parseSourceTableConfigKey('"say"hello"')).toThrow('Double-quote names containing dots');
    expect(() => parseSourceTableConfigKey('conn.test."na""."me"')).toThrow('Double-quote names containing dots');
  });

  test.each([
    ['"other.app"', 'other', 'app'],
    ['"app.v2"', 'app', 'v2']
  ])('resolves %s consistently before and after serialization', (schema, expectedConnection, expectedSchema) => {
    const yaml = /* yaml */ `config:
  edition: 3
streams:
  orders:
    query: SELECT * FROM SCHEMA.orders
`.replace('SCHEMA', schema);
    const fresh = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'public' }).config as PrecompiledSyncConfig;
    const restored = deserializeSyncPlan(serializeSyncPlan(fresh.plan));
    const freshPattern = fresh.plan.dataSources[0].sourceTable.toTablePattern(fresh.defaultSchema);
    const restoredPattern = restored.dataSources[0].sourceTable.toTablePattern(fresh.defaultSchema);

    expect(fresh.plan.dataSources[0].sourceTable.connectionTag).toBe(expectedConnection);
    expect(restored.dataSources[0].sourceTable.connectionTag).toBe(expectedConnection);
    expect(freshPattern).toMatchObject({
      connectionTag: 'default',
      schema: expectedSchema,
      tablePattern: 'orders'
    });
    expect(restoredPattern).toMatchObject({
      connectionTag: 'default',
      schema: expectedSchema,
      tablePattern: 'orders'
    });
  });

  test.each([
    ['orders', 'default', 'public'],
    ['app.orders', 'default', 'app'],
    ['archive.app.orders', 'default', 'app'],
    ['"App.V2".orders', 'default', 'App.V2'],
    ['archive."App.V2".orders', 'default', 'App.V2'],
    ['"Archive.Prod"."App.V2.Archive".orders', 'default', 'App.V2.Archive']
  ])('preserves schemas and existing connection-tag behavior when resolving %s', (key, connectionTag, schema) => {
    expect(parseSourceTableConfigKey(key).toTablePattern('public')).toMatchObject({
      connectionTag,
      schema,
      tablePattern: 'orders'
    });
  });

  test('retains qualified runtime defaults for unqualified source-table keys', () => {
    expect(parseSourceTableConfigKey('orders').toTablePattern('archive.app')).toMatchObject({
      connectionTag: 'archive',
      schema: 'app',
      tablePattern: 'orders'
    });
  });

  test('accepts quoted source-table keys in authored config', () => {
    const { config } = parse(
      /* yaml */ ` config:
          edition: 3
          source_table_options:
            '"audit.events"': { sample: 1 } `,
      [ADDITIONAL_PARSER]
    );
    expect(config.sourceTableConfig).toEqual({ '"audit.events"': { sample: 1 } });

    const escapedQuote = parse(
      /* yaml */ `config:
          edition: 3
          source_table_options:
            'conn.test."na""me"': { sample: 1 } `,
      [ADDITIONAL_PARSER]
    );
    expect(escapedQuote.config.sourceTableConfig).toEqual({ 'conn.test."na""me"': { sample: 1 } });
  });

  test('composes fields from independent parsers', () => {
    const flagParser: AdditionalSyncConfigParser = {
      id: 'example.flags',
      extendJsonSchema({ schema }) {
        const options = (schema.properties as any).config.properties.source_table_options.additionalProperties;
        options.properties.flag = { type: 'boolean' };
      },
      parse({ config, context }) {
        const sourceTables = (config as any).config?.source_table_options ?? {};
        for (const [table, options] of Object.entries(sourceTables) as [string, any][]) {
          if (!Object.hasOwn(options, 'flag')) continue;
          context.parsedConfig.sourceTableConfig = {
            ...context.parsedConfig.sourceTableConfig,
            [table]: { ...context.parsedConfig.sourceTableConfig[table], flag: options.flag }
          };
        }
      }
    };
    const result = parse('config: { edition: 3, source_table_options: { orders: { sample: 1, flag: true } } }', [
      ADDITIONAL_PARSER,
      flagParser
    ]);
    expect(result.config.sourceTableConfig).toEqual({ orders: { sample: 1, flag: true } });
  });

  test('preserves declaration order and options through the sync plan', () => {
    const result = parse(
      /* yaml */ ` config:
          edition: 3
          source_table_options:
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
        source_table_options:
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
      SqlSyncRules.fromYaml(/* yaml */ `{ config: { edition: 2, source_table_options: {} }, streams: {} }`, {
        defaultSchema: 'app'
      })
    ).toThrow('edition 3');
    const config = SqlSyncRules.fromYaml(withStreams(), { defaultSchema: 'app' }).config as PrecompiledSyncConfig;
    const plan = serializeSyncPlan(config.plan);
    expect(plan.version).toBe(1);
    expect(plan).not.toHaveProperty('sourceTableConfig');
    expect(() => deserializeSyncPlan({ ...plan, sourceTableConfig: { orders: { sample: 1 } } })).toThrow('version 3');
  });

  test('normalizes portable values without reordering table declarations', () => {
    const config = { 'orders%': { sample: 1 }, orders: { sample: 2 } };
    expect(sourceTableConfigsEqual(config, { ...config })).toBe(true);
    expect(sourceTableConfigsEqual(config, { orders: config.orders, 'orders%': config['orders%'] })).toBe(false);
    expect(() => normalizeSourceTableConfig({ orders: { date: new Date() } })).toThrow('plain JSON');
    expect(() => normalizeSourceTableConfig({ orders: { amount: Number.NaN } })).toThrow('finite');
  });

  test.each([
    ['Users', 'users'],
    ['Users', '"users"'],
    ['Archive.App.Users', 'archive.app.users'],
    ['Users%', 'users%']
  ])('rejects colliding keys %s and %s at plan persistence boundaries', (first, second) => {
    const sourceTableConfig = { [first]: { sample: 1 }, [second]: { sample: 2 } };
    const message = `Source-table keys ${JSON.stringify(first)} and ${JSON.stringify(second)} resolve to the same pattern.`;
    expect(() => normalizeSourceTableConfig(sourceTableConfig)).toThrow(message);
    expect(() => sourceTableConfigsEqual(sourceTableConfig, {})).toThrow(message);

    const config = parse('config: { edition: 3 }').config as PrecompiledSyncConfig;
    const serialized = serializeSyncPlan(config.plan);
    config.plan.sourceTableConfig = sourceTableConfig;
    expect(() => serializeSyncPlan(config.plan)).toThrow(message);
    expect(() => deserializeSyncPlan({ ...serialized, version: 3, sourceTableConfig })).toThrow(message);
  });

  test('preserves authored keys and declaration order for distinct case-sensitive names in saved plans', () => {
    const sourceTableConfig = { '"Users"': { sample: 1 }, users: { sample: 2 } };
    const config = parse('config: { edition: 3 }').config as PrecompiledSyncConfig;
    config.plan.sourceTableConfig = sourceTableConfig;
    const restored = deserializeSyncPlan(serializeSyncPlan(config.plan));
    expect(restored.sourceTableConfig).toEqual(sourceTableConfig);
    expect(Object.keys(restored.sourceTableConfig!)).toEqual(['"Users"', 'users']);
  });

  test('keeps the generated base table schema closed', () => {
    const schema = createSyncRulesSchema();
    const options = (schema.properties as any).config.properties.source_table_options.additionalProperties;
    expect(options).toMatchObject({ additionalProperties: false, properties: {} });
    const validate = compileSyncRulesSchemaValidator(schema);
    expect(validate({ config: { edition: 3, source_table_options: { orders: {} } }, streams: {} })).toBe(true);
    expect(validate({ config: { edition: 3, source_table_options: { orders: { sample: 1 } } }, streams: {} })).toBe(
      false
    );
  });
});
