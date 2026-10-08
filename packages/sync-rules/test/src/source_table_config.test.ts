import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
import {
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  HydratedSyncConfig,
  ImplicitSchemaTablePattern,
  nodeSqlite,
  normalizeSourceTableConfig,
  parseSourceTableConfigKey,
  PrecompiledSyncConfig,
  serializeSyncPlan,
  sourceTableConfigsEqual,
  SqlSyncRules,
  TablePattern
} from '../../src/index.js';
import { compileSyncRulesSchemaValidator, createSyncRulesSchema } from '../../src/json_schema.js';

const STREAMS = /* yaml */ ` streams:
    orders:
      query: SELECT * FROM orders `;

function withStreams(config = 'config: { edition: 3 }'): string {
  return `${config.trim()}\n${STREAMS.trim()}\n`;
}

function parse(config: string) {
  return SqlSyncRules.fromYaml(withStreams(config), { defaultSchema: 'app' });
}

describe('source table configuration', () => {
  test('hydration reads shared options from the compiled plan', () => {
    const first = parse(
      'config: { edition: 3, source_table_options: { orders: { mongodb_filter_expression: disabled } } }'
    ).config as PrecompiledSyncConfig;
    const second = parse(
      'config: { edition: 3, source_table_options: { orders: { mongodb_filter_expression: disabled } } }'
    ).config;
    const hydrated = new HydratedSyncConfig({
      definitions: [first, second],
      createParams: { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }
    });
    expect(hydrated.sourceTableConfig).toEqual({
      orders: { mongodb_filter_expression: 'disabled' }
    });
  });
  test('accepts source_table_options alongside other settings and rejects options outside config', () => {
    const yaml = /* yaml */ `
      {
        config:
          {
            edition: 3,
            storage_version: 2,
            source_table_options: { orders: { mongodb_filter_expression: 'disabled' } }
          },
        streams: {}
      }
    `;
    const { config } = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' });
    expect(config.storageVersion).toBe(2);
    expect((config as PrecompiledSyncConfig).plan.sourceTableConfig).toEqual({
      orders: { mongodb_filter_expression: 'disabled' }
    });
    expect(() => SqlSyncRules.fromYaml(`${withStreams()}source_table_options: {}`, { defaultSchema: 'app' })).toThrow(
      "Unknown key 'source_table_options'."
    );
  });

  test('does not select additional replication sources', () => {
    const { config } = parse(/* yaml */ ` config:
        edition: 3
        source_table_options:
          orders: { mongodb_filter_expression: 'disabled' }
          another: {} `);
    expect((config as PrecompiledSyncConfig).plan.sourceTableConfig).toEqual({
      orders: { mongodb_filter_expression: 'disabled' },
      another: {}
    });
    expect(config.getSourceTables().map((table) => table.name)).toEqual(['orders']);
  });

  test.each([
    ['"": {}', 'Source table patterns must use'],
    ['orders: null', 'Options for a source table must be a map.'],
    ['orders: []', 'Options for a source table must be a map.'],
    ['a.b.c.d: {}', 'Source table patterns must use'],
    ['a..orders: {}', 'Source table patterns must use'],
    ['orders: { unknown: true }', "Unknown key 'unknown'."],
    ['orders: { mongodb_filter_expression: wrong }', 'Expected exactly one']
  ])('rejects invalid source-table options: %s', (entry, message) => {
    expect(() => parse(`config: { edition: 3, source_table_options: { ${entry} } }`)).toThrow(message);
  });

  test.each(['Orders', '"Orders"'])('matches SQL stream identifier case for %s', (name) => {
    const { config } = SqlSyncRules.fromYaml(
      /* yaml */ `
        # Sync config fixture.
        config:
          edition: 3
        streams:
          orders:
            query: SELECT * FROM ${name}
      `,
      { defaultSchema: 'app' }
    );
    const hydrated = config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
    const [streamPattern] = hydrated.getSourceTables();
    expect(parseSourceTableConfigKey(name).toTablePattern('app')).toEqual(streamPattern);
    expect(streamPattern.tablePattern).toBe(name.startsWith('"') ? 'Orders' : 'orders');
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
    expect(() => SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' })).toThrow(
      `Source-table keys ${JSON.stringify(first)} and ${JSON.stringify(second)} resolve to the same pattern.`
    );
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
    expect(
      sourceTableConfigsEqual(
        { Users: { mongodb_filter_expression: 'disabled' } },
        { users: { mongodb_filter_expression: 'disabled' } }
      )
    ).toBe(false);
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
    const { config } = parse(/* yaml */ ` config:
        edition: 3
        source_table_options:
          '"audit.events"': { mongodb_filter_expression: 'disabled' } `);
    expect((config as PrecompiledSyncConfig).plan.sourceTableConfig).toEqual({
      '"audit.events"': { mongodb_filter_expression: 'disabled' }
    });

    const escapedQuote = parse(/* yaml */ `config:
        edition: 3
        source_table_options:
          'conn.test."na""me"': { mongodb_filter_expression: 'disabled' } `);
    expect((escapedQuote.config as PrecompiledSyncConfig).plan.sourceTableConfig).toEqual({
      'conn.test."na""me"': { mongodb_filter_expression: 'disabled' }
    });
  });

  test('preserves declaration order and options through the sync plan', () => {
    const result = parse(/* yaml */ ` config:
        edition: 3
        source_table_options:
          orders%: { mongodb_filter_expression: 'disabled' }
          orders: { mongodb_filter_expression: 'disabled' } `);
    const plan = serializeSyncPlan((result.config as PrecompiledSyncConfig).plan);
    expect(plan.version).toBe(3);
    expect(plan.sourceTableConfig).toEqual((result.config as PrecompiledSyncConfig).plan.sourceTableConfig);
    expect(deserializeSyncPlan(plan).sourceTableConfig).toEqual(
      (result.config as PrecompiledSyncConfig).plan.sourceTableConfig
    );
    expect(Object.keys(plan.sourceTableConfig!)).toEqual(['orders%', 'orders']);
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
    expect(() =>
      deserializeSyncPlan({ ...plan, sourceTableConfig: { orders: { mongodb_filter_expression: 'disabled' } } })
    ).toThrow('version 3');
  });

  test('normalizes options without reordering table declarations', () => {
    const config = {
      'orders%': { mongodb_filter_expression: 'disabled' as const },
      orders: { mongodb_filter_expression: 'disabled' as const }
    };
    expect(sourceTableConfigsEqual(config, { ...config })).toBe(true);
    expect(sourceTableConfigsEqual(config, { orders: config.orders, 'orders%': config['orders%'] })).toBe(false);
  });

  test.each([
    ['Users', 'users'],
    ['Users', '"users"'],
    ['Archive.App.Users', 'archive.app.users'],
    ['Users%', 'users%']
  ])('rejects colliding keys %s and %s at plan persistence boundaries', (first, second) => {
    const sourceTableConfig = {
      [first]: { mongodb_filter_expression: 'disabled' as const },
      [second]: { mongodb_filter_expression: 'disabled' as const }
    };
    const message = `Source-table keys ${JSON.stringify(first)} and ${JSON.stringify(second)} resolve to the same pattern.`;
    expect(() => normalizeSourceTableConfig(sourceTableConfig)).toThrow(message);
    expect(sourceTableConfigsEqual(sourceTableConfig, {})).toBe(false);
    expect(sourceTableConfigsEqual({}, sourceTableConfig)).toBe(false);
    expect(sourceTableConfigsEqual(sourceTableConfig, sourceTableConfig)).toBe(false);

    const config = parse('config: { edition: 3 }').config as PrecompiledSyncConfig;
    const serialized = serializeSyncPlan(config.plan);
    config.plan.sourceTableConfig = sourceTableConfig;
    expect(() => serializeSyncPlan(config.plan)).toThrow(message);
    expect(() => deserializeSyncPlan({ ...serialized, version: 3, sourceTableConfig })).toThrow(message);
  });

  test('preserves authored keys and declaration order for distinct case-sensitive names in saved plans', () => {
    const sourceTableConfig = {
      '"Users"': { mongodb_filter_expression: 'disabled' as const },
      users: { mongodb_filter_expression: 'disabled' as const }
    };
    const config = parse('config: { edition: 3 }').config as PrecompiledSyncConfig;
    config.plan.sourceTableConfig = sourceTableConfig;
    const restored = deserializeSyncPlan(serializeSyncPlan(config.plan));
    expect(restored.sourceTableConfig).toEqual(sourceTableConfig);
    expect(Object.keys(restored.sourceTableConfig!)).toEqual(['"Users"', 'users']);
  });

  test('generated schema accepts supported table options and rejects unknown options', () => {
    const schema = createSyncRulesSchema();
    const validate = compileSyncRulesSchemaValidator(schema);
    expect(validate({ config: { edition: 3, source_table_options: { orders: {} } }, streams: {} })).toBe(true);
    expect(validate({ config: { edition: 3, source_table_options: { orders: { unknown: true } } }, streams: {} })).toBe(
      false
    );
    expect(
      validate({
        config: { edition: 3, source_table_options: { orders: { mongodb_filter_expression: 'disabled' } } },
        streams: {}
      })
    ).toBe(true);
  });
});
