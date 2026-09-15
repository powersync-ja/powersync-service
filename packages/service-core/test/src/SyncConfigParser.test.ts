import { SqlSyncConfigParser } from '@/storage/SyncConfigParser.js';
import { isCompatible, parsePersistedSyncConfigContent, updateSyncRulesFromConfig } from '@/storage/storage-index.js';
import { ServiceContextContainer, ServiceContextMode } from '@/system/ServiceContext.js';
import { logger } from '@powersync/lib-services-framework';
import {
  AdditionalSyncConfigParser,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  nodeSqlite,
  SyncConfig
} from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { describe, expect, test, vi } from 'vitest';

const streams = 'config: { edition: 3 }\nstreams:\n  orders:\n    query: SELECT * FROM orders\n';
const configured = `
config:
  edition: 3
  connections:
    default:
      type: example
      tables:
        orders: { sample: 10 }
streams:
  orders:
    query: SELECT * FROM orders
`;

// This fixture represents an external module. Core only knows the connection/table maps; sample is module-owned.
function additionalParser(): AdditionalSyncConfigParser {
  const check = (config: SyncConfig) => {
    for (const connection of Object.values(config.connectionConfig)) {
      if (connection?.type != 'example') continue;
      for (const table of Object.values(connection.tables ?? {})) {
        if ((table as { sample?: number })?.sample === 13) throw new Error('Sample 13 is unsupported.');
      }
    }
  };
  return {
    id: 'example.tables',
    extendJsonSchema({ schema }) {
      const map = (schema.properties as any).config.properties.connections;
      map.additionalProperties = {
        if: { type: 'object', required: ['type'], properties: { type: { const: 'example' } } },
        then: {
          type: 'object',
          additionalProperties: false,
          required: ['type'],
          properties: {
            type: { const: 'example' },
            tables: {
              type: 'object',
              propertyNames: { minLength: 1 },
              additionalProperties: {
                type: 'object',
                additionalProperties: false,
                properties: { sample: { type: 'number' } }
              }
            }
          }
        },
        else: map.additionalProperties
      };
    },
    parse({ context }) {
      check(context.parsedConfig);
    },
    validatePersisted({ config }) {
      check(config);
    }
  };
}

function deployOptions(yaml = configured, parser = new SqlSyncConfigParser([additionalParser()])) {
  return updateSyncRulesFromConfig(parser.parseYaml(yaml, { defaultSchema: 'app' }));
}

describe('service sync config parser', () => {
  test('composes generic registrations atomically and keeps returned schema copies isolated', () => {
    const parser = new SqlSyncConfigParser();
    const baseSchema = parser.jsonSchema;
    expect(() => parser.parseYaml(configured, { defaultSchema: 'app' })).toThrow();
    parser.registerParser(additionalParser());
    expect(parser.parseYaml(configured, { defaultSchema: 'app' }).errors).toEqual([]);
    expect(baseSchema).toEqual(new SqlSyncConfigParser().jsonSchema);
    const composed = parser.jsonSchema;
    delete (composed.properties as any).config.properties.connections;
    expect(parser.parseYaml(configured, { defaultSchema: 'app' }).errors).toEqual([]);

    expect(() => parser.registerParser(additionalParser())).toThrow('already registered');
    expect(() => parser.registerParser({ id: '', parse() {} })).toThrow('must not be empty');
    expect(() =>
      parser.registerParser({
        id: 'bad-schema',
        parse() {},
        extendJsonSchema({ schema }) {
          schema.misspelledKeyword = true;
        }
      })
    ).toThrow();
    // The failed schema is not retained and its ID can be used by a corrected registration.
    parser.registerParser({ id: 'bad-schema', parse() {} });
    expect(parser.parseYaml(configured, { defaultSchema: 'app' }).errors).toEqual([]);
  });

  test('supplies one parser to storage and closes registration before asynchronous startup completes', async () => {
    const context = new ServiceContextContainer({
      serviceMode: ServiceContextMode.TEST_CONNECTION,
      configuration: {
        storage: { type: 'example' },
        api_parameters: {
          max_data_fetch_concurrency: 1,
          max_buckets_per_connection: 1,
          max_parameter_query_results: 1
        }
      } as never
    });
    const parser = context.syncConfigParser;
    parser.registerParser(additionalParser());
    let finish!: () => void;
    const ready = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const getStorage = vi.fn(async () => {
      await ready;
      return { storage: {} as never, reportStorage: {} as never, shutDown: async () => {}, tearDown: async () => true };
    });
    context.storageEngine.registerProvider({ type: 'example', getStorage });
    const startup = context.storageEngine.start();
    try {
      expect(() => parser.registerParser({ id: 'too-late', parse() {} })).toThrow(
        'after the storage engine has started'
      );
      finish();
      await startup;
      expect(getStorage).toHaveBeenCalledWith({ resolvedConfig: context.configuration, syncConfigParser: parser });
      expect(context.syncConfigParser).toBe(parser);
    } finally {
      finish();
      await startup;
      await context.storageEngine.shutDown();
    }
  });

  test('requires the module schema and semantic validation when restoring a saved plan', () => {
    const parser = new SqlSyncConfigParser([additionalParser()]);
    const compiled = deployOptions();
    const restore = (syncConfigParser = parser, compiledPlan = compiled.config.plan) =>
      parsePersistedSyncConfigContent({
        content: configured,
        compiledPlan,
        storageVersion: 2,
        parseOptions: { defaultSchema: 'app' },
        syncConfigParser
      });
    const restored = restore();
    expect(restored.config.connectionConfig).toEqual(compiled.config.parsed.config.connectionConfig);
    expect(
      restored.config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }).connectionConfig
    ).toEqual(restored.config.connectionConfig);
    expect(() => restore(new SqlSyncConfigParser())).toThrow('Unsupported persisted connection configuration');
    const malformed = structuredClone(compiled.config.plan!);
    (malformed.plan.connectionConfig!.default!.tables!.orders as any).sample = 'wrong';
    expect(() => restore(parser, malformed)).toThrow();
    (malformed.plan.connectionConfig!.default!.tables!.orders as any).sample = 13;
    expect(() => restore(parser, malformed)).toThrow('Sample 13');
    // A compiled plan is authoritative; its failure must not be hidden by reparsing valid source YAML.
    expect(() => restore(new SqlSyncConfigParser(), null)).toThrow();
    expect(restore(parser, null).config.connectionConfig).toEqual(restored.config.connectionConfig);
  });

  test('runs generic persisted validators with context and retains their warnings', () => {
    const hook: AdditionalSyncConfigParser = {
      id: 'core-check',
      parse() {},
      validatePersisted({ config, context }) {
        expect(config.getSourceTables()[0].name).toBe('orders');
        expect(context.defaultSchema).toBe('app');
        context.reportDiagnostic({ level: 'warning', message: 'Review this configuration.' });
      }
    };
    const parsed = new SqlSyncConfigParser().parseYaml(streams, { defaultSchema: 'app' });
    const parser = new SqlSyncConfigParser([hook]);
    expect(parser.validatePersisted({ config: parsed.config, context: { defaultSchema: 'app' } })).toEqual([
      expect.objectContaining({ type: 'warning', message: 'Review this configuration.' })
    ]);
    hook.validatePersisted = ({ context }) => context.reportDiagnostic({ level: 'fatal', message: 'Rejected.' });
    expect(() => parser.validatePersisted({ config: parsed.config, context: { defaultSchema: 'app' } })).toThrow(
      'Rejected.'
    );
  });

  test('reuses equal connection options but requires replacement for additions, changes and removals', () => {
    const first = deployOptions();
    const same = deployOptions(configured.replace('SELECT * FROM orders', 'SELECT id FROM orders'));
    const changed = deployOptions(configured.replace('sample: 10', 'sample: 20'));
    const absent = deployOptions(streams);
    expect(isCompatible([first.config.plan], same.config, logger)).toBe(true);
    expect(isCompatible([first.config.plan], changed.config, logger)).toBe(false);
    expect(isCompatible([first.config.plan], absent.config, logger)).toBe(false);
    expect(isCompatible([absent.config.plan], first.config, logger)).toBe(false);
  });

  test('merges equal connection config once and rejects conflicting active/processing definitions', () => {
    const first = deployOptions().config.parsed.config;
    const same = deployOptions(configured.replace('SELECT * FROM orders', 'SELECT id FROM orders')).config.parsed
      .config;
    const other = deployOptions(configured.replace('sample: 10', 'sample: 20')).config.parsed.config;
    const hydrate = (definitions: SyncConfig[]) =>
      new HydratedSyncConfig({
        definitions,
        createParams: { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }
      });
    expect(hydrate([first, same]).connectionConfig).toEqual(first.connectionConfig);
    expect(() => hydrate([first, other])).toThrow('different connection configuration');
    expect(() => hydrate([first, deployOptions(streams).config.parsed.config])).toThrow(
      'different connection configuration'
    );
  });
});
