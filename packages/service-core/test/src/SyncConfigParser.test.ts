import { SqlSyncConfigParser } from '@/storage/SyncConfigParser.js';
import { isCompatible, parsePersistedSyncConfigContent, updateSyncRulesFromConfig } from '@/storage/storage-index.js';
import { ServiceContextContainer, ServiceContextMode } from '@/system/ServiceContext.js';
import { logger } from '@powersync/lib-services-framework';
import * as syncRules from '@powersync/service-sync-rules';
import {
  AdditionalSyncConfigParser,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  nodeSqlite,
  normalizeConnectionConfig,
  PrecompiledSyncConfig,
  SyncConfig
} from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { describe, expect, test, vi } from 'vitest';

const STREAMS = /* yaml */ `
  config: { edition: 3 }
  streams:
    orders:
      query: SELECT * FROM orders
`;
const CONFIGURED = /* yaml */ `
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
        if (typeof (table as { sample?: number })?.sample != 'number') throw new Error('Invalid persisted sample.');
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
    parse({ config, context }) {
      const connections = normalizeConnectionConfig(
        (config as { config?: { connections?: unknown } }).config?.connections
      );
      for (const [tag, connection] of Object.entries(connections)) {
        if (connection?.type != 'example') continue;
        context.parsedConfig.connectionConfig = { ...context.parsedConfig.connectionConfig, [tag]: connection };
        const { plan } = context.parsedConfig as PrecompiledSyncConfig;
        plan.moduleData = { ...plan.moduleData, ['example.tables']: null };
      }
      check(context.parsedConfig);
    },
    validatePersisted({ config }) {
      check(config);
    }
  };
}

function deployOptions(yaml = CONFIGURED, parser = new SqlSyncConfigParser([additionalParser()])) {
  return updateSyncRulesFromConfig(parser.parseContent(yaml, { defaultSchema: 'app' }));
}

describe('service sync config parser', () => {
  test('reuses compiled validation until a parser is registered', () => {
    const compile = vi.spyOn(syncRules, 'compileSyncRulesSchemaValidator');
    try {
      const parser = new SqlSyncConfigParser();
      expect(compile).toHaveBeenCalledTimes(1);
      for (let i = 0; i < 2; i++) {
        expect(parser.parseContent(STREAMS, { defaultSchema: 'app' }).errors).toEqual([]);
        expect(() => parser.parseContent(CONFIGURED, { defaultSchema: 'app' })).toThrow();
      }
      expect(compile).toHaveBeenCalledTimes(1);

      parser.registerParser(additionalParser());
      expect(compile).toHaveBeenCalledTimes(2);
      for (let i = 0; i < 2; i++) {
        expect(parser.parseContent(CONFIGURED, { defaultSchema: 'app' }).errors).toEqual([]);
        expect(() =>
          parser.parseContent(CONFIGURED.replace('sample: 10', 'sample: wrong'), { defaultSchema: 'app' })
        ).toThrow();
      }
      expect(compile).toHaveBeenCalledTimes(2);
    } finally {
      compile.mockRestore();
    }
  });

  test('composes generic registrations atomically and keeps returned schema copies isolated', () => {
    const parser = new SqlSyncConfigParser();
    const baseSchema = parser.jsonSchema;
    expect(() => parser.parseContent(CONFIGURED, { defaultSchema: 'app' })).toThrow();
    parser.registerParser(additionalParser());
    expect(parser.parseContent(CONFIGURED, { defaultSchema: 'app' }).errors).toEqual([]);
    expect(baseSchema).toEqual(new SqlSyncConfigParser().jsonSchema);
    const composed = parser.jsonSchema;
    delete (composed.properties as any).config.properties.connections;
    expect(parser.parseContent(CONFIGURED, { defaultSchema: 'app' }).errors).toEqual([]);

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
    expect(parser.parseContent(CONFIGURED, { defaultSchema: 'app' }).errors).toEqual([]);
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

  test('requires recorded modules and semantic validation when restoring a saved plan', () => {
    const parser = new SqlSyncConfigParser([additionalParser()]);
    const compiled = deployOptions();
    const restore = (syncConfigParser = parser, compiledPlan = compiled.config.plan) =>
      parsePersistedSyncConfigContent({
        content: CONFIGURED,
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
    expect(() => restore(new SqlSyncConfigParser())).toThrow('Missing required sync config parsers: example.tables');
    const malformed = structuredClone(compiled.config.plan!);
    (malformed.plan.connectionConfig!.default!.tables!.orders as any).sample = 'wrong';
    expect(() => restore(parser, malformed)).toThrow();
    (malformed.plan.connectionConfig!.default!.tables!.orders as any).sample = 13;
    expect(() => restore(parser, malformed)).toThrow('Sample 13');
    // A compiled plan is authoritative; its failure must not be hidden by reparsing valid source YAML.
    expect(() => restore(new SqlSyncConfigParser(), null)).toThrow();
    expect(restore(parser, null).config.connectionConfig).toEqual(restored.config.connectionConfig);
  });

  test('checks all dependencies before hooks and ignores unused registered parsers', () => {
    const validatePersisted = vi.fn();
    const parser = new SqlSyncConfigParser([{ id: 'installed', parse() {}, validatePersisted }]);
    const { config } = parser.parseContent(STREAMS, { defaultSchema: 'app' });
    expect(parser.validatePersisted({ config, context: { defaultSchema: 'app' } })).toEqual([]);
    expect(validatePersisted).not.toHaveBeenCalled();
    (config as PrecompiledSyncConfig).plan.moduleData = { installed: null, missing: null, 'also-missing': null };
    expect(() => parser.validatePersisted({ config, context: { defaultSchema: 'app' } })).toThrow(
      'Missing required sync config parsers: missing, also-missing'
    );
    expect(validatePersisted).not.toHaveBeenCalled();
  });

  test('restores transformed module state without applying the authoring schema', () => {
    const parser = new SqlSyncConfigParser([
      {
        id: 'transformed',
        parse({ context }) {
          const { plan } = context.parsedConfig as PrecompiledSyncConfig;
          plan.moduleData = { ...plan.moduleData, ['transformed']: null };
          context.parsedConfig.connectionConfig = {
            default: { type: 'transformed', tables: { orders: { compiled: true } } }
          };
        }
      }
    ]);
    const compiled = deployOptions(STREAMS, parser);
    const restore = (syncConfigParser: SqlSyncConfigParser) =>
      parsePersistedSyncConfigContent({
        content: STREAMS,
        compiledPlan: compiled.config.plan,
        storageVersion: 2,
        parseOptions: { defaultSchema: 'app' },
        syncConfigParser
      });
    expect((restore(parser).config as PrecompiledSyncConfig).plan.moduleData).toEqual({ transformed: null });
    expect(restore(parser).config.connectionConfig).toEqual(compiled.config.parsed.config.connectionConfig);
    expect(() => restore(new SqlSyncConfigParser())).toThrow('Missing required sync config parsers: transformed');
  });

  test('persists module dependencies without connection options and accepts legacy plans without module data', () => {
    const parser = new SqlSyncConfigParser([
      {
        id: 'required',
        parse({ context }) {
          const { plan } = context.parsedConfig as PrecompiledSyncConfig;
          plan.moduleData = { ...plan.moduleData, ['required']: null };
        }
      }
    ]);
    const compiled = deployOptions(STREAMS, parser);
    expect(compiled.config.plan!.plan.version).toBe(3);
    expect(compiled.config.plan!.plan.moduleData).toEqual({ required: null });
    const restore = (compiledPlan = compiled.config.plan) =>
      parsePersistedSyncConfigContent({
        content: STREAMS,
        compiledPlan,
        storageVersion: 2,
        parseOptions: { defaultSchema: 'app' },
        syncConfigParser: new SqlSyncConfigParser()
      });
    expect(() => restore()).toThrow('Missing required sync config parsers: required');
    const malformed = structuredClone(compiled.config.plan!);
    (malformed.plan as any).moduleData = 'required';
    expect(() => restore(malformed)).toThrow('Invalid sync config module data');
    const legacy = deployOptions(STREAMS, new SqlSyncConfigParser());
    expect((restore(legacy.config.plan).config as PrecompiledSyncConfig).plan.moduleData).toBeUndefined();
  });

  test('runs generic persisted validators with context and retains their warnings', () => {
    const hook: AdditionalSyncConfigParser = {
      id: 'core-check',
      parse({ context }) {
        const { plan } = context.parsedConfig as PrecompiledSyncConfig;
        plan.moduleData = { ...plan.moduleData, ['core-check']: null };
      },
      validatePersisted({ config, context }) {
        expect(config.getSourceTables()[0].name).toBe('orders');
        expect(context.defaultSchema).toBe('app');
        context.reportDiagnostic({ level: 'warning', message: 'Review this configuration.' });
      }
    };
    const parser = new SqlSyncConfigParser([hook]);
    const parsed = parser.parseContent(STREAMS, { defaultSchema: 'app' });
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
    const same = deployOptions(CONFIGURED.replace('SELECT * FROM orders', 'SELECT id FROM orders'));
    const changed = deployOptions(CONFIGURED.replace('sample: 10', 'sample: 20'));
    const absent = deployOptions(STREAMS);
    expect(isCompatible([first.config.plan], same.config, logger)).toBe(true);
    expect(isCompatible([first.config.plan], changed.config, logger)).toBe(false);
    expect(isCompatible([first.config.plan], absent.config, logger)).toBe(false);
    expect(isCompatible([absent.config.plan], first.config, logger)).toBe(false);
  });

  test('merges equal connection config once', () => {
    const first = deployOptions().config.parsed.config;
    const same = deployOptions(CONFIGURED.replace('SELECT * FROM orders', 'SELECT id FROM orders')).config.parsed
      .config;
    const hydrate = (definitions: SyncConfig[]) =>
      new HydratedSyncConfig({
        definitions,
        createParams: { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }
      });
    expect(hydrate([first, same]).connectionConfig).toEqual(first.connectionConfig);
  });
});
