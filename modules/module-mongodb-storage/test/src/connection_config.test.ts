import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { SqlSyncConfigParser, updateSyncRulesFromConfig } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import {
  AdditionalSyncConfigParser,
  normalizeConnectionConfig,
  PrecompiledSyncConfig
} from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { TEST_STORAGE_VERSIONS } from './util.js';

const streams = `
config:
  edition: 3
streams:
  orders:
    query: SELECT * FROM orders
`;
const configured = `
config:
  edition: 3
  connections:
    default:
      type: example
      tables:
        orders:
          sample: 10
streams:
  orders:
    query: SELECT * FROM orders
`;

// A small module-owned option exercises persistence without introducing MongoDB filter syntax into core.
const tableOptions: AdditionalSyncConfigParser = {
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
  }
};

function storageFactory(withModule: boolean) {
  const syncConfigParser = new SqlSyncConfigParser(withModule ? [tableOptions] : []);
  return mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    supportsMultipleSyncConfigs: true,
    syncConfigParser
  });
}

describe.for(TEST_STORAGE_VERSIONS)('connection config with storage v%i', (storageVersion) => {
  test('preserves module options across closing storage and loading its compiled plan', async () => {
    let expected;
    {
      await using factory = await storageFactory(true).factory();
      const parsed = factory.syncConfigParser.parseContent(configured, test_utils.PARSE_OPTIONS);
      expected = parsed.config.connectionConfig;
      await factory.updateSyncRules(updateSyncRulesFromConfig(parsed, { storageVersion }));
    }

    // Reopen the same database without clearing it, as a service restart would.
    await using reopened = await storageFactory(true).factory({ doNotClear: true });
    const deploying = (await reopened.getDeployingSyncConfig())!;
    expect(deploying.content.compiled_plan?.plan.version).toBe(3);
    const parsed = deploying.content.parsed(test_utils.PARSE_OPTIONS);
    expect(parsed.syncConfigs[0].config.connectionConfig).toEqual(expected);
    expect(parsed.hydratedSyncConfig.connectionConfig).toEqual(expected);
  });

  test('rejects saved options if the module is absent after restart', async () => {
    {
      await using factory = await storageFactory(true).factory();
      await factory.updateSyncRules(
        updateSyncRulesFromConfig(factory.syncConfigParser.parseContent(configured, test_utils.PARSE_OPTIONS), {
          storageVersion
        })
      );
    }
    await using reopened = await storageFactory(false).factory({ doNotClear: true });
    const deploying = (await reopened.getDeployingSyncConfig())!;
    expect(() => deploying.content.parsed(test_utils.PARSE_OPTIONS)).toThrow(
      'Missing required sync config parsers: example.tables'
    );
  });

  test('continues loading unfiltered plans without an additional parser', async () => {
    await using factory = await storageFactory(false).factory();
    await factory.updateSyncRules(
      updateSyncRulesFromConfig(factory.syncConfigParser.parseContent(streams, test_utils.PARSE_OPTIONS), {
        storageVersion
      })
    );
    const deploying = (await factory.getDeployingSyncConfig())!;
    expect(deploying.content.compiled_plan?.plan.version).toBe(1);
    expect(deploying.content.parsed(test_utils.PARSE_OPTIONS).hydratedSyncConfig.connectionConfig).toEqual({});
  });

  test('starts replacement processing when connection options change', async () => {
    await using factory = await storageFactory(true).factory();
    const deploy = (yaml: string) =>
      factory.updateSyncRules(
        updateSyncRulesFromConfig(factory.syncConfigParser.parseContent(yaml, test_utils.PARSE_OPTIONS), {
          storageVersion
        })
      );
    const first = await deploy(configured);
    {
      // Activate the first config so compatibility is checked against data already being served.
      await using writer = await factory.getInstance(first).createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('1/1');
      await writer.commit('1/1');
    }
    if (storageVersion >= 3) {
      // SQL-only edits can share source data when the connection options are identical.
      const sameOptions = await deploy(configured.replace('SELECT * FROM orders', 'SELECT id FROM orders'));
      expect(sameOptions.replicationStreamId).toBe(first.replicationStreamId);
      await using writer = await factory.getInstance(sameOptions).createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('2/1');
      await writer.commit('2/1');
    }
    const changed = await deploy(configured.replace('sample: 10', 'sample: 20'));
    expect(changed.replicationStreamId).not.toBe(first.replicationStreamId);
    expect((await factory.getActiveSyncConfig())?.content.replicationStreamId).toBe(first.replicationStreamId);
  });
});
