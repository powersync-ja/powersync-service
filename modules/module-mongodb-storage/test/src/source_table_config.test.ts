import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { updateSyncRulesFromConfig } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { PrecompiledSyncConfig, SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { TEST_STORAGE_VERSIONS } from './util.js';

const STREAMS = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    orders:
      query: SELECT * FROM orders
`;
const CONFIGURED = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
    source_table_options:
      orders:
        mongodb_filter_expression: { $eq: ['$$doc.active', true] }
  streams:
    orders:
      query: SELECT * FROM orders
`;

function storageFactory() {
  return mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    supportsMultipleSyncConfigs: true
  });
}

describe.for(TEST_STORAGE_VERSIONS)('source-table config with storage v%i', (storageVersion) => {
  test('preserves source-table options across closing storage and loading its compiled plan', async () => {
    let expected;
    {
      await using factory = await storageFactory().factory();
      const parsed = SqlSyncRules.fromYaml(CONFIGURED, test_utils.PARSE_OPTIONS);
      expected = (parsed.config as PrecompiledSyncConfig).plan.sourceTableConfig;
      await factory.updateSyncRules(updateSyncRulesFromConfig(parsed, { storageVersion }));
    }

    // Reopen the same database without clearing it, as a service restart would.
    await using reopened = await storageFactory().factory({ doNotClear: true });
    const deploying = (await reopened.getDeployingSyncConfig())!;
    expect(deploying.content.compiled_plan?.plan.version).toBe(3);
    const parsed = deploying.content.parsed(test_utils.PARSE_OPTIONS);
    expect((parsed.syncConfigs[0].config as PrecompiledSyncConfig).plan.sourceTableConfig).toEqual(expected);
    expect(parsed.hydratedSyncConfig.sourceTableConfig).toEqual(expected);
  });

  test('continues loading unfiltered plans without source options', async () => {
    await using factory = await storageFactory().factory();
    await factory.updateSyncRules(
      updateSyncRulesFromConfig(SqlSyncRules.fromYaml(STREAMS, test_utils.PARSE_OPTIONS), {
        storageVersion
      })
    );
    const deploying = (await factory.getDeployingSyncConfig())!;
    expect(deploying.content.compiled_plan?.plan.version).toBe(1);
    expect(deploying.content.parsed(test_utils.PARSE_OPTIONS).hydratedSyncConfig.sourceTableConfig).toEqual({});
  });

  test('starts replacement processing when source-table options change', async () => {
    await using factory = await storageFactory().factory();
    const deploy = (yaml: string) =>
      factory.updateSyncRules(
        updateSyncRulesFromConfig(SqlSyncRules.fromYaml(yaml, test_utils.PARSE_OPTIONS), {
          storageVersion
        })
      );
    const first = await deploy(CONFIGURED);
    {
      // Activate the first config so compatibility is checked against data already being served.
      const storage = await test_utils.getTestStorage(factory, first);
      await using writer = await storage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('1/1');
      await writer.commit('1/1');
    }
    if (storageVersion >= 3) {
      // SQL-only edits can share source data when the source-table options are identical.
      const sameOptions = await deploy(CONFIGURED.replace('SELECT * FROM orders', 'SELECT id FROM orders'));
      expect(sameOptions.replicationStreamId).toBe(first.replicationStreamId);
      const storage = await test_utils.getTestStorage(factory, sameOptions);
      await using writer = await storage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('2/1');
      await writer.commit('2/1');
    }
    const changed = await deploy(CONFIGURED.replace('true', 'false'));
    expect(changed.replicationStreamId).not.toBe(first.replicationStreamId);
    expect((await factory.getActiveSyncConfig())?.content.replicationStreamId).toBe(first.replicationStreamId);
  });
});
