import { isCompatible, parsePersistedSyncConfigContent, updateSyncRulesFromConfig } from '@/storage/storage-index.js';
import { logger } from '@powersync/lib-services-framework';
import { PrecompiledSyncConfig, SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';

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

describe('source-table options', () => {
  test('preserves filters through compiled storage', () => {
    const parsed = SqlSyncRules.fromYaml(CONFIGURED, { defaultSchema: 'app' });
    const update = updateSyncRulesFromConfig(parsed);
    const restored = parsePersistedSyncConfigContent({
      content: CONFIGURED,
      compiledPlan: update.config.plan!,
      storageVersion: 2,
      parseOptions: { defaultSchema: 'app' }
    });
    expect((restored.config as PrecompiledSyncConfig).plan.sourceTableConfig).toEqual({
      orders: { mongodb_filter_expression: { $eq: ['$$doc.active', true] } }
    });
  });
  test('reports invalid expressions before storage', () => {
    expect(() =>
      SqlSyncRules.fromYaml(CONFIGURED.replace('$$doc.active', '$doc.active'), {
        defaultSchema: 'app'
      })
    ).toThrow('Expected a source field');
  });
  test('requires replacement processing when filters change or are removed', () => {
    const deploy = (yaml: string) => updateSyncRulesFromConfig(SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' }));
    const first = deploy(CONFIGURED);
    const same = deploy(CONFIGURED.replace('SELECT *', 'SELECT id'));
    const changed = deploy(CONFIGURED.replace('true', 'false'));
    const removed = deploy(
      CONFIGURED.replace(
        "mongodb_filter_expression: { $eq: ['$$doc.active', true] }",
        'mongodb_filter_expression: disabled'
      )
    );
    expect(isCompatible([first.config.plan], same.config, logger)).toBe(true);
    expect(isCompatible([first.config.plan], changed.config, logger)).toBe(false);
    expect(isCompatible([first.config.plan], removed.config, logger)).toBe(false);
  });
});
