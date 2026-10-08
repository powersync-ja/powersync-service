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
  test.each([
    ['a..orders: {}', 'Source table patterns must use'],
    ['Users: {}, users: {}', 'resolve to the same pattern']
  ])('persists fatal diagnostics for invalid source-table options: %s', (options, message) => {
    const yaml = /* yaml */ `
      # Sync config fixture.
      config:
        edition: 3
        source_table_options: { ${options} }
      streams:
        orders:
          query: SELECT * FROM orders
    `;
    const parsed = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', throwOnError: false });
    expect(parsed.errors).toContainEqual(
      expect.objectContaining({ type: 'fatal', message: expect.stringContaining(message) })
    );
    const diagnostic = parsed.errors.find((error) => error.type === 'fatal' && error.message.includes(message))!;
    const offendingKey = options.startsWith('a..') ? 'a..orders' : 'users';
    expect(yaml.slice(diagnostic.location.start, diagnostic.location.end)).toContain(offendingKey);
    const update = updateSyncRulesFromConfig(parsed);
    expect(update.config.yaml).toBe(yaml);
    const restored = parsePersistedSyncConfigContent({
      content: update.config.yaml,
      compiledPlan: update.config.plan!,
      storageVersion: 2,
      parseOptions: { defaultSchema: 'app' }
    });
    expect(restored.errors.map(({ message, type, location }) => ({ message, type, location }))).toEqual(
      parsed.errors.map(({ message, type, location }) => ({ message, type, location }))
    );
    expect((restored.config as PrecompiledSyncConfig).plan.sourceTableConfig ?? {}).toEqual({});
  });

  test('keeps valid filters on a partial plan with an unrelated SQL error', () => {
    const yaml = CONFIGURED.replace('SELECT * FROM orders', 'SELECT FROM');
    const parsed = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app', throwOnError: false });
    expect(parsed.errors.some((error) => error.type === 'fatal')).toBe(true);
    const update = updateSyncRulesFromConfig(parsed);
    expect(update.config.plan!.plan.sourceTableConfig).toEqual({
      orders: { mongodb_filter_expression: { $eq: ['$$doc.active', true] } }
    });
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
  test('requires replacement processing when saved source-table options no longer validate', () => {
    const update = updateSyncRulesFromConfig(SqlSyncRules.fromYaml(CONFIGURED, { defaultSchema: 'app' }));
    const existing = structuredClone(update.config.plan!);
    // A saved expression can become unsupported after a module or version change.
    Object.assign(existing.plan.sourceTableConfig!.orders!, { mongodb_filter_expression: { $unsupported: [] } });
    expect(isCompatible([existing], update.config, logger)).toBe(false);
  });
});
