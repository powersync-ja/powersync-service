import type { RouteAPI } from '@/api/RouteAPI.js';
import { assertSourceCapabilities, validateNoMongoFilterExpressions } from '@/api/source-capabilities.js';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';

const FILTERED_CONFIG = /* yaml */ `
config:
  edition: 3
  source_table_options:
    orders:
      mongodb_filter_expression: { $eq: ['$$doc.active', true] }
streams:
  orders:
    query: SELECT * FROM orders
`;

describe('unsupported MongoDB source options', () => {
  test('blocks deployment when a non-MongoDB source uses filtering', async () => {
    const { config } = SqlSyncRules.fromYaml(FILTERED_CONFIG, { defaultSchema: 'public' });
    const adapter = {
      async validateSourceCapabilities(config) {
        return validateNoMongoFilterExpressions(config, 'default');
      }
    } satisfies Pick<RouteAPI, 'validateSourceCapabilities'>;
    await expect(assertSourceCapabilities(adapter as unknown as RouteAPI, config)).rejects.toThrow(
      'only supported by MongoDB sources'
    );
  });

  test('ignores options for a different connection', () => {
    const { config } = SqlSyncRules.fromYaml(FILTERED_CONFIG, { defaultSchema: 'public' });
    expect(validateNoMongoFilterExpressions(config, 'other')).toEqual([]);
  });

  test('allows disabled filtering', () => {
    const { config } = SqlSyncRules.fromYaml(FILTERED_CONFIG.replace("{ $eq: ['$$doc.active', true] }", 'disabled'), {
      defaultSchema: 'public'
    });
    expect(validateNoMongoFilterExpressions(config, 'default')).toEqual([]);
  });
});
