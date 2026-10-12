import { BasicRouterRequest, Context, JwtPayload } from '@/index.js';
import { logger } from '@powersync/lib-services-framework';
import { describe, expect, it, vi } from 'vitest';
import { deploySyncRules, validateSyncRules } from '../../../src/routes/endpoints/sync-rules.js';
import { mockServiceContext } from './mocks.js';

describe('sync-rules routes', () => {
  const request: BasicRouterRequest = {
    headers: {},
    hostname: '',
    protocol: 'http'
  };

  function makeContext(): Context {
    const service_context = mockServiceContext(null);
    (service_context.storageEngine as any).activeBucketStorage = {
      updateSyncRules: vi.fn(async () => ({ replicationStreamName: 'new_slot' }))
    };
    // Deploying through the API requires the sync config to not be set in the service config.
    (service_context as any).configuration = { sync_rules: { present: false } };

    return {
      logger: logger,
      service_context,
      token_payload: new JwtPayload({
        sub: '',
        exp: 0,
        iat: 0
      })
    };
  }

  describe('filter connection validation', () => {
    const FILTERED_CONFIG = /* yaml */ `
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

    it.each(['postgresql', 'mongodb', undefined])('reports connection-type validation for %s', async (type) => {
      const context = makeContext();
      const adapter = context.service_context.routerEngine.getAPI();
      vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(adapter);
      vi.spyOn(adapter, 'getParseSyncRulesOptions').mockReturnValue({
        defaultSchema: 'public',
        connections: type === undefined ? undefined : { default: { type } }
      });
      const response = await validateSyncRules.handler({ context, params: { content: FILTERED_CONFIG }, request });
      const result = JSON.parse(response.data);
      expect(result.valid).toBe(type !== 'postgresql');
      if (type === 'postgresql') {
        expect(result.errors).toEqual([expect.stringContaining('only supported by MongoDB sources')]);
      } else if (type === undefined) {
        expect(result.warnings).toEqual([expect.stringContaining('without connection type information')]);
      } else {
        expect(result.warnings).toBeUndefined();
      }
    });

    it('rejects deployment during parsing before persisting a non-MongoDB filter', async () => {
      const context = makeContext();
      const adapter = context.service_context.routerEngine.getAPI();
      vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(adapter);
      vi.spyOn(adapter, 'getParseSyncRulesOptions').mockReturnValue({
        defaultSchema: 'public',
        connections: { default: { type: 'postgresql' } }
      });
      await expect(deploySyncRules.handler({ context, params: { content: FILTERED_CONFIG }, request })).rejects.toThrow(
        'Sync config parsing failed'
      );
      expect(context.service_context.storageEngine.activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });
  });

  describe('deploy', () => {
    it('logs that bucket_definitions are deprecated', async () => {
      const warnSpy = vi.spyOn(logger, 'warn');

      const response = await deploySyncRules.handler({
        context: makeContext(),
        params: {
          content: `
bucket_definitions:
  global:
    data:
      - SELECT id FROM test
`
        },
        request
      });

      expect(response).toEqual({ slot_name: 'new_slot' });
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('Sync Rules (`bucket_definitions`) are deprecated'));
      warnSpy.mockRestore();
    });

    it('does not log a warning for a Sync Streams config', async () => {
      const warnSpy = vi.spyOn(logger, 'warn');

      await deploySyncRules.handler({
        context: makeContext(),
        params: {
          content: `
config:
  edition: 3
streams:
  global:
    query: SELECT id FROM test
`
        },
        request
      });

      expect(warnSpy).not.toHaveBeenCalledWith(expect.stringContaining('Sync config warning'));
      warnSpy.mockRestore();
    });
  });
});
