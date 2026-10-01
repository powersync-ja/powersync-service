import { BasicRouterRequest, Context, JwtPayload } from '@/index.js';
import { logger } from '@powersync/lib-services-framework';
import { describe, expect, it, vi } from 'vitest';
import { deploySyncRules } from '../../../src/routes/endpoints/sync-rules.js';
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

    it('does not log a warning for a streams config', async () => {
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
