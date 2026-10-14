import { BasicRouterRequest, Context, JwtPayload } from '@/index.js';
import { loadSyncConfig } from '@/system/LoadedSyncConfig.js';
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
    service_context.loadedSyncConfig = { mode: 'api' };

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
    it.each(['configured', 'unavailable'] as const)('rejects deployment in %s mode', async (mode) => {
      const context = makeContext();
      context.service_context.loadedSyncConfig =
        mode == 'configured'
          ? await loadSyncConfig({ present: true, content: 'streams: [', exit_on_error: false })
          : await loadSyncConfig({ present: true, exit_on_error: false });
      expect(context.service_context.loadedSyncConfig.mode).toBe(mode);
      await expect(deploySyncRules.handler({ context, params: { content: '' }, request })).rejects.toMatchObject({
        message: expect.stringContaining('Sync config API disabled')
      });
      expect(context.service_context.storageEngine.activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });

    it('disables API deployment before configured content is supplied', async () => {
      const context = makeContext();
      context.service_context.loadedSyncConfig = await loadSyncConfig({
        source: 'config',
        present: false,
        exit_on_error: true
      });
      await expect(deploySyncRules.handler({ context, params: { content: '' }, request })).rejects.toMatchObject({
        message: expect.stringContaining('Sync config API disabled')
      });
      expect(context.service_context.storageEngine.activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });

    it('uses present to select API mode even if unused content is supplied', async () => {
      expect(await loadSyncConfig({ present: false, content: 'streams: [', exit_on_error: false })).toEqual({
        mode: 'api'
      });
    });

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
