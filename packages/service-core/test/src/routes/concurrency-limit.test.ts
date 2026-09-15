import { JwtPayload, routeDefinition } from '@/index.js';
import { router } from '@powersync/lib-services-framework';
import { APIMetric } from '@powersync/service-types';
import Fastify from 'fastify';
import { describe, expect, it, onTestFinished, vi } from 'vitest';
import { configureFastifyServer } from '../../../src/routes/configure-fastify.js';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { recordingMetricsEngine } from '../recording-metrics.js';
import { mockServiceContext } from './mocks.js';

describe('HTTP concurrency limit metrics', () => {
  it('counts each rejected sync request without changing the HTTP 429 response', async () => {
    const recorder = recordingMetricsEngine();
    onTestFinished(() => recorder.shutdown());
    const service = mockServiceContext(null, recorder.engine);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    vi.spyOn(service.storageEngine.activeBucketStorage, 'getActiveSyncConfig').mockImplementation(async () => {
      entered.resolve();
      await release.promise;
      return null;
    });
    const app = Fastify();
    configureFastifyServer(app, {
      service_context: service,
      routes: {
        api: { routes: [] },
        checkpointing: { routes: [] },
        sync_stream: {
          queue_options: { concurrency: 1, max_queue_depth: 0 },
          routes: [
            {
              ...syncStreamed,
              authorize: async ({ context }) => {
                context.token_payload = new JwtPayload({ sub: 'test-user', exp: Date.now() / 1000 + 10_000 });
                return { authorized: true };
              }
            }
          ]
        }
      }
    });
    const first = app.inject({ method: 'POST', url: '/sync/stream', payload: {} });
    try {
      await entered.promise;
      for (let i = 0; i < 3; i++) {
        const response = await app.inject({ method: 'POST', url: '/sync/stream', payload: {} });
        expect(response.statusCode).toBe(429);
        expect(response.body).toBe('');
      }
      expect(
        await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
          outcome: 'rejected',
          close_reason: 'concurrency_limit',
          error_code: 'other',
          transport: 'http_stream'
        })
      ).toBe(3);
      expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(3);
      expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
    } finally {
      release.resolve();
      await first;
      await app.close();
    }
  });

  it.each(['api', 'checkpointing'] as const)('does not count %s queue rejections as sync attempts', async (group) => {
    const recorder = recordingMetricsEngine();
    onTestFinished(() => recorder.shutdown());
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const app = Fastify();
    configureFastifyServer(app, {
      service_context: mockServiceContext(null, recorder.engine),
      routes: {
        api: { routes: [] },
        checkpointing: { routes: [] },
        sync_stream: { routes: [] },
        [group]: {
          queue_options: { concurrency: 1, max_queue_depth: 0 },
          routes: [
            routeDefinition({
              path: '/test',
              method: router.HTTPMethod.GET,
              handler: async () => {
                entered.resolve();
                await release.promise;
                return {};
              }
            })
          ]
        }
      }
    });
    const first = app.inject({ method: 'GET', url: '/test' });
    try {
      await entered.promise;
      const response = await app.inject({ method: 'GET', url: '/test' });
      expect(response.statusCode).toBe(429);
      expect((await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})) ?? 0).toBe(0);
    } finally {
      release.resolve();
      await first;
      await app.close();
    }
  });
});
