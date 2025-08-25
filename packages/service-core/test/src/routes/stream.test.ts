import { BasicRouterRequest, Context, SyncRulesBucketStorage } from '@/index.js';
import { logger, RouterResponse, ServiceError } from '@powersync/lib-services-framework';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { Readable, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { mockServiceContext } from './mocks.js';

describe('Stream Route', () => {
  describe('compressed stream', () => {
    it('handles missing sync rules', async () => {
      const context: Context = {
        logger: logger,
        service_context: mockServiceContext(null)
      };

      const request: BasicRouterRequest = {
        headers: {},
        hostname: '',
        protocol: 'http'
      };

      const error = (await (syncStreamed.handler({ context, params: {}, request }) as Promise<RouterResponse>).catch(
        (e) => e
      )) as ServiceError;

      expect(error.errorData.status).toEqual(500);
      expect(error.errorData.code).toEqual('PSYNC_S2302');
    });

    it('handles a stream error with compression', async () => {
      // This primarily tests that an underlying storage error doesn't result in an uncaught error
      // when compressing the stream.

      const storage = {
        getParsedSyncRules() {
          return new SqlSyncRules('bucket_definitions: {}');
        },
        watchCheckpointChanges: async function* (options) {
          throw new Error('Simulated storage error');
        }
      } as Partial<SyncRulesBucketStorage>;
      const serviceContext = mockServiceContext(storage);

      const context: Context = {
        logger: logger,
        service_context: serviceContext,
        token_payload: {
          exp: new Date().getTime() / 1000 + 10000,
          iat: new Date().getTime() / 1000 - 10000,
          sub: 'test-user'
        }
      };

      // It may be worth eventually doing this via Fastify to test the full stack

      const request: BasicRouterRequest = {
        headers: {
          'accept-encoding': 'gzip'
        },
        hostname: '',
        protocol: 'http'
      };

      const response = await (syncStreamed.handler({ context, params: {}, request }) as Promise<RouterResponse>);
      expect(response.status).toEqual(200);
      const stream = response.data as Readable;
      const r = await drainWithTimeout(stream).catch((error) => error);
      expect(r.message).toContain('Simulated storage error');
    });
  });
});

export async function drainWithTimeout(readable: Readable, ms = 2_000) {
  const devNull = new Writable({
    write(_chunk, _enc, cb) {
      cb();
    } // discard everything
  });

  // Throws AbortError if it takes longer than ms, and destroys the stream
  await pipeline(readable, devNull, { signal: AbortSignal.timeout(ms) });
}
