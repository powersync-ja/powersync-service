import { BasicRouterRequest, Context, SyncRulesBucketStorage } from '@/index.js';
import { RouterResponse, ServiceError, logger } from '@powersync/lib-services-framework';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { Readable, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { describe, expect, it } from 'vitest';
import winston from 'winston';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS, limitParamsForLogging } from '../../../src/util/param-logging.js';
import { mockServiceContext } from './mocks.js';

describe('Stream Route', () => {
  describe('compressed stream', () => {
    it('handles missing sync rules', async () => {
      const context: Context = {
        logger: logger,
        service_context: mockServiceContext(null),
        token_payload: {
          sub: '',
          exp: 0,
          iat: 0
        }
      };

      const request: BasicRouterRequest = {
        headers: {},
        hostname: '',
        protocol: 'http'
      };

      const error = (await (
        syncStreamed.handler({
          context,
          params: {},
          request
        }) as Promise<RouterResponse>
      ).catch((e) => e)) as ServiceError;
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

    it('logs the application metadata', async () => {
      const storage = {
        getParsedSyncRules() {
          return new SqlSyncRules('bucket_definitions: {}');
        },
        watchCheckpointChanges: async function* (options) {
          throw new Error('Simulated storage error');
        }
      } as Partial<SyncRulesBucketStorage>;
      const serviceContext = mockServiceContext(storage);

      // Create a custom format to capture log info objects (which include defaultMeta)
      const capturedLogs: any[] = [];
      const captureFormat = winston.format((info) => {
        // Capture the info object which includes defaultMeta merged in
        capturedLogs.push({ ...info });
        return info;
      });

      // Create a test logger with the capture format
      const testLogger = winston.createLogger({
        format: winston.format.combine(captureFormat(), winston.format.json()),
        transports: [new winston.transports.Console()]
      });

      const context: Context = {
        logger: testLogger,
        service_context: serviceContext,
        token_payload: {
          exp: new Date().getTime() / 1000 + 10000,
          iat: new Date().getTime() / 1000 - 10000,
          sub: 'test-user'
        }
      };

      const request: BasicRouterRequest = {
        headers: {
          'accept-encoding': 'gzip'
        },
        hostname: '',
        protocol: 'http'
      };

      const inputMeta = {
        test: 'test',
        long_meta: 'a'.repeat(1000)
      };

      const response = await (syncStreamed.handler({
        context,
        params: {
          app_metadata: inputMeta,
          parameters: {
            user_name: 'bob'
          }
        },
        request
      }) as Promise<RouterResponse>);
      expect(response.status).toEqual(200);
      const stream = response.data as Readable;
      const r = await drainWithTimeout(stream).catch((error) => error);
      expect(r.message).toContain('Simulated storage error');

      // Find the "Sync stream started" log entry
      const syncStartedLog = capturedLogs.find((log) => log.message === 'Sync stream started');
      expect(syncStartedLog).toBeDefined();

      // Verify that app_metadata from defaultMeta is present in the log
      expect(syncStartedLog?.app_metadata).toBeDefined();
      expect(syncStartedLog?.app_metadata).toEqual(limitParamsForLogging(inputMeta));
      // Should trim long metadata
      expect(syncStartedLog?.app_metadata.long_meta.length).toEqual(
        DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS.maxStringLength
      );

      // Verify the explicit log parameters
      expect(syncStartedLog?.client_params).toEqual({
        user_name: 'bob'
      });
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
