import {
  BasicRouterRequest,
  CHECKPOINT_INVALIDATE_ALL,
  Context,
  JwtPayload,
  RequestTracker,
  streamResponse,
  SyncRulesBucketStorage
} from '@/index.js';
import { logger, RouterResponse, ServiceError } from '@powersync/lib-services-framework';
import {
  DEFAULT_HYDRATION_STATE,
  HydrateSyncConfigParams,
  nodeSqlite,
  SqlSyncRules
} from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { Readable, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { describe, expect, it } from 'vitest';
import winston from 'winston';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS, limitParamsForLogging } from '../../../src/util/param-logging.js';
import { mockServiceContext } from './mocks.js';

describe('Stream Route', () => {
  const defaultHydrationOptions: HydrateSyncConfigParams = {
    hydrationState: DEFAULT_HYDRATION_STATE,
    sqlite: nodeSqlite(sqlite)
  };

  describe('compressed stream', () => {
    it('handles missing sync rules', async () => {
      const context: Context = {
        logger: logger,
        service_context: mockServiceContext(null),
        token_payload: new JwtPayload({
          sub: '',
          exp: 0,
          iat: 0
        })
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
          return new SqlSyncRules('bucket_definitions: {}').hydrate(defaultHydrationOptions);
        },
        watchCheckpointChanges: async function* (options) {
          throw new Error('Simulated storage error');
        }
      } as Partial<SyncRulesBucketStorage>;
      const serviceContext = mockServiceContext(storage);

      const context: Context = {
        logger: logger,
        service_context: serviceContext,
        token_payload: new JwtPayload({
          exp: new Date().getTime() / 1000 + 10000,
          iat: new Date().getTime() / 1000 - 10000,
          sub: 'test-user'
        })
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
          return new SqlSyncRules('bucket_definitions: {}').hydrate(defaultHydrationOptions);
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
        token_payload: new JwtPayload({
          exp: new Date().getTime() / 1000 + 10000,
          iat: new Date().getTime() / 1000 - 10000,
          sub: 'test-user'
        })
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
            user_name: 'bob',
            nested_object: {
              nested_key: 'b'.repeat(1000)
            }
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
      expect(syncStartedLog?.client_params).toEqual(
        expect.objectContaining({
          user_name: 'bob'
        })
      );

      expect(typeof syncStartedLog?.client_params.nested_object).toEqual('string');
      expect(syncStartedLog?.client_params.nested_object.length).toEqual(
        DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS.maxStringLength
      );
    });
  });

  it('closes after a data fetch error while waiting for a new checkpoint', async () => {
    // After 1,000 operations, the sync stream concurrently calls next() on the
    // checkpoint watcher. This test then fails the next data fetch without
    // producing another checkpoint. Before the fix, awaiting watcher.return()
    // during cleanup queued it behind that pending next(), leaving the response
    // open until an unrelated checkpoint was written.
    const syncRules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  global:
    data: []
      `,
      { defaultSchema: 'public' }
    ).config.hydrate(defaultHydrationOptions);

    let checkpointWatcherAborted = false;
    let dataFetches = 0;
    const storage = {
      watchCheckpointChanges: async function* ({ signal }) {
        yield {
          base: {
            checkpoint: 1n,
            lsn: '1',
            getParameterSets: async () => []
          },
          writeCheckpoint: null,
          update: CHECKPOINT_INVALIDATE_ALL
        };

        await new Promise<void>((_resolve, reject) => {
          // There is intentionally no next checkpoint: cleanup must abort this
          // pending wait rather than wait for it to complete naturally.
          signal.addEventListener(
            'abort',
            () => {
              checkpointWatcherAborted = true;
              reject(new Error('Checkpoint watcher aborted'));
            },
            { once: true }
          );
        });
      },
      getChecksums: async (_checkpoint, buckets) =>
        new Map(buckets.map(({ bucket }) => [bucket, { bucket, checksum: 1, count: 1000 }])),
      getBucketDataBatch: async function* () {
        if (dataFetches++ > 0) {
          throw new Error('Simulated data fetch failure');
        }

        yield {
          chunkData: {
            bucket: '1#global[]',
            data: Array.from({ length: 1000 }, () => ({ op: 'PUT' })),
            has_more: true,
            after: '0',
            next_after: '1000'
          },
          targetOp: null
        };
        yield { hasMore: true };
      }
    } as Partial<SyncRulesBucketStorage>;
    const serviceContext = mockServiceContext(storage);
    const controller = new AbortController();

    const response = (async () => {
      for await (const _line of streamResponse({
        syncContext: serviceContext.syncContext,
        bucketStorage: storage as SyncRulesBucketStorage,
        syncRules,
        params: { client_id: 'test-client', raw_data: true },
        token: new JwtPayload({ sub: 'test-user', exp: Date.now() / 1000 + 10_000 }),
        tracker: new RequestTracker(serviceContext.metricsEngine),
        isEncodingAsBson: false,
        signal: controller.signal
      })) {
        // Consume the response until the simulated data fetch error is propagated.
      }
    })();

    const outcome = await Promise.race([
      response.then(
        () => new Error('Sync stream unexpectedly completed'),
        (error) => error
      ),
      new Promise((resolve) => setTimeout(() => resolve(new Error('Sync stream did not close')), 500))
    ]);

    controller.abort();
    await response.catch(() => {});

    expect(outcome).toBeInstanceOf(Error);
    expect((outcome as Error).message).toContain('Simulated data fetch failure');
    expect(checkpointWatcherAborted).toBe(true);
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
