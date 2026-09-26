import { Context, JwtPayload, RouterEngine, SyncRulesBucketStorage, SyncTransport } from '@/index.js';
import { ErrorCode, logger, RouterResponse, ServiceError } from '@powersync/lib-services-framework';
import {
  handleReactiveStream,
  ReactiveSocketRouter,
  ReactiveStreamRequest,
  SocketRouterObserver
} from '@powersync/service-rsocket-router';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import { APIMetric } from '@powersync/service-types';
import * as sqlite from 'node:sqlite';
import { Readable } from 'node:stream';
import { describe, expect, it, onTestFinished, vi } from 'vitest';
import { syncStreamReactive } from '../../../src/routes/endpoints/socket-route.js';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { recordingMetricsEngine } from '../recording-metrics.js';
import { mockServiceContext } from './mocks.js';

function testContext(storage: Partial<SyncRulesBucketStorage> | null) {
  const recorder = recordingMetricsEngine();
  onTestFinished(() => recorder.shutdown());
  const service = mockServiceContext(storage, recorder.engine);
  const api = service.routerEngine.getAPI();
  service.routerEngine = new RouterEngine();
  service.routerEngine.registerAPI({ ...api, shutdown: async () => {} });
  const context: Context = {
    service_context: service,
    logger: logger.child({}),
    token_payload: new JwtPayload({ sub: 'test-user', exp: Date.now() / 1000 + 10_000 })
  };
  return { recorder, context, service };
}

function startConnection(transport: SyncTransport, context: Context) {
  const controller = new AbortController();
  const disconnected = Promise.withResolvers<void>();
  const errors: unknown[] = [];
  let response: RouterResponse | undefined;
  let clientClosed = false;
  const disconnect = () => {
    clientClosed = true;
    controller.abort();
    (response?.data as Readable | undefined)?.destroy();
    disconnected.resolve();
  };

  const finished =
    transport === SyncTransport.HttpStream
      ? (async () => {
          try {
            response = (await syncStreamed.handler({
              context,
              params: {},
              request: { headers: {}, hostname: '', protocol: 'http' }
            })) as RouterResponse;
            const drained = (async () => {
              try {
                for await (const _chunk of response!.data as Readable) {
                  // Consume the response as the HTTP router would.
                }
              } catch (error) {
                errors.push(error);
                disconnect();
              }
            })();
            // Fastify runs afterSend on socket close, without waiting for the iterator to finish.
            await Promise.race([drained, disconnected.promise]);
            await response.afterSend({ clientClosed });
            await drained;
          } catch (error) {
            errors.push(error);
          }
        })()
      : handleReactiveStream(
          context,
          {
            payload: { data: Buffer.from('{}'), metadata: Buffer.from(JSON.stringify({ path: '/sync/stream' })) },
            metadataMimeType: 'application/json',
            dataMimeType: 'application/json',
            initialN: 10,
            responder: {
              onNext() {},
              onComplete() {},
              onExtension() {},
              onError(error) {
                errors.push(error);
                disconnect();
              }
            },
            connection: { tracker: {} } as ReactiveStreamRequest['connection']
          },
          new SocketRouterObserver(),
          controller,
          {
            contextProvider: async () => context,
            endpoints: [syncStreamReactive(new ReactiveSocketRouter())],
            metaDecoder: async (buffer) => JSON.parse(buffer.contents.toString()),
            payloadDecoder: async (buffer) => buffer && JSON.parse(buffer.contents.toString())
          }
        );
  onTestFinished(async () => {
    disconnect();
    await finished;
  });
  return { finished, disconnect, errors };
}

function idleStorage() {
  const ready = Promise.withResolvers<void>();
  const ending = Promise.withResolvers<void>();
  const storage: Partial<SyncRulesBucketStorage> = {
    getParsedSyncRules: () =>
      new SqlSyncRules('bucket_definitions: {}').hydrate({
        hydrationState: DEFAULT_HYDRATION_STATE,
        sqlite: nodeSqlite(sqlite)
      }),
    async *watchCheckpointChanges({ signal }) {
      const onAbort = () => ending.resolve();
      signal.addEventListener('abort', onAbort, { once: true });
      if (signal.aborted) onAbort();
      ready.resolve();
      try {
        await ending.promise;
      } finally {
        signal.removeEventListener('abort', onAbort);
      }
    }
  };
  return { storage, ready: ready.promise, finish: () => ending.resolve(), fail: ending.reject };
}

describe.each([SyncTransport.HttpStream, SyncTransport.RSocket])('%s connection metrics', (transport) => {
  it.each([
    ['service_unavailable', 'PSYNC_S2003'],
    ['no_sync_config', 'PSYNC_S2302'],
    ['storage_error', 'PSYNC_S2403'],
    ['storage_error', 'other']
  ])('counts %s / %s once without accepting a stream', async (closeReason, errorCode) => {
    const { recorder, context, service } = testContext(null);
    if (closeReason === 'service_unavailable') {
      await service.routerEngine.shutDown();
    } else if (closeReason === 'storage_error') {
      const error =
        errorCode === 'other'
          ? new Error('Storage lookup failed')
          : new ServiceError(ErrorCode.PSYNC_S2403, 'Storage query timed out');
      vi.spyOn(service.storageEngine.activeBucketStorage, 'getActiveSyncConfig').mockRejectedValue(error);
    }

    const connection = startConnection(transport, context);
    await connection.finished;
    expect(connection.errors).toHaveLength(1);
    if (errorCode !== 'other') {
      expect(connection.errors[0]).toMatchObject({ errorData: { code: errorCode } });
    }
    expect(
      await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
        transport,
        outcome: 'rejected',
        close_reason: closeReason,
        error_code: errorCode
      })
    ).toBe(1);
    expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(1);
    expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
  });

  it.each(['client_closed', 'service_closed', 'process_shutdown'])(
    'counts %s once and restores the active gauge',
    async (closeReason) => {
      const source = idleStorage();
      const { recorder, context, service } = testContext(source.storage);
      const connection = startConnection(transport, context);
      await source.ready;
      expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(1);
      expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(0);

      if (closeReason === 'client_closed') {
        connection.disconnect();
      } else if (closeReason === 'process_shutdown') {
        await service.routerEngine.shutDown();
      } else {
        source.finish();
      }
      await connection.finished;
      connection.disconnect();
      await service.routerEngine.shutDown();

      expect(
        await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
          transport,
          outcome: 'success',
          close_reason: closeReason,
          error_code: 'none'
        })
      ).toBe(1);
      expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(1);
      expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
    }
  );

  it('counts token expiry as a successful server close', async () => {
    const source = idleStorage();
    const { recorder, context } = testContext(source.storage);
    context.token_payload = new JwtPayload({ sub: 'test-user', exp: 0 });
    const connection = startConnection(transport, context);
    await connection.finished;

    expect(connection.errors).toEqual([]);
    expect(
      await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
        transport,
        outcome: 'success',
        close_reason: 'service_closed',
        error_code: 'none'
      })
    ).toBe(1);
    expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(1);
    expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
  });

  it.each([
    [new ServiceError(ErrorCode.PSYNC_S2305, 'Too many buckets'), 'PSYNC_S2305'],
    [new Error('Storage read failed'), 'other']
  ] as const)('preserves an accepted stream failure through client disconnect: %s', async (error, errorCode) => {
    const source = idleStorage();
    const { recorder, context } = testContext(source.storage);
    const connection = startConnection(transport, context);
    await source.ready;
    expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(1);
    source.fail(error);
    await connection.finished;

    expect(connection.errors).toHaveLength(1);
    expect(
      await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
        transport,
        outcome: 'error',
        close_reason: 'stream_error',
        error_code: errorCode
      })
    ).toBe(1);
    expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(1);
    expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
  });

  it('counts a sync config hydration failure as one rejection without accepting a stream', async () => {
    const failure = new Error('Persisted sync config could not be hydrated');
    const { recorder, context } = testContext({
      getParsedSyncRules() {
        throw failure;
      }
    });
    const connection = startConnection(transport, context);
    await connection.finished;

    expect(connection.errors).toEqual([failure]);
    expect(
      await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
        transport,
        outcome: 'rejected',
        close_reason: 'sync_config_error',
        error_code: 'other'
      })
    ).toBe(1);
    expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(1);
    expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
  });
});
