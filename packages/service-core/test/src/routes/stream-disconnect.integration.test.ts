import { CHECKPOINT_INVALIDATE_ALL, ContextProvider, JwtPayload, SyncRulesBucketStorage } from '@/index.js';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import { APIMetric } from '@powersync/service-types';
import Fastify from 'fastify';
import * as http from 'node:http';
import * as sqlite from 'node:sqlite';
import { describe, expect, it } from 'vitest';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { registerFastifyErrorHandler, registerFastifyRoutes } from '../../../src/routes/route-register.js';
import { recordingMetricsEngine } from '../recording-metrics.js';
import { mockServiceContext } from './mocks.js';

/**
 * The HTTP transport infers a client disconnect from `clientClosed` with no other close reason
 * attributed, since the route handler has no access to the request socket. That relies on a hangup
 * closing the response stream without erroring it: were it to error, the stream error would take
 * the reason first and every routine reconnect would count as an error, which is exactly what the
 * metric exists to rule out. These tests abort a real client against the route as the service
 * registers it, rather than asserting the assumption against a hand-built error.
 */
describe('Sync stream client disconnect', () => {
  /** A stream still writing operations when the client disconnects, as on an initial sync. */
  function busyStorage() {
    return {
      getParsedSyncRules: () =>
        SqlSyncRules.fromYaml('bucket_definitions:\n  global:\n    data: []', {
          defaultSchema: 'public'
        }).config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }),
      getChecksums: async (_checkpoint, buckets) =>
        new Map(buckets.map(({ bucket }) => [bucket, { bucket, checksum: 1, count: 100_000 }])),
      getBucketDataBatch: async function* () {
        // Bounded so an abort that fails to stop the stream cannot run away.
        for (let batch = 0; batch < 100; batch++) {
          yield {
            chunkData: {
              bucket: 'global[]',
              data: Array.from({ length: 1000 }, () => ({ op: 'PUT' })),
              has_more: true,
              after: `${batch * 1000}`,
              next_after: `${(batch + 1) * 1000}`
            },
            targetOp: null
          };
        }
        yield { hasMore: true };
      },
      watchCheckpointChanges: async function* ({ signal }) {
        yield {
          base: { checkpoint: 1n, lsn: '1', getParameterSets: async () => [] },
          writeCheckpoint: null,
          update: CHECKPOINT_INVALIDATE_ALL
        };
        await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
      }
    } as Partial<SyncRulesBucketStorage>;
  }

  /**
   * Serves one sync stream over a real HTTP server and hangs the client up once the response body
   * starts arriving. The route is registered through `registerFastifyRoutes`, so `clientClosed` and
   * `afterSend` are computed exactly as they are in the service; only authorization is stubbed.
   */
  async function abortClientMidStream(acceptEncoding: string) {
    const recorder = recordingMetricsEngine('stream-disconnect-test');
    const service_context = mockServiceContext(busyStorage(), recorder.engine);
    const contextProvider: ContextProvider = async (_request, options) => ({
      logger: options.logger,
      service_context,
      token_payload: new JwtPayload({
        exp: Date.now() / 1000 + 10_000,
        iat: Date.now() / 1000 - 10_000,
        sub: 'test-user'
      })
    });

    const app = Fastify();
    registerFastifyErrorHandler(app);
    registerFastifyRoutes(app, contextProvider, [{ ...syncStreamed, authorize: async () => ({ authorized: true }) }]);

    try {
      const address = await app.listen({ port: 0, host: '127.0.0.1' });
      const url = new URL(syncStreamed.path, address);

      await new Promise<void>((resolve, reject) => {
        const clientRequest = http.request(
          {
            host: url.hostname,
            port: url.port,
            path: url.pathname,
            method: 'POST',
            headers: { 'content-type': 'application/json', 'accept-encoding': acceptEncoding }
          },
          (clientResponse) => {
            clientResponse.once('data', () => {
              // Hang up the way a client that loses connectivity does, mid-response.
              clientRequest.destroy();
              resolve();
            });
            clientResponse.on('error', () => {});
          }
        );
        clientRequest.on('error', (error) => reject(error));
        clientRequest.end(JSON.stringify({ raw_data: true }));
      });

      // `afterSend` runs once the client is gone, so the metric lands shortly after the abort.
      await expect
        .poll(() =>
          recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {
            outcome: 'success',
            close_reason: 'client_closed',
            error_code: 'none',
            transport: 'http_stream'
          })
        )
        .toBe(1);

      // Nothing errored the response stream: had it, `stream.on('error')` would have claimed the
      // close reason first and an error series would exist instead.
      expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, { outcome: 'error' })).toBeUndefined();
    } finally {
      await app.close();
    }
  }

  it('counts a real client hangup as a success', async () => {
    await abortClientMidStream('identity');
  });

  it('counts a real client hangup on a compressed response as a success', async () => {
    // Compression inserts a pipeline between the sync stream and the socket, so the teardown the
    // handler sees is not necessarily the same one.
    await abortClientMidStream('gzip');
  });
});
