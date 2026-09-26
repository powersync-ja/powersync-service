import { CHECKPOINT_INVALIDATE_ALL, ContextProvider, JwtPayload, SyncRulesBucketStorage } from '@/index.js';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import { APIMetric } from '@powersync/service-types';
import Fastify from 'fastify';
import * as http from 'node:http';
import * as sqlite from 'node:sqlite';
import { describe, expect, it, onTestFinished } from 'vitest';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { registerFastifyErrorHandler, registerFastifyRoutes } from '../../../src/routes/route-register.js';
import { recordingMetricsEngine } from '../recording-metrics.js';
import { mockServiceContext } from './mocks.js';

// Use real HTTP disconnects to verify that transport teardown isn't counted as a stream error.
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

  async function abortClientMidStream(acceptEncoding: string) {
    const recorder = recordingMetricsEngine('stream-disconnect-test');
    onTestFinished(() => recorder.shutdown());
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
              clientRequest.destroy();
              resolve();
            });
            clientResponse.on('error', () => {});
          }
        );
        clientRequest.on('error', (error) => reject(error));
        clientRequest.end(JSON.stringify({ raw_data: true }));
      });

      // Wait for afterSend to record the close.
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

      expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, { outcome: 'error' })).toBe(0);
      expect(await recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, {})).toBe(1);
      expect(await recorder.seriesValue(APIMetric.CONCURRENT_CONNECTIONS, {})).toBe(0);
    } finally {
      await app.close();
    }
  }

  it('counts a real client hangup as a success', async () => {
    await abortClientMidStream('identity');
  });

  it('counts a real client hangup on a compressed response as a success', async () => {
    // Gzip adds a separate teardown path.
    await abortClientMidStream('gzip');
  });
});
