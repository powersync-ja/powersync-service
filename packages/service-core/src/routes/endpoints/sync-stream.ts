import { ErrorCode, errors, logger, router, schema } from '@powersync/lib-services-framework';
import { RequestParameters } from '@powersync/service-sync-rules';
import { Readable } from 'stream';

import * as sync from '../../sync/sync-index.js';
import * as util from '../../util/util-index.js';

import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

import { APIMetric } from '@powersync/service-types';

export enum SyncRoutes {
  STREAM = '/sync/stream'
}

export const syncStreamed = routeDefinition({
  path: SyncRoutes.STREAM,
  method: router.HTTPMethod.POST,
  authorize: authUser,
  validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { service_context, logger } = payload.context;
    const { routerEngine, storageEngine, metricsEngine, syncContext } = service_context;
    const headers = payload.request.headers;
    const userAgent = headers['x-user-agent'] ?? headers['user-agent'];
    const clientId = payload.params.client_id;
    const streamStart = Date.now();

    logger.defaultMeta = {
      ...logger.defaultMeta,
      user_agent: userAgent,
      client_id: clientId,
      user_id: payload.context.user_id
    };

    if (routerEngine.closed) {
      throw new errors.ServiceError({
        status: 503,
        code: ErrorCode.PSYNC_S2003,
        description: 'Service temporarily unavailable'
      });
    }

    const params: util.StreamingSyncRequest = payload.params;
    const syncParams = new RequestParameters(payload.context.token_payload!, payload.params.parameters ?? {});

    const bucketStorage = await storageEngine.activeBucketStorage.getActiveStorage();

    if (bucketStorage == null) {
      throw new errors.ServiceError({
        status: 500,
        code: ErrorCode.PSYNC_S2302,
        description: 'No sync rules available'
      });
    }

    const syncRules = bucketStorage.getParsedSyncRules(routerEngine.getAPI().getParseSyncRulesOptions());

    const controller = new AbortController();
    const tracker = new sync.RequestTracker(metricsEngine);
    try {
      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(1);
      const stream = Readable.from(
        sync.transformToBytesTracked(
          sync.ndjson(
            sync.streamResponse({
              syncContext: syncContext,
              bucketStorage,
              syncRules: syncRules,
              params,
              syncParams,
              token: payload.context.token_payload!,
              tracker,
              signal: controller.signal,
              logger
            })
          ),
          tracker
        ),
        { objectMode: false, highWaterMark: 16 * 1024 }
      );

      // Best effort guess on why the stream was closed.
      // We use the `??=` operator everywhere, so that we catch the first relevant
      // event, which is usually the most specific.
      let closeReason: string | undefined = undefined;

      const deregister = routerEngine.addStopHandler(() => {
        // This error is not currently propagated to the client
        controller.abort();
        closeReason ??= 'process shutdown';
        stream.destroy(new Error('Shutting down system'));
      });

      stream.on('end', () => {
        // Auth failure or switch to new sync rules
        closeReason ??= 'service closing stream';
      });

      stream.on('close', () => {
        deregister();
      });

      stream.on('error', (error) => {
        closeReason ??= 'stream error';
        controller.abort();
        // Note: This appears as a 200 response in the logs.
        if (error.message != 'Shutting down system') {
          logger.error('Streaming sync request failed', error);
        }
      });

      return new router.RouterResponse({
        status: 200,
        headers: {
          'Content-Type': 'application/x-ndjson'
        },
        data: stream,
        afterSend: async (details) => {
          if (details.clientClosed) {
            closeReason ??= 'client closing stream';
          }
          controller.abort();
          metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
          logger.info(`Sync stream complete`, {
            ...tracker.getLogMeta(),
            stream_ms: Date.now() - streamStart,
            close_reason: closeReason ?? 'unknown'
          });
        }
      });
    } catch (ex) {
      controller.abort();
      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
    }
  }
});

export const SYNC_STREAM_ROUTES = [syncStreamed];
