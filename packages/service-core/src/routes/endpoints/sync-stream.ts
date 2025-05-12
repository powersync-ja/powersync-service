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
    const { service_context } = payload.context;
    const { routerEngine, storageEngine, metricsEngine, syncContext } = service_context;
    const headers = payload.request.headers;
    const userAgent = headers['x-user-agent'] ?? headers['user-agent'];
    const clientId = payload.params.client_id;

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
              signal: controller.signal
            })
          ),
          tracker
        ),
        { objectMode: false, highWaterMark: 16 * 1024 }
      );

      const deregister = routerEngine.addStopHandler(() => {
        // This error is not currently propagated to the client
        controller.abort();
        stream.destroy(new Error('Shutting down system'));
      });
      stream.on('close', () => {
        deregister();
      });

      stream.on('error', (error) => {
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
        afterSend: async () => {
          controller.abort();
          metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
          logger.info(`Sync stream complete`, {
            user_id: syncParams.user_id,
            client_id: clientId,
            user_agent: userAgent,
            operations_synced: tracker.operationsSynced,
            data_synced_bytes: tracker.dataSyncedBytes
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
