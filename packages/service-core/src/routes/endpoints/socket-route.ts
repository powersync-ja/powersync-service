import { ErrorCode, errors, schema } from '@powersync/lib-services-framework';
import { RequestParameters } from '@powersync/service-sync-rules';

import * as sync from '../../sync/sync-index.js';
import * as util from '../../util/util-index.js';
import { SocketRouteGenerator } from '../router-socket.js';
import { SyncRoutes } from './sync-stream.js';

import { APIMetric } from '@powersync/service-types';

export const syncStreamReactive: SocketRouteGenerator = (router) =>
  router.reactiveStream<util.StreamingSyncRequest, any>(SyncRoutes.STREAM, {
    validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
    handler: async ({ context, params, responder, observer, initialN, signal: upstreamSignal }) => {
      const { service_context, logger } = context;
      const { routerEngine, metricsEngine, syncContext } = service_context;

      logger.defaultMeta = {
        ...logger.defaultMeta,
        user_id: context.token_payload?.sub,
        client_id: params.client_id,
        user_agent: context.user_agent
      };
      const streamStart = Date.now();

      // Best effort guess on why the stream was closed.
      // We use the `??=` operator everywhere, so that we catch the first relevant
      // event, which is usually the most specific.
      let closeReason: string | undefined = undefined;

      // Create our own controller that we can abort directly
      const controller = new AbortController();
      upstreamSignal.addEventListener('abort', () => {
        closeReason ??= 'client closing stream';
        controller.abort();
      });
      if (upstreamSignal.aborted) {
        controller.abort();
      }
      const signal = controller.signal;

      let requestedN = initialN;
      const disposer = observer.registerListener({
        request(n) {
          requestedN += n;
        }
      });

      if (routerEngine.closed) {
        responder.onError(
          new errors.ServiceError({
            status: 503,
            code: ErrorCode.PSYNC_S2003,
            description: 'Service temporarily unavailable'
          })
        );
        responder.onComplete();
        return;
      }

      const syncParams = new RequestParameters(context.token_payload!, params.parameters ?? {});

      const {
        storageEngine: { activeBucketStorage }
      } = service_context;

      const bucketStorage = await activeBucketStorage.getActiveStorage();
      if (bucketStorage == null) {
        responder.onError(
          new errors.ServiceError({
            status: 500,
            code: ErrorCode.PSYNC_S2302,
            description: 'No sync rules available'
          })
        );
        responder.onComplete();
        return;
      }

      const syncRules = bucketStorage.getParsedSyncRules(routerEngine.getAPI().getParseSyncRulesOptions());

      const removeStopHandler = routerEngine.addStopHandler(() => {
        closeReason ??= 'process shutdown';
        controller.abort();
      });

      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(1);
      const tracker = new sync.RequestTracker(metricsEngine);
      try {
        for await (const data of sync.streamResponse({
          syncContext: syncContext,
          bucketStorage: bucketStorage,
          syncRules: syncRules,
          params: {
            ...params,
            binary_data: true // always true for web sockets
          },
          syncParams,
          token: context!.token_payload!,
          tokenStreamOptions: {
            // RSocket handles keepalive events by default
            keep_alive: false
          },
          tracker,
          signal,
          logger
        })) {
          if (signal.aborted) {
            break;
          }
          if (data == null) {
            continue;
          }

          {
            const serialized = sync.syncLineToBson(data);
            responder.onNext({ data: serialized }, false);
            requestedN--;
            tracker.addDataSynced(serialized.length);
          }

          if (requestedN <= 0 && !signal.aborted) {
            await new Promise<void>((resolve) => {
              const l = observer.registerListener({
                request() {
                  if (requestedN > 0) {
                    // Management of updating the total requested items is done above
                    resolve();
                    l();
                    signal.removeEventListener('abort', onAbort);
                  }
                }
              });
              const onAbort = () => {
                // Don't wait here if the request is cancelled
                resolve();
                l();
                signal.removeEventListener('abort', onAbort);
              };
              signal.addEventListener('abort', onAbort);
            });
          }
        }
        closeReason ??= 'service closing stream';
      } catch (ex) {
        // Convert to our standard form before responding.
        // This ensures the error can be serialized.
        const error = new errors.InternalServerError(ex);
        logger.error('Sync stream error', error);
        closeReason ??= 'stream error';
        responder.onError(error);
      } finally {
        responder.onComplete();
        removeStopHandler();
        disposer();
        logger.info(`Sync stream complete`, {
          ...tracker.getLogMeta(),
          stream_ms: Date.now() - streamStart,
          close_reason: closeReason ?? 'unknown'
        });
        metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
      }
    }
  });
