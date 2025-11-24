import { ErrorCode, errors, schema } from '@powersync/lib-services-framework';

import * as sync from '../../sync/sync-index.js';
import * as util from '../../util/util-index.js';
import { SocketRouteGenerator } from '../router-socket.js';
import { SyncRoutes } from './sync-stream.js';

import { APIMetric, event_types } from '@powersync/service-types';
import { formatParamsForLogging } from '../../util/param-logging.js';

export const syncStreamReactive: SocketRouteGenerator = (router) =>
  router.reactiveStream<util.StreamingSyncRequest, any>(SyncRoutes.STREAM, {
    validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
    handler: async ({ context, params, responder, observer, initialN, signal: upstreamSignal, connection }) => {
      const { service_context, logger } = context;
      const { routerEngine, metricsEngine, syncContext } = service_context;
      const streamStart = Date.now();

      logger.defaultMeta = {
        ...logger.defaultMeta,
        user_id: context.token_payload?.sub,
        client_id: params.client_id,
        user_agent: context.user_agent,
        app_metadata: params.app_metadata ? formatParamsForLogging(params.app_metadata ?? {}) : undefined
      };

      const sdkData: event_types.ConnectedUserData & event_types.ClientConnectionEventData = {
        client_id: params.client_id ?? '',
        user_id: context.user_id!,
        user_agent: context.user_agent,
        // At this point the token_payload is guaranteed to be present
        jwt_exp: new Date(context.token_payload!.exp * 1000),
        connected_at: new Date(streamStart)
      };

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
      service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_CONNECT_EVENT, sdkData);
      const tracker = new sync.RequestTracker(metricsEngine);
      if (connection.tracker.encoding) {
        // Must be set before we start the stream
        tracker.setCompressed(connection.tracker.encoding);
      }

      const formattedAppMetadata = params.app_metadata ? formatParamsForLogging(params.app_metadata) : undefined;
      logger.info('Sync stream started', {
        app_metadata: formattedAppMetadata,
        client_params: params.parameters ? formatParamsForLogging(params.parameters) : undefined,
        streams: params.streams?.subscriptions.map((subscription) => subscription.stream)
      });

      try {
        for await (const data of sync.streamResponse({
          syncContext: syncContext,
          bucketStorage: bucketStorage,
          syncRules: {
            syncRules,
            version: bucketStorage.group_id
          },
          params: {
            ...params
          },
          token: context!.token_payload!,
          tokenStreamOptions: {
            // RSocket handles keepalive events by default
            keep_alive: false
          },
          tracker,
          signal,
          logger,
          isEncodingAsBson: true
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
            tracker.addPlaintextDataSynced(serialized.length);
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
        if (connection.tracker.encoding) {
          // Technically, this may not be unique to this specific stream, since there could be multiple
          // rsocket streams on the same websocket connection. We don't have a way to track compressed bytes
          // on individual streams, and we generally expect 1 stream per connection, so this is a reasonable
          // approximation.
          // If there are multiple streams, bytes written would be split arbitrarily across them, but the
          // total should be correct.
          // For non-compressed cases, this is tracked by the stream itself.
          const socketBytes = connection.tracker.getBytesWritten();
          tracker.addCompressedDataSent(socketBytes);
        }
        logger.info(`Sync stream complete`, {
          ...tracker.getLogMeta(),
          app_metadata: formattedAppMetadata,
          stream_ms: Date.now() - streamStart,
          close_reason: closeReason ?? 'unknown'
        });
        metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
        service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_DISCONNECT_EVENT, {
          ...sdkData,
          disconnected_at: new Date()
        });
      }
    }
  });
