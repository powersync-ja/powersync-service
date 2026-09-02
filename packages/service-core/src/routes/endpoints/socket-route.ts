import { errors, schema } from '@powersync/lib-services-framework';

import * as sync from '../../sync/sync-index.js';
import * as util from '../../util/util-index.js';
import { SocketRouteGenerator } from '../router-socket.js';
import { SyncRoutes } from './sync-stream.js';

import { APIMetric, event_types } from '@powersync/service-types';
import {
  recordSyncConnection,
  SyncCloseReason,
  syncConnectionCloseReasonLogText,
  SyncTransport
} from '../../metrics/connection-metrics.js';
import { limitParamsForLogging } from '../../util/param-logging.js';
import { resolveSyncConnectionSetup } from '../sync-connection.js';

export const syncStreamReactive: SocketRouteGenerator = (router) =>
  router.reactiveStream<util.StreamingSyncRequest, any>(SyncRoutes.STREAM, {
    validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
    handler: async ({ context, params, responder, observer, initialN, signal: upstreamSignal, connection }) => {
      const { service_context, logger } = context;
      const { routerEngine, metricsEngine, syncContext } = service_context;
      const streamStart = Date.now();

      logger.defaultMeta = {
        ...logger.defaultMeta,
        user_id: context.token_payload!.userIdJson,
        client_id: params.client_id,
        user_agent: context.user_agent
      };

      const sdkData: event_types.ConnectedUserData & event_types.ClientConnectionEventData = {
        client_id: params.client_id ?? '',
        user_id: context.token_payload!.userIdString,
        user_agent: context.user_agent,
        // At this point the token_payload is guaranteed to be present
        jwt_exp: new Date(context.token_payload!.exp * 1000),
        connected_at: new Date(streamStart)
      };

      // Best effort guess on why the stream was closed. Keep the first relevant event,
      // which is usually the most specific.
      let closeReason: SyncCloseReason | undefined = undefined;
      let connectionError: unknown;

      // Create our own controller that we can abort directly
      const controller = new AbortController();
      upstreamSignal.addEventListener('abort', () => {
        closeReason ??= SyncCloseReason.ClientClosed;
        controller.abort();
      });
      if (upstreamSignal.aborted) {
        closeReason ??= SyncCloseReason.ClientClosed;
        controller.abort();
      }
      const signal = controller.signal;

      let requestedN = initialN;
      const disposer = observer.registerListener({
        request(n) {
          requestedN += n;
        }
      });

      const setup = await resolveSyncConnectionSetup(service_context, SyncTransport.RSocket);
      if (setup.rejected) {
        responder.onError(setup.error);
        responder.onComplete();
        return;
      }
      const { bucketStorage, syncRules } = setup;

      const removeStopHandler = routerEngine.addStopHandler(() => {
        closeReason ??= SyncCloseReason.ProcessShutdown;
        controller.abort();
      });

      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(1);
      service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_CONNECT_EVENT, sdkData);
      const tracker = new sync.RequestTracker(metricsEngine);
      if (connection.tracker.encoding) {
        // Must be set before we start the stream
        tracker.setCompressed(connection.tracker.encoding);
      }

      const formattedAppMetadata = params.app_metadata ? limitParamsForLogging(params.app_metadata) : undefined;
      logger.info('Sync stream started', {
        app_metadata: formattedAppMetadata,
        client_params: params.parameters ? limitParamsForLogging(params.parameters) : undefined
      });

      try {
        for await (const data of sync.streamResponse({
          syncContext: syncContext,
          bucketStorage: bucketStorage,
          syncRules,
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
        closeReason ??= SyncCloseReason.ServiceClosed;
      } catch (ex) {
        // Convert to our standard form before responding.
        // This ensures the error can be serialized.
        // However, use the original error for the logs, so that we have the stack trace.
        const error = new errors.InternalServerError(ex);
        logger.error('Sync stream error', ex);
        if (closeReason == null) {
          closeReason = SyncCloseReason.StreamError;
          connectionError = ex;
        }
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
          close_reason: syncConnectionCloseReasonLogText(closeReason)
        });
        metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
        recordSyncConnection(metricsEngine, {
          transport: SyncTransport.RSocket,
          closeReason: closeReason ?? SyncCloseReason.Unknown,
          error: connectionError
        });
        service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_DISCONNECT_EVENT, {
          ...sdkData,
          disconnected_at: new Date()
        });
      }
    }
  });
