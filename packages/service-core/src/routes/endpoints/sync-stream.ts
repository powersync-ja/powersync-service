import { router, schema } from '@powersync/lib-services-framework';
import Negotiator from 'negotiator';
import { Readable } from 'stream';

import * as sync from '../../sync/sync-index.js';
import * as util from '../../util/util-index.js';

import { APIMetric, event_types } from '@powersync/service-types';
import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

import {
  recordSyncConnection,
  SyncCloseReason,
  syncConnectionCloseReasonLogText,
  SyncTransport
} from '../../metrics/connection-metrics.js';
import { limitParamsForLogging } from '../../util/param-logging.js';
import { maybeCompressResponseStream } from '../compression.js';
import { resolveSyncConnectionSetup } from '../sync-connection.js';

export enum SyncRoutes {
  STREAM = '/sync/stream'
}

const ndJsonContentType = 'application/x-ndjson';
const concatenatedBsonContentType = 'application/vnd.powersync.bson-stream';
const supportedContentTypes = [ndJsonContentType, concatenatedBsonContentType];

export const syncStreamed = routeDefinition({
  path: SyncRoutes.STREAM,
  method: router.HTTPMethod.POST,
  authorize: authUser,
  validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { service_context, logger, token_payload } = payload.context;
    const { routerEngine, metricsEngine, syncContext } = service_context;
    const headers = payload.request.headers;
    const userAgent = headers['x-user-agent'] ?? headers['user-agent'];
    const clientId = payload.params.client_id;
    const streamStart = Date.now();
    const negotiator = new Negotiator(payload.request);
    // This falls back to JSON unless there's preference for the bson-stream in the Accept header.
    const useBson = payload.request.headers.accept
      ? negotiator.mediaType(supportedContentTypes) == concatenatedBsonContentType
      : false;

    logger.defaultMeta = {
      ...logger.defaultMeta,
      user_agent: userAgent,
      client_id: clientId,
      user_id: payload.context.token_payload!.userIdJson,
      bson: useBson
    };
    const sdkData: event_types.ConnectedUserData & event_types.ClientConnectionEventData = {
      client_id: clientId ?? '',
      user_id: payload.context.token_payload!.userIdString,
      user_agent: userAgent as string,
      // At this point the token_payload is guaranteed to be present
      jwt_exp: new Date(token_payload!.exp * 1000),
      connected_at: new Date(streamStart)
    };

    const setup = await resolveSyncConnectionSetup(service_context, SyncTransport.HttpStream);
    if (setup.rejected) {
      throw setup.error;
    }
    const { bucketStorage, syncRules } = setup;

    const controller = new AbortController();
    const tracker = new sync.RequestTracker(metricsEngine);

    const formattedAppMetadata = payload.params.app_metadata
      ? limitParamsForLogging(payload.params.app_metadata)
      : undefined;

    logger.info('Sync stream started', {
      app_metadata: formattedAppMetadata,
      client_params: payload.params.parameters ? limitParamsForLogging(payload.params.parameters) : undefined
    });

    try {
      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(1);
      service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_CONNECT_EVENT, sdkData);
      const syncLines = sync.streamResponse({
        syncContext: syncContext,
        bucketStorage,
        syncRules,
        params: payload.params,
        token: payload.context.token_payload!,
        tracker,
        signal: controller.signal,
        logger,
        isEncodingAsBson: useBson
      });

      const byteContents = useBson ? sync.bsonLines(syncLines) : sync.ndjson(syncLines);
      const plainStream = Readable.from(sync.transformToBytesTracked(byteContents, tracker), {
        objectMode: false,
        highWaterMark: 16 * 1024
      });
      const { stream, encodingHeaders } = maybeCompressResponseStream(negotiator, plainStream, tracker);

      // Best effort guess on why the stream was closed. Keep the first relevant event,
      // which is usually the most specific.
      let closeReason: SyncCloseReason | undefined = undefined;
      let connectionError: unknown;

      const deregister = routerEngine.addStopHandler(() => {
        // This error is not currently propagated to the client
        controller.abort();
        closeReason ??= SyncCloseReason.ProcessShutdown;
        stream.destroy(new Error('Shutting down system'));
      });

      stream.on('end', () => {
        // Auth failure or switch to new sync config
        closeReason ??= SyncCloseReason.ServiceClosed;
      });

      stream.on('close', () => {
        deregister();
      });

      stream.on('error', (error) => {
        if (closeReason == null) {
          closeReason = SyncCloseReason.StreamError;
          connectionError = error;
        }
        controller.abort();
        // Note: This appears as a 200 response in the logs.
        if (error.message != 'Shutting down system') {
          logger.error('Streaming sync request failed', error);
        }
      });

      return new router.RouterResponse({
        status: 200,
        headers: {
          'Content-Type': useBson ? concatenatedBsonContentType : ndJsonContentType,
          ...encodingHeaders,
          // If the service is behind an nginx reverse-proxy with the default configuration, the response we're about to
          // send would be buffered. This is not what we want for this streaming endpoint, and this behavior keeps
          // breaking users. Setting this unconditionally isn't great, but we don't have a reliable way of checking
          // whether we're behind nginx and we just want the default config to work.
          'X-Accel-Buffering': 'no'
        },
        data: stream,
        afterSend: async (details) => {
          // A hangup closes the response stream without erroring it, so a closed request socket
          // with no other reason attributed is the client going away. `??=` keeps it from
          // overriding a server-initiated close or a mid-stream failure, which the socket closing
          // is only a consequence of.
          if (details.clientClosed) {
            closeReason ??= SyncCloseReason.ClientClosed;
          }
          controller.abort();
          metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
          recordSyncConnection(metricsEngine, {
            transport: SyncTransport.HttpStream,
            closeReason: closeReason ?? SyncCloseReason.Unknown,
            error: connectionError
          });
          service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_DISCONNECT_EVENT, {
            ...sdkData,
            disconnected_at: new Date()
          });
          logger.info(`Sync stream complete`, {
            ...tracker.getLogMeta(),
            app_metadata: formattedAppMetadata,
            stream_ms: Date.now() - streamStart,
            close_reason: syncConnectionCloseReasonLogText(closeReason)
          });
        }
      });
    } catch (ex) {
      controller.abort();
      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(-1);
      recordSyncConnection(metricsEngine, {
        transport: SyncTransport.HttpStream,
        closeReason: SyncCloseReason.StreamError,
        error: ex
      });
      service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_DISCONNECT_EVENT, {
        ...sdkData,
        disconnected_at: new Date()
      });
    }
  }
});

export const SYNC_STREAM_ROUTES = [syncStreamed];
