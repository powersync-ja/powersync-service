/**
 * This is a small Router wrapper which uses the RSocket lib
 * to expose reactive websocket stream in an interface similar to
 * other Journey micro routers.
 */
import { ErrorCode, errors, logger, Logger } from '@powersync/lib-services-framework';
import * as http from 'http';
import { Payload, RSocketServer } from 'rsocket-core';
import * as ws from 'ws';
import { SocketRouterObserver } from './SocketRouterListener.js';
import { WebsocketDuplexConnection } from './transport/WebsocketDuplexConnection.js';
import { WebsocketServerTransport } from './transport/WebSocketServerTransport.js';
import {
  CommonParams,
  IReactiveStream,
  IReactiveStreamInput,
  ReactiveSocketRouterOptions,
  RS_ENDPOINT_TYPE,
  SocketResponder
} from './types.js';

export interface ReactiveStreamRequest {
  payload: Payload;
  metadataMimeType: string;
  dataMimeType: string;
  initialN: number;
  responder: SocketResponder;
  connection: WebsocketDuplexConnection;
}

export interface SocketBaseContext {
  logger: Logger;
}

export class ReactiveSocketRouter<C extends SocketBaseContext> {
  constructor(protected options?: ReactiveSocketRouterOptions<C>) {}

  reactiveStream<I, O>(path: string, stream: IReactiveStreamInput<I, O, C>): IReactiveStream<I, O, C> {
    return {
      ...stream,
      type: RS_ENDPOINT_TYPE.STREAM,
      path: path
    };
  }

  /**
   * Apply a set of subscriptions to a raw http server. The specified path is used to tell
   * the server on which path it should handle WebSocket upgrades
   */
  applyWebSocketEndpoints<I>(server: http.Server, params: CommonParams<C>) {
    /**
     * Use upgraded connections from the existing server.
     * This follows a similar pattern to the Journey Micro
     * web sockets router.
     */
    const wss = new ws.WebSocketServer({
      noServer: true,
      perMessageDeflate: {
        zlibDeflateOptions: {
          chunkSize: 128 * 1024, // default is 16kb - increased for better efficiency
          memLevel: 7, // default is 8
          level: 3
        },
        zlibInflateOptions: {
          // for decompressing messages from the client
          chunkSize: 32 * 1024
        },
        // don't keep client context between messages
        clientNoContextTakeover: true,
        // keep context between messages from the server
        serverNoContextTakeover: false,
        // bigger window uses more memory and potentially more cpu. 10-15 is a good range.
        serverMaxWindowBits: 12,
        // Limit concurrent compression threads
        concurrencyLimit: 8,
        // Size (in bytes) below which messages should not be compressed _if context takeover is disabled_.
        threshold: 1024
      }
    });
    server.on('upgrade', (request, socket, head) => {
      wss.handleUpgrade(request, socket as any, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    });
    server.on('close', () => wss.close());

    const transport = new WebsocketServerTransport({
      wsCreator: () => wss
    });

    const rSocketServer = new RSocketServer({
      transport,
      acceptor: {
        accept: async (payload, rsocket) => {
          const connection = (rsocket as any).connection as WebsocketDuplexConnection;

          const { max_concurrent_connections } = this.options ?? {};
          logger.info(`Currently have ${wss.clients.size} active WebSocket connection(s)`);
          // wss.clients.size includes this connection, so we check for greater than
          // TODO: Share connection limit between this and http stream connections
          if (max_concurrent_connections && wss.clients.size > max_concurrent_connections) {
            const err = new errors.ServiceError({
              status: 429,
              code: errors.ErrorCode.PSYNC_S2304,
              description: `Maximum active concurrent connections limit has been reached`
            });
            logger.warn(err);
            throw err;
          }

          // Throwing an exception in this context will be returned to the client side request
          if (!payload.metadata) {
            // Meta data is required for endpoint handler path matching
            throw new errors.AuthorizationError(ErrorCode.PSYNC_S2101, 'No context meta data provided');
          }

          const metadataMimeType = payload.metadataMimeType;
          const dataMimeType = payload.dataMimeType;

          const context = await params.contextProvider({
            mimeType: payload.metadataMimeType,
            contents: payload.metadata!
          });

          return {
            // RequestStream is currently the only supported connection type
            requestStream: (payload, initialN, responder) => {
              const observer = new SocketRouterObserver();
              const abortController = new AbortController();

              // TODO: Consider limiting the number of active streams per connection to prevent abuse
              handleReactiveStream(
                context,
                { payload, initialN, responder, dataMimeType, metadataMimeType, connection },
                observer,
                abortController,
                params
              ).catch((ex) => {
                context.logger.error(ex);
                responder.onError(ex);
                responder.onComplete();
              });
              return {
                cancel: () => {
                  abortController.abort();
                },
                onExtension: () => observer.triggerExtension(),
                request: (n) => observer.triggerRequest(n)
              };
            }
          };
        }
      }
    });

    Promise.resolve().then(() => {
      // RSocket listens for this event before accepting connections
      wss.emit('listening');
    });

    return rSocketServer.bind();
  }
}

export async function handleReactiveStream<Context extends SocketBaseContext>(
  context: Context,
  request: ReactiveStreamRequest,
  observer: SocketRouterObserver,
  abortController: AbortController,
  params: CommonParams<Context>
) {
  const { payload, responder, initialN } = request;
  const { metadata } = payload;
  const startTime = new Date();

  const exitWithError = (error: any) => {
    responder.onError(error);
    responder.onComplete();
  };

  if (!metadata) {
    return exitWithError(new errors.ValidationError('Metadata is not provided'));
  }

  const meta = await params.metaDecoder({
    mimeType: request.metadataMimeType,
    contents: metadata
  });

  const { path } = meta;

  const route = params.endpoints.find((e) => e.path == path && e.type == RS_ENDPOINT_TYPE.STREAM);

  if (!route) {
    return exitWithError(new errors.RouteNotFound(path));
  }

  const { handler, authorize, validator, decoder = params.payloadDecoder } = route;
  const requestPayload = await decoder(
    payload.data
      ? {
          contents: payload.data,
          mimeType: request.dataMimeType
        }
      : undefined
  );

  if (validator) {
    const isValid = validator.validate(requestPayload);
    if (!isValid.valid) {
      return exitWithError(new errors.ValidationError(isValid.errors));
    }
  }

  if (authorize) {
    const isAuthorized = await authorize({
      params: requestPayload,
      context,
      observer,
      signal: abortController.signal,
      connection: request.connection,
      responder
    });
    if (!isAuthorized.authorized) {
      return exitWithError(
        isAuthorized.error ?? new errors.AuthorizationError(ErrorCode.PSYNC_S2101, 'Authorization failed')
      );
    }
  }

  try {
    await handler({
      params: requestPayload,
      context,
      observer,
      signal: abortController.signal,
      responder,
      connection: request.connection,
      initialN
    });
  } catch (ex) {
    logger.error(ex);
    responder.onError(ex);
    responder.onComplete();
  } finally {
    context.logger.info(`STREAM ${path}`, {
      duration_ms: Math.round(new Date().valueOf() - startTime.valueOf() + Number.EPSILON)
    });
  }
}
