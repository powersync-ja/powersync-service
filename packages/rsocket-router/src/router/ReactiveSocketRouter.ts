/**
 * This is a small Router wrapper which uses the RSocket lib
 * to expose reactive websocket stream in an interface similar to
 * other Journey micro routers.
 */
import * as http from 'http';
import { Payload, RSocketServer } from 'rsocket-core';
import * as ws from 'ws';
import { SocketRouterObserver } from './SocketRouterListener.js';
import {
  CommonParams,
  IReactiveStream,
  IReactiveStreamInput,
  RS_ENDPOINT_TYPE,
  ReactiveSocketRouterOptions,
  SocketResponder
} from './types.js';
import { WebsocketServerTransport } from './transport/WebSocketServerTransport.js';
import { container, errors } from '@powersync/lib-services-framework';

export class ReactiveSocketRouter<C> {
  protected activeConnections: number;

  constructor(protected options?: ReactiveSocketRouterOptions<C>) {
    this.activeConnections = 0;
  }

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
    const wss = new ws.WebSocketServer({ noServer: true });
    server.on('upgrade', (request, socket, head) => {
      wss.handleUpgrade(request, socket as any, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    });
    server.on('close', () => wss.close());

    const transport = new WebsocketServerTransport({
      wsCreator: () => wss
    });

    const { logger } = container;

    const rSocketServer = new RSocketServer({
      transport,
      acceptor: {
        accept: async (payload) => {
          const { max_concurrent_connections } = this.options ?? {};
          if (max_concurrent_connections && this.activeConnections >= max_concurrent_connections) {
            throw new errors.JourneyError({
              code: '429',
              description: `Maximum active concurrent connections limit has been reached`
            });
          }

          // Throwing an exception in this context will be returned to the client side request
          if (!payload.metadata) {
            // Meta data is required for endpoint handler path matching
            throw new errors.AuthorizationError('No context meta data provided');
          }

          const context = await params.contextProvider(payload.metadata!);

          return {
            // RequestStream is currently the only supported connection type
            requestStream: (payload, initialN, responder) => {
              const observer = new SocketRouterObserver();

              handleReactiveStream(context, { payload, initialN, responder }, observer, params).catch((ex) => {
                logger.error(ex);
                responder.onError(ex);
                responder.onComplete();
              });

              this.activeConnections++;
              return {
                cancel: () => {
                  this.activeConnections--;
                  observer.triggerCancel();
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

export async function handleReactiveStream<Context>(
  context: Context,
  request: {
    payload: Payload;
    initialN: number;
    responder: SocketResponder;
  },
  observer: SocketRouterObserver,
  params: CommonParams<Context>
) {
  const { payload, responder, initialN } = request;
  const { metadata } = payload;
  const { logger } = container;

  const exitWithError = (error: any) => {
    responder.onError(error);
    responder.onComplete();
  };

  if (!metadata) {
    return exitWithError(new errors.ValidationError('Metadata is not provided'));
  }

  const meta = await params.metaDecoder(metadata);

  const { path } = meta;

  const route = params.endpoints.find((e) => e.path == path && e.type == RS_ENDPOINT_TYPE.STREAM);

  if (!route) {
    return exitWithError(new errors.ResourceNotFound('route', `No route for ${path} is configured`));
  }

  const { handler, authorize, validator, decoder = params.payloadDecoder } = route;
  const requestPayload = await decoder(payload.data || undefined);

  if (validator) {
    const isValid = validator.validate(requestPayload);
    if (!isValid.valid) {
      return exitWithError(new errors.ValidationError(isValid.errors));
    }
  }

  if (authorize) {
    const isAuthorized = await authorize({ params: requestPayload, context, observer, responder });
    if (!isAuthorized.authorized) {
      return exitWithError(new errors.AuthorizationError(isAuthorized.errors));
    }
  }

  try {
    await handler({
      params: requestPayload,
      context,
      observer,
      responder,
      initialN
    });
  } catch (ex) {
    logger.error(ex);
    responder.onError(ex);
    responder.onComplete();
  }
}
