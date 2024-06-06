/**
 * This is a small Router wrapper which uses the RSocket lib
 * to expose reactive websocket stream in an interface similar to
 * other journey micro routers.
 */
import * as micro from '@journeyapps-platform/micro';
import * as http from 'http';
import { Payload, RSocketServer } from 'rsocket-core';
import { WebsocketServerTransport } from 'rsocket-websocket-server';
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

export class ReactiveSocketRouter<C> {
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
    const wss = new ws.WebSocketServer({ noServer: true });

    server.on('upgrade', (request, socket, head) => {
      wss.handleUpgrade(request, socket as any, head, (ws) => {
        const originalSend = ws.send.bind(ws);
        ws.send = (...args) => {
          // Work around for this issue
          // https://github.com/websockets/ws/issues/1515
          if (ws.readyState == ws.CLOSING || ws.readyState == ws.CLOSED) {
            return;
          }
          // @ts-expect-error the overloaded function causes type issues which should be fine in this case
          return originalSend(...args);
        };
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
        accept: async (payload) => {
          // Throwing an exception in this context will be returned to the client side request
          if (!payload.metadata) {
            // Meta data is required for endpoint handler path matching
            throw new micro.errors.AuthorizationError('No context meta data provided');
          }

          const context = await params.contextProvider(payload.metadata!);

          return {
            // RequestStream is currently the only supported connection type
            requestStream: (payload, initialN, responder) => {
              const observer = new SocketRouterObserver();

              handleReactiveStream(context, { payload, initialN, responder }, observer, params).catch((ex) => {
                micro.logger.error(ex);
                responder.onError(ex);
                responder.onComplete();
              });

              return {
                cancel: () => {
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

  const exitWithError = (error: any) => {
    responder.onError(error);
    responder.onComplete();
  };

  if (!metadata) {
    return exitWithError(new micro.errors.ValidationError('Metadata is not provided'));
  }

  const meta = await params.metaDecoder(metadata);

  const { path } = meta;

  const route = params.endpoints.find((e) => e.path == path && e.type == RS_ENDPOINT_TYPE.STREAM);

  if (!route) {
    return exitWithError(new micro.errors.ResourceNotFound('route', `No route for ${path} is configured`));
  }

  const { handler, authorize, validator, decoder = params.payloadDecoder } = route;
  const requestPayload = await decoder(payload.data || undefined);

  if (validator) {
    const isValid = validator.validate(requestPayload);
    if (!isValid.valid) {
      return exitWithError(new micro.errors.ValidationError(isValid.errors));
    }
  }

  if (authorize) {
    const isAuthorized = await authorize({ params: requestPayload, context, observer, responder });
    if (!isAuthorized.authorized) {
      return exitWithError(new micro.errors.AuthorizationError(isAuthorized.errors));
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
    micro.logger.error(ex);
    responder.onError(ex);
    responder.onComplete();
  }
}
