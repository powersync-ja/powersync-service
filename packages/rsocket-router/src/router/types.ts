import * as t from 'ts-codec';
import { router } from '@powersync/lib-services-framework';

import { OnExtensionSubscriber, OnNextSubscriber, OnTerminalSubscriber } from 'rsocket-core';

import { SocketRouterObserver } from './SocketRouterListener.js';
import { WebsocketDuplexConnection } from './transport/WebsocketDuplexConnection.js';

export enum RS_ENDPOINT_TYPE {
  // Other methods are supported by RSocket, but are not yet mapped here
  STREAM = 'stream'
}

export const RSocketRequestMeta = t.object({
  path: t.string
});

export type RequestMeta = t.Decoded<typeof RSocketRequestMeta>;

export type ReactiveSocketRouterOptions<C> = {
  max_concurrent_connections?: number;
};

export type SocketResponder = OnTerminalSubscriber & OnNextSubscriber & OnExtensionSubscriber;

export type CommonStreamPayload = {
  observer: SocketRouterObserver;
  responder: SocketResponder;
  signal: AbortSignal;
  /**
   * The underlying websocket connection. Should not be used directly apart from tracking metadata.
   */
  connection: WebsocketDuplexConnection;
};

export type ReactiveStreamPayload<O> = CommonStreamPayload & {
  initialN: number;
};

export type IReactiveStream<I = any, O = any, C = any> = Omit<
  router.Endpoint<
    I,
    O,
    C,
    router.EndpointHandlerPayload<I, C> & CommonStreamPayload,
    router.EndpointHandler<router.EndpointHandlerPayload<I, C> & ReactiveStreamPayload<O>, undefined>
  >,
  'method'
> & {
  type: RS_ENDPOINT_TYPE.STREAM;
  /**
   * Decodes raw payload buffer to [I].
   * Falls back to router level decoder if not specified.
   */
  decoder?: (rawData?: TypedBuffer) => Promise<I>;
};

/**
 * A {@link Buffer} with an associated mimeType inferred from the RSocket `SETUP` frame.
 */
export interface TypedBuffer {
  mimeType: string;
  contents: Buffer;
}

export type IReactiveStreamInput<I, O, C> = Omit<IReactiveStream<I, O, C>, 'path' | 'type' | 'method'>;

export type ReactiveEndpoint = IReactiveStream;

export type CommonParams<C> = {
  endpoints: Array<ReactiveEndpoint>;
  contextProvider: (metaData: TypedBuffer) => Promise<C>;
  metaDecoder: (meta: TypedBuffer) => Promise<RequestMeta>;
  payloadDecoder: (rawData?: TypedBuffer) => Promise<any>;
};
