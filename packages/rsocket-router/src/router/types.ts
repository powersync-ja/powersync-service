import * as t from 'ts-codec';
import * as framework from '@powersync/service-framework';

import { OnExtensionSubscriber, OnNextSubscriber, OnTerminalSubscriber } from 'rsocket-core';

import { SocketRouterObserver } from './SocketRouterListener.js';

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
};

export type ReactiveStreamPayload<O> = CommonStreamPayload & {
  initialN: number;
};

export type IReactiveStream<I = any, O = any, C = any> = Omit<
  framework.router.Endpoint<
    I,
    O,
    C,
    framework.router.EndpointHandlerPayload<I, C> & CommonStreamPayload,
    framework.router.EndpointHandler<
      framework.router.EndpointHandlerPayload<I, C> & ReactiveStreamPayload<O>,
      undefined
    >
  >,
  'method'
> & {
  type: RS_ENDPOINT_TYPE.STREAM;
  /**
   * Decodes raw payload buffer to [I].
   * Falls back to router level decoder if not specified.
   */
  decoder?: (rawData?: Buffer) => Promise<I>;
};

export type IReactiveStreamInput<I, O, C> = Omit<IReactiveStream<I, O, C>, 'path' | 'type' | 'method'>;

export type ReactiveEndpoint = IReactiveStream;

export type CommonParams<C> = {
  endpoints: Array<ReactiveEndpoint>;
  contextProvider: (metaData: Buffer) => Promise<C>;
  metaDecoder: (meta: Buffer) => Promise<RequestMeta>;
  payloadDecoder: (rawData?: Buffer) => Promise<any>;
};
