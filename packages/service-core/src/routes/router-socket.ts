import * as t from 'ts-codec';
import { ReactiveSocketRouter, IReactiveStream } from '@powersync/service-rsocket-router';

import { Context } from './router.js';

export const RSocketContextMeta = t.object({
  token: t.string
});

/**
 * Creates a socket route handler given a router instance
 */
export type SocketRouteGenerator = (router: ReactiveSocketRouter<Context>) => IReactiveStream;
