import { IReactiveStream, ReactiveSocketRouter } from '@powersync/service-rsocket-router';
import * as t from 'ts-codec';

import { Context } from './router.js';

/**
 * Creates a socket route handler given a router instance
 */
export type SocketRouteGenerator = (router: ReactiveSocketRouter<Context>) => IReactiveStream;

export const RSocketContextMeta = t.object({
  token: t.string
});
