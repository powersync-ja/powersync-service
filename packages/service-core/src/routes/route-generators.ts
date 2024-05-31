import * as micro from '@journeyapps-platform/micro';

import { Context, RouteGenerator } from './router.js';
import { admin_routes } from './admin.js';
import { writeCheckpoint, writeCheckpoint2 } from './checkpointing.js';
import { dev_routes } from './dev.js';
import { syncRulesRoutes } from './sync-rules.js';
import { IReactiveStream, ReactiveSocketRouter } from '@powersync/service-rsocket-router';
import { sync_stream_reactive } from './socket-route.js';
import { syncStreamed } from './sync-stream.js';

/**
 * Generates router endpoints using the specified router instance
 */
export const generateHTTPRoutes = (router: micro.fastify.FastifyRouter<Context>): micro.router.Route[] => {
  const generators: RouteGenerator[] = [
    ...dev_routes,
    writeCheckpoint,
    writeCheckpoint2,
    ...syncRulesRoutes,
    ...admin_routes
  ];

  return generators.map((generateRoute) => generateRoute(router));
};

/**
 * Generates route endpoint for HTTP sync streaming
 */
export const generateHTTPStreamRoutes = (router: micro.fastify.FastifyRouter<Context>): micro.router.Route[] => {
  return [syncStreamed].map((generateRoute) => generateRoute(router));
};

/**
 *  Generates socket router endpoints using the specified router instance
 */
export const generateSocketRoutes = (router: ReactiveSocketRouter<Context>): IReactiveStream[] => {
  return [sync_stream_reactive].map((generateRoute) => generateRoute(router));
};
