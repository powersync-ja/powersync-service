import type fastify from 'fastify';

import { registerFastifyNotFoundHandler, registerFastifyRoutes } from './route-register.js';

import * as system from '../system/system-index.js';

import { ADMIN_ROUTES } from './endpoints/admin.js';
import { CHECKPOINT_ROUTES } from './endpoints/checkpointing.js';
import { PROBES_ROUTES } from './endpoints/probes.js';
import { SYNC_RULES_ROUTES } from './endpoints/sync-rules.js';
import { SYNC_STREAM_ROUTES } from './endpoints/sync-stream.js';
import { createRequestQueueHook, CreateRequestQueueParams } from './hooks.js';
import { ContextProvider, RouteDefinition } from './router.js';

/**
 * A list of route definitions to be registered as endpoints.
 * Supplied concurrency limits will be applied to the grouped routes.
 */
export type RouteRegistrationOptions = {
  routes: RouteDefinition[];
  queue_options: CreateRequestQueueParams;
};

/**
 * HTTP routes separated by API and Sync stream categories.
 * This allows for separate concurrency limits.
 */
export type RouteDefinitions = {
  api?: Partial<RouteRegistrationOptions>;
  sync_stream?: Partial<RouteRegistrationOptions>;
};

export type FastifyServerConfig = {
  service_context: system.ServiceContext;
  routes?: RouteDefinitions;
};

export const DEFAULT_ROUTE_OPTIONS = {
  api: {
    routes: [...ADMIN_ROUTES, ...CHECKPOINT_ROUTES, ...SYNC_RULES_ROUTES, ...PROBES_ROUTES],
    queue_options: {
      concurrency: 10,
      max_queue_depth: 20
    }
  },
  sync_stream: {
    routes: [...SYNC_STREAM_ROUTES],
    queue_options: {
      concurrency: 200,
      max_queue_depth: 0
    }
  }
};

/**
 * Registers default routes on a Fastify server. Consumers can optionally configure
 * concurrency queue limits or override routes.
 */
export function configureFastifyServer(server: fastify.FastifyInstance, options: FastifyServerConfig) {
  const { service_context, routes = DEFAULT_ROUTE_OPTIONS } = options;

  const generateContext: ContextProvider = async (request, options) => {
    return {
      service_context: service_context,
      logger: options.logger
    };
  };

  /**
   * Fastify creates an encapsulated context for each `.register` call.
   * Creating a separate context here to separate the concurrency limits for Admin APIs
   * and Sync Streaming routes.
   * https://github.com/fastify/fastify/blob/main/docs/Reference/Encapsulation.md
   */
  server.register(async function (childContext) {
    registerFastifyRoutes(childContext, generateContext, routes.api?.routes ?? DEFAULT_ROUTE_OPTIONS.api.routes);
    registerFastifyNotFoundHandler(childContext);

    // Limit the active concurrent requests
    childContext.addHook(
      'onRequest',
      createRequestQueueHook(routes.api?.queue_options ?? DEFAULT_ROUTE_OPTIONS.api.queue_options)
    );
  });

  // Create a separate context for concurrency queueing
  server.register(async function (childContext) {
    registerFastifyRoutes(
      childContext,
      generateContext,
      routes.sync_stream?.routes ?? DEFAULT_ROUTE_OPTIONS.sync_stream.routes
    );
    // Limit the active concurrent requests
    childContext.addHook(
      'onRequest',
      createRequestQueueHook(routes.sync_stream?.queue_options ?? DEFAULT_ROUTE_OPTIONS.sync_stream.queue_options)
    );
  });
}
