import type fastify from 'fastify';
import { registerFastifyRoutes } from './route-register.js';

import * as system from '../system/system-index.js';

import { ADMIN_ROUTES } from './endpoints/admin.js';
import { CHECKPOINT_ROUTES } from './endpoints/checkpointing.js';
import { SYNC_RULES_ROUTES } from './endpoints/sync-rules.js';
import { SYNC_STREAM_ROUTES } from './endpoints/sync-stream.js';
import { createRequestQueueHook, CreateRequestQueueParams } from './hooks.js';
import { RouteDefinition } from './router.js';

/**
 * A list of route definitions to be registered as endpoints.
 * Supplied concurrency limits will be applied to the grouped routes.
 */
export type RouteRegistrationOptions = {
  routes: RouteDefinition[];
  queueOptions: CreateRequestQueueParams;
};

/**
 * HTTP routes separated by API and Sync stream categories.
 * This allows for separate concurrency limits.
 */
export type RouteDefinitions = {
  api?: Partial<RouteRegistrationOptions>;
  syncStream?: Partial<RouteRegistrationOptions>;
};

export type FastifyServerConfig = {
  system: system.CorePowerSyncSystem;
  routes?: RouteDefinitions;
};

export const DEFAULT_ROUTE_OPTIONS = {
  api: {
    routes: [...ADMIN_ROUTES, ...CHECKPOINT_ROUTES, ...SYNC_RULES_ROUTES],
    queueOptions: {
      concurrency: 10,
      max_queue_depth: 20
    }
  },
  syncStream: {
    routes: [...SYNC_STREAM_ROUTES],
    queueOptions: {
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
  const { system, routes = DEFAULT_ROUTE_OPTIONS } = options;
  /**
   * Fastify creates an encapsulated context for each `.register` call.
   * Creating a separate context here to separate the concurrency limits for Admin APIs
   * and Sync Streaming routes.
   * https://github.com/fastify/fastify/blob/main/docs/Reference/Encapsulation.md
   */
  server.register(async function (childContext) {
    registerFastifyRoutes(
      childContext,
      async () => {
        return {
          user_id: undefined,
          system: system
        };
      },
      routes.api?.routes ?? DEFAULT_ROUTE_OPTIONS.api.routes
    );
    // Limit the active concurrent requests
    childContext.addHook(
      'onRequest',
      createRequestQueueHook(routes.api?.queueOptions ?? DEFAULT_ROUTE_OPTIONS.api.queueOptions)
    );
  });

  // Create a separate context for concurrency queueing
  server.register(async function (childContext) {
    registerFastifyRoutes(
      childContext,
      async () => {
        return {
          user_id: undefined,
          system: system
        };
      },
      routes.syncStream?.routes ?? DEFAULT_ROUTE_OPTIONS.syncStream.routes
    );
    // Limit the active concurrent requests
    childContext.addHook(
      'onRequest',
      createRequestQueueHook(routes.syncStream?.queueOptions ?? DEFAULT_ROUTE_OPTIONS.syncStream.queueOptions)
    );
  });
}
