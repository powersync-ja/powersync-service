import cors from '@fastify/cors';
import { container, errors, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';
import { RSocketRequestMeta } from '@powersync/service-rsocket-router';
import { deserialize } from 'bson';
import fastify from 'fastify';
import { SocketRouter } from './router.js';

export const routerSetup: core.routes.RouterSetup = async () => {
  const serviceContext = container.getImplementation(core.system.ServiceContext);

  const server = fastify.fastify();

  /**
   * Fastify creates an encapsulated context for each `.register` call.
   * Creating a separate context here to separate the concurrency limits for Admin APIs
   * and Sync Streaming routes.
   * https://github.com/fastify/fastify/blob/main/docs/Reference/Encapsulation.md
   */
  server.register(async function (childContext) {
    core.routes.registerFastifyRoutes(
      childContext,
      async () => {
        return {
          user_id: undefined,
          // Put here for convenience
          service_context: serviceContext
        };
      },
      [
        ...core.routes.endpoints.ADMIN_ROUTES,
        ...core.routes.endpoints.CHECKPOINT_ROUTES,
        ...core.routes.endpoints.DEV_ROUTES,
        ...core.routes.endpoints.SYNC_RULES_ROUTES
      ]
    );
    // Limit the active concurrent requests
    childContext.addHook(
      'onRequest',
      core.routes.hooks.createRequestQueueHook({
        max_queue_depth: 20,
        concurrency: 10
      })
    );
  });

  // Create a separate context for concurrency queueing
  server.register(async function (childContext) {
    core.routes.registerFastifyRoutes(
      childContext,
      async () => {
        return {
          user_id: undefined,
          // Put here for convenience
          service_context: container.getImplementation(core.system.ServiceContext)
        };
      },
      [...core.routes.endpoints.SYNC_STREAM_ROUTES]
    );
    // Limit the active concurrent requests
    childContext.addHook(
      'onRequest',
      core.routes.hooks.createRequestQueueHook({
        max_queue_depth: 0,
        concurrency: 200
      })
    );
  });

  server.register(cors, {
    origin: '*',
    allowedHeaders: ['Content-Type', 'Authorization'],
    exposedHeaders: ['Content-Type'],
    // Cache time for preflight response
    maxAge: 3600
  });

  SocketRouter.applyWebSocketEndpoints(server.server, {
    contextProvider: async (data: Buffer) => {
      const { token } = core.routes.RSocketContextMeta.decode(deserialize(data) as any);

      if (!token) {
        throw new errors.ValidationError('No token provided in context');
      }

      const serviceContext = container.getImplementation(core.system.ServiceContext);

      try {
        const extracted_token = core.routes.auth.getTokenFromHeader(token);
        if (extracted_token != null) {
          const { context, errors } = await core.routes.auth.generateContext(extracted_token);
          return {
            token,
            ...context,
            token_errors: errors,
            // Put here for convenience
            service_context: serviceContext
          };
        }
      } catch (ex) {
        logger.error(ex);
      }

      return {
        token,
        service_context: serviceContext
      };
    },
    endpoints: [core.routes.endpoints.syncStreamReactive(SocketRouter)],
    metaDecoder: async (meta: Buffer) => {
      return RSocketRequestMeta.decode(deserialize(meta) as any);
    },
    payloadDecoder: async (rawData?: Buffer) => rawData && deserialize(rawData)
  });

  const { port } = serviceContext.configuration;

  await server.listen({
    host: '0.0.0.0',
    port
  });

  logger.info(`Running on port ${port}`);

  return {
    onShutDown: async () => {
      logger.info('Shutting down HTTP server...');
      await server.close();
      logger.info('HTTP server stopped');
    }
  };
};
