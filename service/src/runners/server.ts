import { deserialize } from 'bson';
import fastify from 'fastify';
import cors from '@fastify/cors';
import * as core from '@powersync/service-core';
import { container, errors, logger } from '@powersync/lib-services-framework';
import { RSocketRequestMeta } from '@powersync/service-rsocket-router';

import { PowerSyncSystem } from '../system/PowerSyncSystem.js';
import { SocketRouter } from '../routes/router.js';
/**
 * Starts an API server
 */
export async function startServer(runnerConfig: core.utils.RunnerConfig) {
  logger.info('Booting');

  const config = await core.utils.loadConfig(runnerConfig);
  const system = new PowerSyncSystem(config);

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
          system: system
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
          system: system
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

      try {
        const extracted_token = core.routes.auth.getTokenFromHeader(token);
        if (extracted_token != null) {
          const { context, errors } = await core.routes.auth.generateContext(system, extracted_token);
          return {
            token,
            ...context,
            token_errors: errors,
            system
          };
        }
      } catch (ex) {
        logger.error(ex);
      }

      return {
        token,
        system
      };
    },
    endpoints: [core.routes.endpoints.syncStreamReactive(SocketRouter)],
    metaDecoder: async (meta: Buffer) => {
      return RSocketRequestMeta.decode(deserialize(meta) as any);
    },
    payloadDecoder: async (rawData?: Buffer) => rawData && deserialize(rawData)
  });

  logger.info('Starting system');
  await system.start();
  logger.info('System started');

  container.getImplementation(core.Metrics).configureApiMetrics();

  await server.listen({
    host: '0.0.0.0',
    port: system.config.port
  });

  container.terminationHandler.handleTerminationSignal(async () => {
    logger.info('Shutting down HTTP server...');
    await server.close();
    logger.info('HTTP server stopped');
  });

  // MUST be after adding the termination handler above.
  // This is so that the handler is run before the server's handler, allowing streams to be interrupted on exit
  system.addTerminationHandler();

  logger.info(`Running on port ${system.config.port}`);
  await container.probes.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
}
