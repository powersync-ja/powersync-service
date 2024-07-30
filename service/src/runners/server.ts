import cors from '@fastify/cors';
import fastify from 'fastify';

import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { SocketRouter } from '../routes/router.js';

/**
 * Starts an API server
 */
export async function startServer(serviceContext: core.system.ServiceContext) {
  logger.info('Booting');

  logger.info('Starting service');

  // TODO cleanup the initialization of metrics
  await serviceContext.start();

  serviceContext.metrics.configureApiMetrics();

  await serviceContext.routerEngine.start(async (routes) => {
    const server = fastify.fastify();

    server.register(cors, {
      origin: '*',
      allowedHeaders: ['Content-Type', 'Authorization'],
      exposedHeaders: ['Content-Type'],
      // Cache time for preflight response
      maxAge: 3600
    });

    core.routes.configureFastifyServer(server, {
      service_context: serviceContext,
      routes: { api: { routes: routes.api_routes }, sync_stream: { routes: routes.stream_routes } }
    });

    core.routes.configureRSocket(SocketRouter, { server: server.server, service_context: serviceContext });

    const { port } = serviceContext.configuration;

    await server.listen({
      host: '0.0.0.0',
      port
    });

    logger.info(`Running on port ${port}`);

    return {
      onShutdown: async () => {
        logger.info('Shutting down HTTP server...');
        await server.close();
        logger.info('HTTP server stopped');
      }
    };
  });

  logger.info('service started');

  await container.probes.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
}
