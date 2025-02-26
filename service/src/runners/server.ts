import cors from '@fastify/cors';
import fastify from 'fastify';

import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { MetricModes, registerMetrics } from '../metrics.js';
import { SocketRouter } from '../routes/router.js';
import { ReactiveSocketRouter } from '@powersync/service-rsocket-router';

/**
 * Configures the server portion on a {@link ServiceContext}
 */
export function registerServerServices(serviceContext: core.system.ServiceContextContainer) {
  serviceContext.register(core.routes.RouterEngine, new core.routes.RouterEngine());
  serviceContext.lifeCycleEngine.withLifecycle(serviceContext.routerEngine!, {
    start: async (routerEngine) => {
      await routerEngine!.start(async (routes) => {
        const server = fastify.fastify();

        server.register(cors, {
          origin: '*',
          allowedHeaders: ['Content-Type', 'Authorization', 'User-Agent', 'X-User-Agent'],
          exposedHeaders: ['Content-Type'],
          // Cache time for preflight response
          maxAge: 3600
        });

        core.routes.configureFastifyServer(server, {
          service_context: serviceContext,
          routes: {
            api: { routes: routes.api_routes },
            sync_stream: {
              routes: routes.stream_routes,
              queue_options: {
                concurrency: serviceContext.configuration.api_parameters.max_concurrent_connections,
                max_queue_depth: 0
              }
            }
          }
        });

        const socketRouter = new ReactiveSocketRouter<core.routes.Context>({
          max_concurrent_connections: serviceContext.configuration.api_parameters.max_concurrent_connections
        });

        core.routes.configureRSocket(socketRouter, {
          server: server.server,
          service_context: serviceContext,
          route_generators: routes.socket_routes
        });

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
    },
    stop: (routerEngine) => routerEngine!.shutDown()
  });
}

/**
 * Starts an API server
 */
export async function startServer(runnerConfig: core.utils.RunnerConfig) {
  logger.info('Booting');

  const config = await core.utils.loadConfig(runnerConfig);
  const serviceContext = new core.system.ServiceContextContainer(config);

  registerServerServices(serviceContext);

  await registerMetrics({
    service_context: serviceContext,
    modes: [MetricModes.API]
  });

  const moduleManager = container.getImplementation(core.modules.ModuleManager);
  await moduleManager.initialize(serviceContext);

  logger.info('Starting service...');

  await serviceContext.lifeCycleEngine.start();
  logger.info('Service started.');

  await container.probes.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
}
