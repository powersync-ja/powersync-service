import cors from '@fastify/cors';
import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';
import fastify from 'fastify';

import { SocketRouter } from '../routes/router.js';
import { PowerSyncSystem } from '../system/PowerSyncSystem.js';
/**
 * Starts an API server
 */
export async function startServer(runnerConfig: core.utils.RunnerConfig) {
  logger.info('Booting');

  const config = await core.utils.loadConfig(runnerConfig);
  const system = new PowerSyncSystem(config);

  const server = fastify.fastify();

  server.register(cors, {
    origin: '*',
    allowedHeaders: ['Content-Type', 'Authorization'],
    exposedHeaders: ['Content-Type'],
    // Cache time for preflight response
    maxAge: 3600
  });

  core.routes.configureFastifyServer(server, { system });
  core.routes.configureRSocket(SocketRouter, { server: server.server, system });

  logger.info('Starting system');
  await system.start();
  logger.info('System started');

  core.Metrics.getInstance().configureApiMetrics();

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
