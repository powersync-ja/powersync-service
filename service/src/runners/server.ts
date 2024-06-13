import { deserialize } from 'bson';
import fastify from 'fastify';
import cors from '@fastify/cors';
import * as micro from '@journeyapps-platform/micro';
import * as framework from '@powersync/service-framework';
import { RSocketRequestMeta } from '@powersync/service-rsocket-router';
import { Metrics, routes, utils } from '@powersync/service-core';

import { PowerSyncSystem } from '../system/PowerSyncSystem.js';
import { Router, SocketRouter, StreamingRouter } from '../routes/router.js';
/**
 * Starts an API server
 */
export async function startServer(runnerConfig: utils.RunnerConfig) {
  framework.logger.info('Booting');

  const config = await utils.loadConfig(runnerConfig);
  const system = new PowerSyncSystem(config);

  const server = fastify.fastify();
  server.register(
    Router.plugin({
      routes: [...routes.generateHTTPRoutes(Router), ...micro.router.createProbeRoutes()],
      contextProvider: (payload) => {
        return {
          user_id: undefined,
          system: system
        };
      }
    })
  );

  server.register(cors, {
    origin: '*',
    allowedHeaders: ['Content-Type', 'Authorization'],
    exposedHeaders: ['Content-Type'],
    // Cache time for preflight response
    maxAge: 3600
  });

  server.register(
    StreamingRouter.plugin({
      routes: routes.generateHTTPStreamRoutes(StreamingRouter),
      contextProvider: (payload) => {
        return {
          user_id: undefined,
          system: system
        };
      }
    })
  );

  SocketRouter.applyWebSocketEndpoints(server.server, {
    contextProvider: async (data: Buffer) => {
      const { token } = routes.RSocketContextMeta.decode(deserialize(data) as any);

      if (!token) {
        throw new framework.errors.ValidationError('No token provided in context');
      }

      try {
        const extracted_token = routes.auth.getTokenFromHeader(token);
        if (extracted_token != null) {
          const { context, errors } = await routes.auth.generateContext(system, extracted_token);
          return {
            token,
            ...context,
            token_errors: errors,
            system
          };
        }
      } catch (ex) {
        framework.logger.error(ex);
      }

      return {
        token,
        system
      };
    },
    endpoints: routes.generateSocketRoutes(SocketRouter),
    metaDecoder: async (meta: Buffer) => {
      return RSocketRequestMeta.decode(deserialize(meta) as any);
    },
    payloadDecoder: async (rawData?: Buffer) => rawData && deserialize(rawData)
  });

  framework.logger.info('Starting system');
  await system.start();
  framework.logger.info('System started');

  Metrics.getInstance().configureApiMetrics();

  await micro.fastify.startServer(server, system.config.port);

  // MUST be after startServer.
  // This is so that the handler is run before the server's handler, allowing streams to be interrupted on exit
  system.addTerminationHandler();

  framework.logger.info(`Running on port ${system.config.port}`);
  await system.probe.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
}
