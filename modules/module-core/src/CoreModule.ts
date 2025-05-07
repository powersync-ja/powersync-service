import cors from '@fastify/cors';
import * as framework from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';
import { ReactiveSocketRouter } from '@powersync/service-rsocket-router';
import fastify from 'fastify';

export class CoreModule extends core.modules.AbstractModule {
  constructor() {
    super({
      name: 'Core'
    });
  }

  public async initialize(context: core.ServiceContextContainer): Promise<void> {
    this.configureTags(context);

    if ([core.system.ServiceContextMode.API, core.system.ServiceContextMode.UNIFIED].includes(context.serviceMode)) {
      // Service should include API routes
      this.registerAPIRoutes(context);
    }

    // Configures a Fastify server and RSocket server
    this.configureRouterImplementation(context);

    // Configures health check probes based off configuration
    this.configureHealthChecks(context);

    await this.configureMetrics(context);
  }

  protected configureTags(context: core.ServiceContextContainer) {
    core.utils.setTags(context.configuration.metadata);
  }

  protected registerAPIRoutes(context: core.ServiceContextContainer) {
    context.routerEngine.registerRoutes({
      api_routes: [
        ...core.routes.endpoints.ADMIN_ROUTES,
        ...core.routes.endpoints.CHECKPOINT_ROUTES,
        ...core.routes.endpoints.SYNC_RULES_ROUTES
      ],
      stream_routes: [...core.routes.endpoints.SYNC_STREAM_ROUTES],
      socket_routes: [core.routes.endpoints.syncStreamReactive]
    });
  }

  /**
   * Configures the HTTP server which will handle routes once the router engine is started
   */
  protected configureRouterImplementation(context: core.ServiceContextContainer) {
    context.lifeCycleEngine.withLifecycle(context.routerEngine, {
      start: async (routerEngine) => {
        // The router engine will only start servers if routes have been registered
        await routerEngine.start(async (routes) => {
          const server = fastify.fastify();

          server.register(cors, {
            origin: '*',
            allowedHeaders: ['Content-Type', 'Authorization', 'User-Agent', 'X-User-Agent'],
            exposedHeaders: ['Content-Type'],
            // Cache time for preflight response
            maxAge: 3600
          });

          core.routes.configureFastifyServer(server, {
            service_context: context,
            routes: {
              api: { routes: routes.api_routes },
              sync_stream: {
                routes: routes.stream_routes,
                queue_options: {
                  concurrency: context.configuration.api_parameters.max_concurrent_connections,
                  max_queue_depth: 0
                }
              }
            }
          });

          const socketRouter = new ReactiveSocketRouter<core.routes.Context>({
            max_concurrent_connections: context.configuration.api_parameters.max_concurrent_connections
          });

          core.routes.configureRSocket(socketRouter, {
            server: server.server,
            service_context: context,
            route_generators: routes.socket_routes
          });

          const { port } = context.configuration;

          await server.listen({
            host: '0.0.0.0',
            port
          });

          framework.logger.info(`Running on port ${port}`);

          return {
            onShutdown: async () => {
              framework.logger.info('Shutting down HTTP server...');
              await server.close();
              framework.logger.info('HTTP server stopped');
            }
          };
        });
      }
    });
  }

  protected configureHealthChecks(context: core.ServiceContextContainer) {
    const {
      configuration: {
        healthcheck: { probes }
      }
    } = context;

    const exposesAPI = [core.system.ServiceContextMode.API, core.system.ServiceContextMode.UNIFIED].includes(
      context.serviceMode
    );

    /**
     * Maintains backwards compatibility if LEGACY_DEFAULT is present by:
     *  - Enabling HTTP probes if the service started in API or UNIFIED mode
     *  - Always enabling filesystem probes always exposing HTTP probes
     * Probe types must explicitly be selected if not using LEGACY_DEFAULT
     */
    if (probes.use_http || (exposesAPI && probes.use_legacy)) {
      context.routerEngine.registerRoutes({
        api_routes: core.routes.endpoints.PROBES_ROUTES
      });
    }

    if (probes.use_legacy || probes.use_filesystem) {
      context.register(framework.ContainerImplementation.PROBES, framework.createFSProbe());
    }
  }

  protected async configureMetrics(context: core.ServiceContextContainer) {
    const apiMetrics = [core.metrics.MetricModes.API];
    const streamMetrics = [core.metrics.MetricModes.REPLICATION, core.metrics.MetricModes.STORAGE];
    const metricsModeMap: Partial<Record<core.system.ServiceContextMode, core.metrics.MetricModes[]>> = {
      [core.system.ServiceContextMode.API]: apiMetrics,
      [core.system.ServiceContextMode.SYNC]: streamMetrics,
      [core.system.ServiceContextMode.UNIFIED]: [...apiMetrics, ...streamMetrics]
    };

    await core.metrics.registerMetrics({
      service_context: context,
      modes: metricsModeMap[context.serviceMode] ?? []
    });
  }

  public async teardown(options: core.modules.TearDownOptions): Promise<void> {}
}
