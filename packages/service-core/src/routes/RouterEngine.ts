import { logger } from '@powersync/lib-services-framework';

import * as api from '../api/api-index.js';

import { SocketRouteGenerator } from './router-socket.js';
import { RouteDefinition } from './router.js';

export type RouterSetupResponse = {
  onShutdown: () => Promise<void>;
};

export type RouterEngineRoutes = {
  api_routes: RouteDefinition[];
  stream_routes: RouteDefinition[];
  socket_routes: SocketRouteGenerator[];
};

export type RouterSetup = (routes: RouterEngineRoutes) => Promise<RouterSetupResponse>;

/**
 *  Serves as a registry from which SyncAPIs can be retrieved based on Replication DataSource type
 *  Initially only one SyncAPI per DataSource type is supported
 */
export class RouterEngine {
  closed: boolean;
  routes: RouterEngineRoutes;

  protected stopHandlers: Set<() => void>;

  /**
   * A final cleanup handler to be executed after all stopHandlers
   */
  protected cleanupHandler: (() => Promise<void>) | null;

  private api: api.RouteAPI | null;

  constructor() {
    this.api = null;
    this.stopHandlers = new Set();
    this.cleanupHandler = null;
    this.closed = false;

    this.routes = {
      api_routes: [],
      stream_routes: [],
      socket_routes: []
    };
  }

  public registerRoutes(routes: Partial<RouterEngineRoutes>) {
    this.routes.api_routes.push(...(routes.api_routes ?? []));
    this.routes.stream_routes.push(...(routes.stream_routes ?? []));
    this.routes.socket_routes.push(...(routes.socket_routes ?? []));
  }

  public get hasRoutes() {
    return (
      this.routes.api_routes.length > 0 || this.routes.stream_routes.length > 0 || this.routes.socket_routes.length > 0
    );
  }

  public registerAPI(api: api.RouteAPI) {
    if (this.api) {
      logger.warn('A RouteAPI has already been registered. Overriding existing implementation');
    }

    this.api = api;
  }

  public getAPI(): api.RouteAPI {
    if (!this.api) {
      throw new Error('No RouteAPI adapter has been registered yet.');
    }
    return this.api;
  }

  /**
   * Starts the router given the configuration provided
   */
  async start(setup: RouterSetup) {
    logger.info('Starting Router Engine...');

    if (!this.hasRoutes) {
      logger.info('Router Engine will not start an HTTP server as no routes have been registered.');
      return;
    }

    const { onShutdown } = await setup(this.routes);
    this.cleanupHandler = onShutdown;
    logger.info('Successfully started Router Engine.');
  }

  /**
   * Runs all stop handlers then final cleanup.
   */
  async shutDown() {
    logger.info('Shutting down Router Engine...');
    // Close open streams, so that they don't block the server from closing.
    // Note: This does not work well when streaming requests are queued. In that case, the server still doesn't
    // close in the 30-second timeout.
    this.closed = true;

    logger.info(`Closing ${this.stopHandlers.size} streams.`);
    for (let handler of this.stopHandlers) {
      handler();
    }

    logger.info(`Running cleanup.`);

    // Typically closes the server
    await this.cleanupHandler?.();

    // Close the api handlers
    await this.api?.shutdown();
    logger.info('Successfully shut down Router Engine.');
  }

  /**
   * Add a stop handler callback to be executed when the router engine is being
   * shutdown.
   */
  addStopHandler(handler: () => void): () => void {
    if (this.closed) {
      handler();
      return () => {};
    }
    this.stopHandlers.add(handler);
    return () => {
      this.stopHandlers.delete(handler);
    };
  }
}
