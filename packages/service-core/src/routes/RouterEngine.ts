import { logger } from '@powersync/lib-services-framework';

import * as api from '../api/api-index.js';

import { ADMIN_ROUTES } from './endpoints/admin.js';
import { CHECKPOINT_ROUTES } from './endpoints/checkpointing.js';
import { SYNC_RULES_ROUTES } from './endpoints/sync-rules.js';
import { SYNC_STREAM_ROUTES } from './endpoints/sync-stream.js';
import { RouteDefinition } from './router.js';

export type RouterSetupResponse = {
  onShutdown: () => Promise<void>;
};

export type RouterEngineRoutes = {
  api_routes: RouteDefinition[];
  stream_routes: RouteDefinition[];
  socket_routes: RouteDefinition[];
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

    // Default routes
    this.routes = {
      api_routes: [...ADMIN_ROUTES, ...CHECKPOINT_ROUTES, ...SYNC_RULES_ROUTES],
      stream_routes: [...SYNC_STREAM_ROUTES],
      socket_routes: [
        // TODO
      ]
    };
  }

  public registerAPI(api: api.RouteAPI) {
    if (this.api) {
      logger.warn('A SyncAPI has already been registered. Overriding existing implementation');
    }

    this.api = api;
  }

  public getAPI(): api.RouteAPI | null {
    return this.api;
  }

  /**
   * Starts the router given the configuration provided
   */
  async start(setup: RouterSetup) {
    const { onShutdown } = await setup(this.routes);
    this.cleanupHandler = onShutdown;
  }

  /**
   * Runs all stop handlers then final cleanup.
   */
  async shutdown() {
    // Close open streams, so that they don't block the server from closing.
    // Note: This does not work well when streaming requests are queued. In that case, the server still doesn't
    // close in the 30-second timeout.
    this.closed = true;

    logger.info(`Closing ${this.stopHandlers.size} streams`);
    for (let handler of this.stopHandlers) {
      handler();
    }

    logger.info(`Running close cleanup`);

    // Typically closes the server
    await this.cleanupHandler?.();

    // Close the api handlers
    await this.api?.shutdown();
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
