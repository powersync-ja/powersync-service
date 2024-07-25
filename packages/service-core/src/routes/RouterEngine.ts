import { container, logger } from '@powersync/lib-services-framework';

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
  apiRoutes: RouteDefinition[];
  streamRoutes: RouteDefinition[];
  socketRoutes: RouteDefinition[];
};

export type RouterSetup = (routes: RouterEngineRoutes) => Promise<RouterSetupResponse>;

/**
 *  Serves as a registry from which SyncAPIs can be retrieved based on Replication DataSource type
 *  Initially only one SyncAPI per DataSource type is supported
 */
export class RouterEngine {
  closed: boolean;

  /**
   * The reference itself is readonly, but users should eventually
   * be able to add their own route definitions.
   */
  readonly routes: RouterEngineRoutes;

  protected stopHandlers: Set<() => void>;
  private api: api.RouteAPI | null;

  constructor() {
    this.api = null;
    this.stopHandlers = new Set();
    this.closed = false;

    // Default routes
    this.routes = {
      apiRoutes: [...ADMIN_ROUTES, ...CHECKPOINT_ROUTES, ...SYNC_RULES_ROUTES],
      streamRoutes: [...SYNC_STREAM_ROUTES],
      socketRoutes: [
        // TODO
      ]
    };

    /**
     * This adds a termination handler to the begining of the queue
     * A server termination handler should be added to run after this one with
     * `handleTerminationSignalLast`
     */
    container.terminationHandler.handleTerminationSignal(async () => {
      // Close open streams, so that they don't block the server from closing.
      // Note: This does not work well when streaming requests are queued. In that case, the server still doesn't
      // close in the 30-second timeout.
      this.closed = true;
      logger.info(`Closing ${this.stopHandlers.size} streams`);
      for (let handler of this.stopHandlers) {
        handler();
      }
    });
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

    // This will cause the router shutdown to occur after the stop handlers have completed
    container.terminationHandler.handleTerminationSignalLast(async () => {
      await onShutdown();
    });
  }

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
