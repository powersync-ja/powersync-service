import { container, logger } from '@powersync/lib-services-framework';

import * as api from '../api/api-index.js';

export type RouterSetupResponse = {
  onShutDown: () => Promise<void>;
};

export type RouterSetup = () => Promise<RouterSetupResponse>;

/**
 *  Serves as a registry from which SyncAPIs can be retrieved based on Replication DataSource type
 *  Initially only one SyncAPI per DataSource type is supported
 */
export class RouterEngine {
  closed: boolean;

  protected stopHandlers: Set<() => void>;
  protected routerSetup: RouterSetup | null;

  private api: api.RouteAPI | null;

  constructor() {
    this.api = null;
    this.routerSetup = null;
    this.stopHandlers = new Set();
    this.closed = false;

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

  public register(api: api.RouteAPI) {
    if (this.api) {
      logger.warn('A SyncAPI has already been registered. Overriding existing implementation');
    }

    this.api = api;
  }

  public getAPI(): api.RouteAPI | null {
    return this.api;
  }

  async initialize() {
    if (!this.routerSetup) {
      throw new Error(`Router setup procedure has not been registered`);
    }
    const { onShutDown } = await this.routerSetup();

    // This will cause the router shutdown to occur after the stop handlers have completed
    container.terminationHandler.handleTerminationSignalLast(async () => {
      await onShutDown();
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
