import * as pgwire from '@powersync/service-jpgwire';
import { LifeCycledSystem, LifeCycledSystemOptions, logger } from '@powersync/service-framework';

import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';

export abstract class CorePowerSyncSystem extends LifeCycledSystem {
  abstract storage: storage.BucketStorageFactory;
  abstract pgwire_pool?: pgwire.PgClient;

  protected stopHandlers: Set<() => void> = new Set();

  closed: boolean;

  constructor(public config: utils.ResolvedPowerSyncConfig, options?: LifeCycledSystemOptions) {
    super(options);
    this.closed = false;
  }

  get client_keystore() {
    return this.config.client_keystore;
  }

  get dev_client_keystore() {
    return this.config.dev_client_keystore;
  }

  /**
   * Adds a termination handler which will call handlers registered via
   * [addStopHandler].
   * This should be called after the server is started and it's termination handler is added.
   * This is so that the handler is run before the server's handler, allowing streams to be interrupted on exit
   *
   * TODO this could be improved once router terminations are handled
   */
  addTerminationHandler() {
    this.terminationHandler.handleTerminationSignal(async () => {
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

  requirePgPool() {
    if (this.pgwire_pool == null) {
      throw new Error('No source connection configured');
    } else {
      return this.pgwire_pool!;
    }
  }
}
