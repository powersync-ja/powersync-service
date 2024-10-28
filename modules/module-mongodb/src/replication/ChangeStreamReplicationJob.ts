import { container } from '@powersync/lib-services-framework';
import { MissingReplicationSlotError, ChangeStream } from './ChangeStream.js';

import { replication } from '@powersync/service-core';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

import * as mongo from 'mongodb';

export interface ChangeStreamReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class ChangeStreamReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: ConnectionManagerFactory;

  constructor(options: ChangeStreamReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  async cleanUp(): Promise<void> {
    // TODO: Implement?
  }

  async keepAlive() {
    // TODO: Implement?
  }

  private get slotName() {
    return this.options.storage.slot_name;
  }

  async replicate() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {}
      });
      this.logger.error(`Replication failed`, e);

      if (e instanceof MissingReplicationSlotError) {
        // This stops replication on this slot, and creates a new slot
        await this.options.storage.factory.slotRemoved(this.slotName);
      }
    } finally {
      this.abortController.abort();
    }
  }

  async replicateLoop() {
    while (!this.isStopped) {
      await this.replicateOnce();

      if (!this.isStopped) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async replicateOnce() {
    // New connections on every iteration (every error with retry),
    // otherwise we risk repeating errors related to the connection,
    // such as caused by cached PG schemas.
    const connectionManager = this.connectionFactory.create();
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new ChangeStream({
        abort_signal: this.abortController.signal,
        storage: this.options.storage,
        connections: connectionManager
      });
      await stream.replicate();
    } catch (e) {
      if (this.abortController.signal.aborted) {
        return;
      }
      this.logger.error(`Replication error`, e);
      if (e.cause != null) {
        // Without this additional log, the cause may not be visible in the logs.
        this.logger.error(`cause`, e.cause);
      }
      if (e instanceof mongo.MongoError && e.hasErrorLabel('NonResumableChangeStreamError')) {
        throw new MissingReplicationSlotError(e.message);
      } else {
        // Report the error if relevant, before retrying
        container.reporter.captureException(e, {
          metadata: {}
        });
        // This sets the retry delay
        this.rateLimiter?.reportError(e);
      }
    } finally {
      await connectionManager.end();
    }
  }
}
