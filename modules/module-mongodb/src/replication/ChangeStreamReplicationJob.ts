import { container, logger as defaultLogger } from '@powersync/lib-services-framework';
import { replication } from '@powersync/service-core';

import { ChangeStream, ChangeStreamInvalidatedError } from './ChangeStream.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

export interface ChangeStreamReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class ChangeStreamReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: ConnectionManagerFactory;
  private lastStream: ChangeStream | null = null;

  constructor(options: ChangeStreamReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    // We use a custom formatter to process the prefix
    this.logger = defaultLogger.child({ prefix: `[powersync_${this.storage.group_id}] ` });
  }

  async cleanUp(): Promise<void> {
    // Nothing needed here
  }

  async keepAlive() {
    // Nothing needed here
  }

  async replicate() {
    try {
      await this.replicateOnce();
    } catch (e) {
      if (!this.abortController.signal.aborted) {
        container.reporter.captureException(e, {
          metadata: {}
        });

        this.logger.error(`Replication error`, e);
        if (e.cause != null) {
          // Without this additional log, the cause may not be visible in the logs.
          this.logger.error(`cause`, e.cause);
        }
      }

      if (e instanceof ChangeStreamInvalidatedError) {
        // This stops replication and restarts with a new instance
        await this.options.storage.factory.restartReplication(this.storage.group_id);
      }
    } finally {
      this.abortController.abort();
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
        metrics: this.options.metrics,
        connections: connectionManager,
        logger: this.logger
      });
      this.lastStream = stream;
      await stream.replicate();
    } finally {
      await connectionManager.end();
    }
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.lastStream?.getReplicationLagMillis();
  }
}
