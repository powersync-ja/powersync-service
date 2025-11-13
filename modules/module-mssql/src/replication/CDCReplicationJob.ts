import { replication } from '@powersync/service-core';
import { MSSQLConnectionManagerFactory } from './MSSQLConnectionManagerFactory.js';
import { container, logger as defaultLogger } from '@powersync/lib-services-framework';
import { CDCDataExpiredError, CDCStream } from './CDCStream.js';

export interface CDCReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: MSSQLConnectionManagerFactory;
}

export class CDCReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: MSSQLConnectionManagerFactory;
  private lastStream: CDCStream | null = null;

  constructor(options: CDCReplicationJobOptions) {
    super(options);
    this.logger = defaultLogger.child({ prefix: `[powersync_${this.options.storage.group_id}] ` });
    this.connectionFactory = options.connectionFactory;
  }

  async keepAlive() {
    // TODO Might need to leverage checkpoints table as a keepAlive
  }

  async replicate() {
    try {
      await this.replicateOnce();
    } catch (e) {
      // Fatal exception
      if (!this.isStopped) {
        // Ignore aborted errors
        this.logger.error(`Replication error`, e);
        if (e.cause != null) {
          this.logger.error(`cause`, e.cause);
        }

        container.reporter.captureException(e, {
          metadata: {}
        });

        // This sets the retry delay
        this.rateLimiter.reportError(e);
      }
      if (e instanceof CDCDataExpiredError) {
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
    const connectionManager = this.connectionFactory.create({
      idleTimeoutMillis: 30_000,
      max: 2
    });
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new CDCStream({
        logger: this.logger,
        abortSignal: this.abortController.signal,
        storage: this.options.storage,
        metrics: this.options.metrics,
        connections: connectionManager
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
