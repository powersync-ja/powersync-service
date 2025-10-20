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
    // Keepalives are handled by the binlog heartbeat mechanism
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

      if (e instanceof CDCDataExpiredError) {
        // This stops replication and restarts with a new instance
        await this.options.storage.factory.restartReplication(this.storage.group_id);
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
    const connectionManager = this.connectionFactory.create({});
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
    } catch (e) {
      if (this.abortController.signal.aborted) {
        return;
      }
      this.logger.error(`Replication error`, e);
      if (e.cause != null) {
        this.logger.error(`cause`, e.cause);
      }

      if (e instanceof CDCDataExpiredError) {
        throw e;
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

  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.lastStream?.getReplicationLagMillis();
  }
}
