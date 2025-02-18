import { container } from '@powersync/lib-services-framework';
import { replication } from '@powersync/service-core';
import { BinlogConfigurationError, BinLogStream } from './BinLogStream.js';
import { MySQLConnectionManagerFactory } from './MySQLConnectionManagerFactory.js';

export interface BinLogReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: MySQLConnectionManagerFactory;
}

export class BinLogReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: MySQLConnectionManagerFactory;

  constructor(options: BinLogReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  get slot_name() {
    return this.options.storage.slot_name;
  }

  async keepAlive() {}

  async replicate() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {
          replication_slot: this.slot_name
        }
      });
      this.logger.error(`Replication failed on ${this.slot_name}`, e);
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
    const connectionManager = this.connectionFactory.create({
      // Pool connections are only used intermittently.
      idleTimeout: 30_000
    });
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new BinLogStream({
        abortSignal: this.abortController.signal,
        storage: this.options.storage,
        connections: connectionManager
      });
      await stream.replicate();
    } catch (e) {
      if (this.abortController.signal.aborted) {
        return;
      }
      this.logger.error(`Sync rules ${this.id} Replication error`, e);
      if (e.cause != null) {
        this.logger.error(`cause`, e.cause);
      }

      if (e instanceof BinlogConfigurationError) {
        throw e;
      } else {
        // Report the error if relevant, before retrying
        container.reporter.captureException(e, {
          metadata: {
            replication_slot: this.slot_name
          }
        });
        // This sets the retry delay
        this.rateLimiter?.reportError(e);
      }
    } finally {
      await connectionManager.end();
    }
  }
}
