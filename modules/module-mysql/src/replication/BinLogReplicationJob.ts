import { container, logger as defaultLogger } from '@powersync/lib-services-framework';
import { replication } from '@powersync/service-core';
import { BinlogConfigurationError, BinLogStream } from './BinLogStream.js';
import { MySQLConnectionManagerFactory } from './MySQLConnectionManagerFactory.js';

export interface BinLogReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: MySQLConnectionManagerFactory;
}

export class BinLogReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: MySQLConnectionManagerFactory;
  private lastStream: BinLogStream | null = null;

  constructor(options: BinLogReplicationJobOptions) {
    super(options);
    this.logger = defaultLogger.child({ prefix: `[powersync_${this.options.storage.group_id}] ` });
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
      this.logger.error(`Replication failed`, e);
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

  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.lastStream?.getReplicationLagMillis();
  }
}
