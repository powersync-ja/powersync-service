import { container, logger as defaultLogger } from '@powersync/lib-services-framework';
import { replication } from '@powersync/service-core';
import { ConvexConnectionManagerFactory } from './ConvexConnectionManagerFactory.js';
import { ConvexCursorExpiredError, ConvexStream } from './ConvexStream.js';

export interface ConvexReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConvexConnectionManagerFactory;
}

export class ConvexReplicationJob extends replication.AbstractReplicationJob {
  private readonly connectionFactory: ConvexConnectionManagerFactory;
  private lastStream: ConvexStream | null = null;

  constructor(options: ConvexReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.logger = defaultLogger.child({ prefix: `[powersync_${this.options.storage.group_id}] ` });
  }

  async keepAlive() {
    // streaming API is polled continuously; no dedicated keepalive operation required here
  }

  async replicate() {
    try {
      await this.replicateOnce();
    } catch (error) {
      if (!this.isStopped) {
        this.logger.error('Replication error', error);
        if (error?.cause != null) {
          this.logger.error('cause', error.cause);
        }

        container.reporter.captureException(error, {
          metadata: {}
        });

        this.rateLimiter.reportError(error);
      }

      if (error instanceof ConvexCursorExpiredError) {
        await this.options.storage.factory.restartReplication(this.storage.group_id);
      }
    } finally {
      this.abortController.abort();
    }
  }

  async replicateOnce() {
    const manager = this.connectionFactory.create();
    try {
      await this.rateLimiter.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }

      const stream = new ConvexStream({
        abortSignal: this.abortController.signal,
        connections: manager,
        logger: this.logger,
        metrics: this.options.metrics,
        storage: this.options.storage
      });

      this.lastStream = stream;
      await stream.replicate();
    } finally {
      await manager.end();
    }
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.lastStream?.getReplicationLagMillis();
  }
}
