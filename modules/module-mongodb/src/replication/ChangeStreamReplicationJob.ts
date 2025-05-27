import { isMongoServerError } from '@powersync/lib-service-mongodb';
import { container } from '@powersync/lib-services-framework';
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

      if (e instanceof ChangeStreamInvalidatedError) {
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
        connections: connectionManager
      });
      this.lastStream = stream;
      await stream.replicate();
    } catch (e) {
      if (this.abortController.signal.aborted) {
        return;
      }
      this.logger.error(`${this.slotName} Replication error`, e);
      if (e.cause != null) {
        // Without this additional log, the cause may not be visible in the logs.
        this.logger.error(`cause`, e.cause);
      }
      if (e instanceof ChangeStreamInvalidatedError) {
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

  async getReplicationLag(): Promise<number | undefined> {
    const lag = await this.lastStream?.getReplicationLag();
    if (lag == null) {
      // Booting; potentially in an error loop. Check last replication status.
      const content = await this.storage.factory.getActiveSyncRulesContent();
      if (content == null) {
        return undefined;
      }
      if (content.id == this.storage.group_id) {
        // Technically this is the time we wrote to our storage, not the source db time.
        // This is the best we can currently do while not actively replicating.
        const checkpointTs = content?.last_checkpoint_ts?.getTime() ?? 0;
        const keepaliveTs = content?.last_keepalive_ts?.getTime() ?? 0;
        const latestTs = Math.max(checkpointTs, keepaliveTs);
        if (latestTs != 0) {
          return Date.now() - latestTs;
        }
      }
    }
    return lag;
  }
}
