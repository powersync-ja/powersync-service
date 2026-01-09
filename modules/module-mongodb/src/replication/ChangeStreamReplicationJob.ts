import { container, logger as defaultLogger } from '@powersync/lib-services-framework';
import {
  PersistedSyncRulesContent,
  replication,
  ReplicationLock,
  SyncRulesBucketStorage
} from '@powersync/service-core';

import { ChangeStream, ChangeStreamInvalidatedError } from './ChangeStream.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

export interface ChangeStreamReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConnectionManagerFactory;
  streams: ReplicationStreamConfig[];
}

export interface ReplicationStreamConfig {
  syncRules: PersistedSyncRulesContent;
  storage: SyncRulesBucketStorage;
  lock: ReplicationLock;
}

export class ChangeStreamReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: ConnectionManagerFactory;
  private lastStream: ChangeStream | null = null;

  private readonly streams: ReplicationStreamConfig[];

  constructor(options: ChangeStreamReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.streams = options.streams;
    // We use a custom formatter to process the prefix
    this.logger = defaultLogger.child({
      prefix: `[powersync-${this.streams.map((stream) => stream.syncRules.id).join(',')}] `
    });
  }

  async cleanUp(): Promise<void> {
    // Nothing needed here
  }

  async keepAlive() {
    // Nothing needed here
  }

  isDifferent(syncRules: PersistedSyncRulesContent[]): boolean {
    if (syncRules.length != this.streams.length) {
      return true;
    }

    for (let rules of syncRules) {
      const existing = this.streams.find((stream) => stream.syncRules.id === rules.id);
      if (existing == null) {
        return true;
      }
    }

    return false;
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

        this.rateLimiter.reportError(e);
      }

      if (e instanceof ChangeStreamInvalidatedError) {
        // This stops replication and restarts with a new instance
        // FIXME: check this logic with multiple streams
        for (let { storage } of Object.values(this.streams)) {
          await storage.factory.restartReplication(storage.group_id);
        }
      }

      // No need to rethrow - the error is already logged, and retry behavior is the same on error
    } finally {
      this.abortController.abort();

      for (let { lock } of this.streams) {
        await lock.release();
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
        streams: this.streams,
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
