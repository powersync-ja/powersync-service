import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import mysql from 'mysql2/promise';
import { NormalizedMySQLConnectionConfig } from '../types/types.js';
import { MysqlBinLogStream } from './MysqlBinLogStream.js';

export interface BinLogStreamRunnerOptions {
  factory: storage.BucketStorageFactory;
  storage: storage.SyncRulesBucketStorage;
  source_db: mysql.Pool;
  lock: storage.ReplicationLock;
  connection_config: NormalizedMySQLConnectionConfig;
}

export class MysqlBinLogStreamRunner {
  private abortController = new AbortController();

  private runPromise?: Promise<void>;

  constructor(public options: BinLogStreamRunnerOptions) {}

  start() {
    this.runPromise = this.run();
  }

  get slot_name() {
    return this.options.storage.slot_name;
  }

  get stopped() {
    return this.abortController.signal.aborted;
  }

  async run() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      // if (e instanceof MissingReplicationSlotError) {
      // This stops replication on this slot, and creates a new slot
      // await this.options.storage.factory.slotRemoved(this.slot_name);
      // }
    } finally {
      this.abortController.abort();
    }
    await this.options.lock.release();
  }

  async replicateLoop() {
    while (!this.stopped) {
      await this.replicateOnce();

      if (!this.stopped) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async replicateOnce() {
    // New connections on every iteration (every error with retry),
    // otherwise we risk repeating errors related to the connection,
    // such as caused by cached PG schemas.
    try {
      // await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.stopped) {
        return;
      }
      const stream = new MysqlBinLogStream({
        abort_signal: this.abortController.signal,
        connection_config: this.options.connection_config,
        factory: this.options.factory,
        storage: this.options.storage,
        pool: this.options.source_db
      });
      await stream.replicate();
    } catch (e) {
      logger.error(`Replication error`, e);
      if (e.cause != null) {
        logger.error(`cause`, e.cause);
      }
    }
  }

  /**
   * This will also release the lock if start() was called earlier.
   */
  async stop(options?: { force?: boolean }) {
    logger.info(`${this.slot_name} Stopping replication`);
    // End gracefully
    this.abortController.abort();

    if (options?.force) {
      // destroy() is more forceful.
      this.options.source_db?.destroy();
    }
    await this.runPromise;
  }

  /**
   * Terminate this replication stream. This drops the replication slot and deletes the replication data.
   *
   * Stops replication if needed.
   */
  async terminate(options?: { force?: boolean }) {
    logger.info(`${this.slot_name} Terminating replication`);
    // if (!isNormalizedPostgresConnection(this.options.source_db)) {
    // throw new Error('Replication only supported for normalized Postgres connections');
    // }
    await this.stop(options);
    await this.options.source_db.end();
    await this.options.storage.terminate();
  }
}
