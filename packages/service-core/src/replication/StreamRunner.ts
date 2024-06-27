import { container, logger } from '@powersync/lib-services-framework';

import * as storage from '../storage/storage-index.js';
import { ErrorRateLimiter } from './ErrorRateLimiter.js';

export interface StreamRunnerOptions<ConnectionConfig extends {} = {}> {
  factory: storage.BucketStorageFactory;
  storage: storage.SyncRulesBucketStorage;
  lock: storage.ReplicationLock;
  rateLimiter: ErrorRateLimiter;
  config: ConnectionConfig;
}

export abstract class AbstractStreamRunner<ConnectionConfig extends {} = {}> {
  protected abortController = new AbortController();

  protected runPromise?: Promise<void>;

  protected rateLimiter: ErrorRateLimiter;

  constructor(public options: StreamRunnerOptions<ConnectionConfig>) {
    this.rateLimiter = options.rateLimiter;
  }

  start() {
    this.runPromise = this.run();
  }

  get slot_name() {
    return this.options.storage.slot_name;
  }

  get stopped() {
    return this.abortController.signal.aborted;
  }

  protected abstract _ping(): Promise<void>;

  async ping() {
    if (this.rateLimiter.mayPing()) {
      await this._ping();
    }
  }

  protected abstract handleReplicationLoopError(ex: any): Promise<void>;

  async run() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {
          replication_slot: this.slot_name
        }
      });
      logger.error(`Replication failed on ${this.slot_name}`, e);
      await this.handleReplicationLoopError(e);
    } finally {
      this.abortController.abort();
    }
    await this.options.lock.release();
  }

  abstract replicateOnce(): Promise<void>;

  async replicateLoop() {
    while (!this.stopped) {
      await this.replicateOnce();

      if (!this.stopped) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  protected abstract _forceStop(): Promise<void>;

  /**
   * This will also release the lock if start() was called earlier.
   */
  async stop(options?: { force?: boolean }) {
    logger.info(`${this.slot_name} Stopping replication`);
    // End gracefully
    this.abortController.abort();

    if (options?.force) {
      // destroy() is more forceful.
      await this._forceStop();
    }
    await this.runPromise;
  }

  protected abstract _terminate(options?: { force?: boolean }): Promise<void>;

  /**
   * Terminate this replication stream. This drops the replication slot and deletes the replication data.
   *
   * Stops replication if needed.
   */
  async terminate(options?: { force?: boolean }) {
    logger.info(`${this.slot_name} Terminating replication`);
    await this.stop(options);

    await this._terminate(options);

    await this.options.storage.terminate();
  }
}
