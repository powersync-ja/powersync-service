import { container, logger } from '@powersync/lib-services-framework';
import winston from 'winston';
import * as storage from '../storage/storage-index.js';
import { ErrorRateLimiter } from './ErrorRateLimiter.js';

export interface AbstractReplicationJobOptions {
  id: string;
  storage: storage.SyncRulesBucketStorage;
  lock: storage.ReplicationLock;
  rateLimiter?: ErrorRateLimiter;
}

export abstract class AbstractReplicationJob {
  protected logger: winston.Logger;
  protected abortController = new AbortController();
  protected isReplicatingPromise: Promise<void> | null = null;

  protected constructor(protected options: AbstractReplicationJobOptions) {
    this.logger = logger.child({ name: `ReplicationJob: ${options.id}` });
  }

  /**
   *  Copy the initial data set from the data source if required and then keep it in sync.
   */
  abstract replicate(): Promise<void>;

  /**
   *  Ensure the connection to the data source remains active
   */
  abstract keepAlive(): Promise<void>;

  /**
   *  Clean up any configuration or state for this replication on the datasource.
   *  This assumes that the replication is not currently active.
   */
  abstract cleanUp(): Promise<void>;

  /**
   *  Start the replication process
   */
  public async start(): Promise<void> {
    this.isReplicatingPromise = this.replicate()
      .catch((ex) => {
        container.reporter.captureException(ex, {
          metadata: {
            replicator: this.id
          }
        });
        logger.error(`Replication failed.`, ex);
      })
      .finally(async () => {
        this.abortController.abort();
        await this.options.lock.release();
      });
  }

  /**
   *  Safely stop the replication process
   */
  public async stop(): Promise<void> {
    logger.info(`Stopping ${this.id} replication job for sync rule iteration: ${this.storage.group_id}`);
    this.abortController.abort();
    await this.isReplicatingPromise;
  }

  /**
   *  Stop the replication if it is still running.
   *  Clean up any config on the datasource related to this replication job
   */
  public async terminate(): Promise<void> {
    logger.info(`${this.id} Terminating replication`);
    await this.stop();
    await this.cleanUp();
    await this.options.storage.terminate();
  }

  public get id() {
    return this.options.id;
  }

  protected get storage() {
    return this.options.storage;
  }

  protected get lock() {
    return this.options.lock;
  }

  protected get rateLimiter() {
    return this.options.rateLimiter;
  }

  public get isStopped(): boolean {
    return this.abortController.signal.aborted;
  }
}
