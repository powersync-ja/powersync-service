import { container, logger } from '@powersync/lib-services-framework';
import { hrtime } from 'node:process';
import winston from 'winston';
import * as storage from '../storage/storage-index.js';
import { StorageEngine } from '../storage/storage-index.js';
import { SyncRulesProvider } from '../util/config/sync-rules/sync-rules-provider.js';
import { AbstractReplicationJob } from './AbstractReplicationJob.js';
import { ErrorRateLimiter } from './ErrorRateLimiter.js';
import { ConnectionTestResult } from './ReplicationModule.js';
import { MetricsEngine } from '../metrics/MetricsEngine.js';

// 5 minutes
const PING_INTERVAL = 1_000_000_000n * 300n;

export interface CreateJobOptions {
  lock: storage.ReplicationLock;
  storage: storage.SyncRulesBucketStorage;
}

export interface AbstractReplicatorOptions {
  id: string;
  storageEngine: StorageEngine;
  metricsEngine: MetricsEngine;
  syncRuleProvider: SyncRulesProvider;
  /**
   * This limits the effect of retries when there is a persistent issue.
   */
  rateLimiter: ErrorRateLimiter;
}

/**
 *   A replicator manages the mechanics for replicating data from a data source to a storage bucket.
 *   This includes copying across the original data set and then keeping it in sync with the data source using Replication Jobs.
 *   It also handles any changes to the sync rules.
 */
export abstract class AbstractReplicator<T extends AbstractReplicationJob = AbstractReplicationJob> {
  protected logger: winston.Logger;

  /**
   *  Map of replication jobs by sync rule id. Usually there is only one running job, but there could be two when
   *  transitioning to a new set of sync rules.
   *  @private
   */
  private replicationJobs = new Map<number, T>();
  private stopped = false;

  // First ping is only after 5 minutes, not when starting
  private lastPing = hrtime.bigint();

  protected constructor(private options: AbstractReplicatorOptions) {
    this.logger = logger.child({ name: `Replicator:${options.id}` });
  }

  abstract createJob(options: CreateJobOptions): T;

  /**
   *  Clean up any configuration or state for the specified sync rule on the datasource.
   *  Should be a no-op if the configuration has already been cleared
   */
  abstract cleanUp(syncRuleStorage: storage.SyncRulesBucketStorage): Promise<void>;

  public get id() {
    return this.options.id;
  }

  protected get storage() {
    return this.options.storageEngine.activeBucketStorage;
  }

  protected get syncRuleProvider() {
    return this.options.syncRuleProvider;
  }

  protected get rateLimiter() {
    return this.options.rateLimiter;
  }

  protected get metrics() {
    return this.options.metricsEngine;
  }

  public async start(): Promise<void> {
    this.runLoop().catch((e) => {
      this.logger.error('Data source fatal replication error', e);
      container.reporter.captureException(e);
      setTimeout(() => {
        process.exit(1);
      }, 1000);
    });
  }

  public async stop(): Promise<void> {
    this.stopped = true;
    let promises: Promise<void>[] = [];
    for (const job of this.replicationJobs.values()) {
      promises.push(job.stop());
    }
    await Promise.all(promises);
  }

  private async runLoop() {
    const syncRules = await this.syncRuleProvider.get();

    let configuredLock: storage.ReplicationLock | undefined = undefined;
    if (syncRules != null) {
      this.logger.info('Loaded sync rules');
      try {
        // Configure new sync rules, if they have changed.
        // In that case, also immediately take out a lock, so that another process doesn't start replication on it.

        const { lock } = await this.storage.configureSyncRules({
          content: syncRules,
          lock: true,
          validate: this.syncRuleProvider.exitOnError
        });
        if (lock) {
          configuredLock = lock;
        }
      } catch (e) {
        // Log and re-raise to exit.
        // Should only reach this due to validation errors if exit_on_error is true.
        this.logger.error(`Failed to update sync rules from configuration`, e);
        throw e;
      }
    } else {
      this.logger.info('No sync rules configured - configure via API');
    }
    while (!this.stopped) {
      await container.probes.touch();
      try {
        await this.refresh({ configured_lock: configuredLock });
        // The lock is only valid on the first refresh.
        configuredLock = undefined;

        // Ensure that the replication jobs' connections are kept alive.
        // We don't ping while in error retry back-off, to avoid having too failures.
        if (this.rateLimiter.mayPing()) {
          const now = hrtime.bigint();
          if (now - this.lastPing >= PING_INTERVAL) {
            for (const activeJob of this.replicationJobs.values()) {
              await activeJob.keepAlive();
            }

            this.lastPing = now;
          }
        }
      } catch (e) {
        this.logger.error('Failed to refresh replication jobs', e);
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  private async refresh(options?: { configured_lock?: storage.ReplicationLock }) {
    if (this.stopped) {
      return;
    }

    let configuredLock = options?.configured_lock;

    const existingJobs = new Map<number, T>(this.replicationJobs.entries());
    const replicatingSyncRules = await this.storage.getReplicatingSyncRules();
    const newJobs = new Map<number, T>();
    for (let syncRules of replicatingSyncRules) {
      const existingJob = existingJobs.get(syncRules.id);
      if (existingJob && !existingJob.isStopped) {
        // No change
        existingJobs.delete(syncRules.id);
        newJobs.set(syncRules.id, existingJob);
      } else if (existingJob && existingJob.isStopped) {
        // Stopped (e.g. fatal error).
        // Remove from the list. Next refresh call will restart the job.
        existingJobs.delete(syncRules.id);
      } else {
        // New sync rules were found (or resume after restart)
        try {
          let lock: storage.ReplicationLock;
          if (configuredLock?.sync_rules_id == syncRules.id) {
            lock = configuredLock;
          } else {
            lock = await syncRules.lock();
          }
          const storage = this.storage.getInstance(syncRules);
          const newJob = this.createJob({
            lock: lock,
            storage: storage
          });

          newJobs.set(syncRules.id, newJob);
          newJob.start();
        } catch (e) {
          // Could be a sync rules parse error,
          // for example from stricter validation that was added.
          // This will be retried every couple of seconds.
          // When new (valid) sync rules are deployed and processed, this one be disabled.
          this.logger.error('Failed to start replication for new sync rules', e);
        }
      }
    }

    this.replicationJobs = newJobs;

    // Terminate any orphaned jobs that no longer have sync rules
    for (let job of existingJobs.values()) {
      // Old - stop and clean up
      try {
        await job.stop();
        await this.terminateSyncRules(job.storage);
        job.storage[Symbol.dispose]();
      } catch (e) {
        // This will be retried
        this.logger.warn('Failed to terminate old replication job}', e);
      }
    }

    // Sync rules stopped previously or by a different process.
    const stopped = await this.storage.getStoppedSyncRules();
    for (let syncRules of stopped) {
      try {
        using syncRuleStorage = this.storage.getInstance(syncRules);
        await this.terminateSyncRules(syncRuleStorage);
      } catch (e) {
        this.logger.warn(`Failed clean up replication config for sync rule: ${syncRules.id}`, e);
      }
    }
  }

  protected createJobId(syncRuleId: number) {
    return `${this.id}-${syncRuleId}`;
  }

  protected async terminateSyncRules(syncRuleStorage: storage.SyncRulesBucketStorage) {
    this.logger.info(`Terminating sync rules: ${syncRuleStorage.group_id}...`);
    try {
      await this.cleanUp(syncRuleStorage);
      await syncRuleStorage.terminate();
      this.logger.info(`Successfully terminated sync rules: ${syncRuleStorage.group_id}`);
    } catch (e) {
      this.logger.warn(`Failed clean up replication config for sync rules: ${syncRuleStorage.group_id}`, e);
    }
  }

  abstract testConnection(): Promise<ConnectionTestResult>;
}
