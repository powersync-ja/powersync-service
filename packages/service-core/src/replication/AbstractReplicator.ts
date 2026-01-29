import { container, ErrorCode, logger } from '@powersync/lib-services-framework';
import { ReplicationMetric } from '@powersync/service-types';
import { hrtime } from 'node:process';
import winston from 'winston';
import { MetricsEngine } from '../metrics/MetricsEngine.js';
import * as storage from '../storage/storage-index.js';
import { StorageEngine } from '../storage/storage-index.js';
import { SyncRulesProvider } from '../util/config/sync-rules/sync-rules-provider.js';
import { AbstractReplicationJob } from './AbstractReplicationJob.js';
import { ErrorRateLimiter } from './ErrorRateLimiter.js';
import { ConnectionTestResult } from './ReplicationModule.js';

// 1 minute
const PING_INTERVAL = 1_000_000_000n * 60n;

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
  private lockAlerted: boolean = false;
  /**
   *  Map of replication jobs by sync rule id. Usually there is only one running job, but there could be two when
   *  transitioning to a new set of sync rules.
   */
  private replicationJobs = new Map<number, T>();

  /**
   * Map of sync rule ids to promises that are clearing the sync rule configuration.
   *
   * We primarily do this to keep track of what we're currently clearing, but don't currently
   * use the Promise value.
   */
  private clearingJobs = new Map<number, Promise<void>>();

  /**
   * Used for replication lag computation.
   */
  private activeReplicationJob: T | undefined = undefined;

  // First ping is only after 5 minutes, not when starting
  private lastPing = hrtime.bigint();

  private abortController: AbortController | undefined;

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

  protected get stopped() {
    return this.abortController?.signal.aborted;
  }

  public async start(): Promise<void> {
    this.abortController = new AbortController();
    this.runLoop().catch((e) => {
      this.logger.error('Data source fatal replication error', e);
      container.reporter.captureException(e);
      setTimeout(() => {
        process.exit(1);
      }, 1000);
    });
    this.metrics.getObservableGauge(ReplicationMetric.REPLICATION_LAG_SECONDS).setValueProvider(async () => {
      const lag = await this.getReplicationLagMillis().catch((e) => {
        this.logger.error('Failed to get replication lag', e);
        return undefined;
      });
      if (lag == null) {
        return undefined;
      }
      // ms to seconds
      return Math.round(lag / 1000);
    });
  }

  public async stop(): Promise<void> {
    this.abortController?.abort();
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
    let activeJob: T | undefined = undefined;
    for (let syncRules of replicatingSyncRules) {
      const existingJob = existingJobs.get(syncRules.id);
      if (syncRules.active && activeJob == null) {
        activeJob = existingJob;
      }
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
          if (syncRules.active) {
            activeJob = newJob;
          }
          this.lockAlerted = false;
        } catch (e) {
          if (e?.errorData?.code === ErrorCode.PSYNC_S1003) {
            if (!this.lockAlerted) {
              this.logger.info(`[${e.errorData.code}] ${e.errorData.description}`);
              this.lockAlerted = true;
            }
          } else {
            // Could be a sync rules parse error,
            // for example from stricter validation that was added.
            // This will be retried every couple of seconds.
            // When new (valid) sync rules are deployed and processed, this one be disabled.
            this.logger.error('Failed to start replication for new sync rules', e);
          }
        }
      }
    }

    this.replicationJobs = newJobs;
    this.activeReplicationJob = activeJob;

    // Stop any orphaned jobs that no longer have sync rules.
    // Termination happens below
    for (let job of existingJobs.values()) {
      // Old - stop and clean up
      try {
        await job.stop();
      } catch (e) {
        // This will be retried
        this.logger.warn('Failed to stop old replication job}', e);
      }
    }

    // Sync rules stopped previously, including by a different process.
    const stopped = await this.storage.getStoppedSyncRules();
    for (let syncRules of stopped) {
      if (this.clearingJobs.has(syncRules.id)) {
        // Already in progress
        continue;
      }

      // We clear storage asynchronously.
      // It is important to be able to continue running the refresh loop, otherwise we cannot
      // retry locked sync rules, for example.
      const syncRuleStorage = this.storage.getInstance(syncRules, { skipLifecycleHooks: true });
      const promise = this.terminateSyncRules(syncRuleStorage)
        .catch((e) => {
          this.logger.warn(`Failed clean up replication config for sync rule: ${syncRules.id}`, e);
        })
        .finally(() => {
          this.clearingJobs.delete(syncRules.id);
        });
      this.clearingJobs.set(syncRules.id, promise);
    }
  }

  protected createJobId(syncRuleId: number) {
    return `${this.id}-${syncRuleId}`;
  }

  protected async terminateSyncRules(syncRuleStorage: storage.SyncRulesBucketStorage) {
    this.logger.info(`Terminating sync rules: ${syncRuleStorage.group_id}...`);
    // This deletes postgres replication slots - should complete quickly.
    // It is safe to do before or after clearing the data in the storage.
    await this.cleanUp(syncRuleStorage);
    await syncRuleStorage.terminate({ signal: this.abortController?.signal, clearStorage: true });
    this.logger.info(`Successfully terminated sync rules: ${syncRuleStorage.group_id}`);
  }

  abstract testConnection(): Promise<ConnectionTestResult>;

  /**
   * Measure replication lag in milliseconds.
   *
   * In general, this is the difference between now() and the time the oldest record, that we haven't committed yet,
   * has been written (committed) to the source database.
   *
   * This is roughly a measure of the _average_ amount of time we're behind.
   * If we get a new change as soon as each previous one has finished processing, and each change takes 1000ms
   * to process, the average replication lag will be 500ms, not 1000ms.
   *
   * 1. When we are actively replicating, this is the difference between now and when the time the change was
   *    written to the source database.
   * 2. When the replication stream is idle, this is either 0, or the delay for keepalive messages to make it to us.
   * 3. When the active replication stream is an error state, this is the time since the last successful commit.
   * 4. If there is no active replication stream, this is undefined.
   *
   * "processing" replication streams are not taken into account for this metric.
   */
  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.activeReplicationJob?.getReplicationLagMillis();
  }
}
