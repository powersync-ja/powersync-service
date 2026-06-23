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
 *   It also handles any changes to the sync config.
 */
export abstract class AbstractReplicator<T extends AbstractReplicationJob = AbstractReplicationJob> {
  protected logger: winston.Logger;
  private lockAlerted: boolean = false;
  /**
   *  Map of replication jobs by replication stream job id. Usually there is only one running job, but there could be two
   *  when transitioning to a new replication stream.
   */
  private replicationJobs = new Map<string, T>();

  /**
   * Map of replciation stream ids to promises that are clearing the replication stream.
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
  private acceptingReplicationJobs = true;
  private completedResolver = Promise.withResolvers<void>();
  public completed: Promise<void> = this.completedResolver.promise;

  protected constructor(private options: AbstractReplicatorOptions) {
    this.logger = logger.child({ name: `Replicator:${options.id}` });
  }

  abstract createJob(options: CreateJobOptions): T;

  /**
   * Clean up any configuration or state for the specified replication stream on the datasource.
   * Should be a no-op if the replication stream has already been cleared
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

  public start(): void {
    this.abortController = new AbortController();
    this.completedResolver = Promise.withResolvers<void>();
    this.completed = this.completedResolver.promise;
    this.runLoop()
      .then(() => {
        this.completedResolver.resolve();
      })
      .catch((e) => {
        this.logger.error('Fatal replication error', e);
        container.reporter.captureException(e);
        this.completedResolver.reject(e);
        setTimeout(() => {
          process.exit(1);
        }, 1000);
      });
    this.metrics.getObservableGauge(ReplicationMetric.REPLICATION_LAG_SECONDS).setValueProvider(async () => {
      try {
        const lag = this.getReplicationLagMillis();
        if (lag == null) {
          return undefined;
        }
        // ms to seconds
        return Math.round(lag / 1000);
      } catch (e) {
        this.logger.error('Failed to get replication lag', e);
        return undefined;
      }
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

  public stopAcceptingReplicationJobs(): void {
    if (!this.acceptingReplicationJobs) {
      return;
    }

    this.acceptingReplicationJobs = false;
  }

  private async runLoop() {
    const syncRules = await this.syncRuleProvider.get();

    let configuredLock: storage.ReplicationLock | undefined = undefined;
    if (syncRules != null) {
      this.logger.info('Loaded sync config');
      try {
        // Configure new sync config, if they have changed.
        // In that case, also immediately take out a lock, so that another process doesn't start replication on it.

        const { lock } = await this.storage.configureSyncRules(
          storage.updateSyncRulesFromYaml(syncRules, { lock: true, validate: this.syncRuleProvider.exitOnError })
        );
        if (lock) {
          configuredLock = lock;
        }
      } catch (e) {
        // Log and re-raise to exit.
        // Should only reach this due to validation errors if exit_on_error is true.
        this.logger.error(`Failed to update sync config`, e);
        throw e;
      }
    } else {
      this.logger.info('No sync streams or rules configured - configure via API');
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

  protected async refresh(options?: { configured_lock?: storage.ReplicationLock }) {
    if (this.stopped) {
      return;
    }

    let configuredLock = options?.configured_lock;

    const existingJobs = new Map<string, T>(this.replicationJobs.entries());
    const replicatingStreams = await this.storage.getReplicatingReplicationStreams();
    const newJobs = new Map<string, T>();
    let activeJob: T | undefined = undefined;
    for (let replicationStream of replicatingStreams) {
      const jobId = replicationStream.replicationJobId;
      const existingJob = existingJobs.get(jobId);
      if (replicationStream.state == storage.SyncRuleState.ACTIVE && activeJob == null) {
        activeJob = existingJob;
      }
      if (existingJob && !existingJob.isStopped) {
        // No change
        existingJobs.delete(jobId);
        newJobs.set(jobId, existingJob);
      } else if (existingJob && existingJob.isStopped) {
        // Stopped (e.g. fatal error).
        // Remove from the list. Next refresh call will restart the job.
        existingJobs.delete(jobId);
      } else {
        // New sync config was found (or resume after restart)
        try {
          if (!this.acceptingReplicationJobs) {
            if (configuredLock?.sync_rules_id == replicationStream.replicationStreamId) {
              await configuredLock.release();
              configuredLock = undefined;
            }
            continue;
          }

          let lock: storage.ReplicationLock;
          if (configuredLock?.sync_rules_id == replicationStream.replicationStreamId) {
            lock = configuredLock;
          } else {
            lock = await replicationStream.lock();
          }
          const syncRuleStorage = this.storage.getInstance(replicationStream);
          const newJob = this.createJob({
            lock: lock,
            storage: syncRuleStorage
          });

          newJobs.set(jobId, newJob);
          newJob.start();
          if (replicationStream.state == storage.SyncRuleState.ACTIVE) {
            activeJob = newJob;
          }
          this.lockAlerted = false;
        } catch (e) {
          if (e?.errorData?.code === ErrorCode.PSYNC_S1003) {
            if (!this.lockAlerted) {
              replicationStream.logger.info(`[${e.errorData.code}] ${e.errorData.description}`);
              this.lockAlerted = true;
            }
          } else {
            // Could be a sync config parse error,
            // for example from stricter validation that was added.
            // This will be retried every couple of seconds.
            // When new (valid) sync config is deployed and processed, this one be disabled.
            replicationStream.logger.error('Failed to start replication for new sync config', e);
          }
        }
      }
    }

    this.replicationJobs = newJobs;
    this.activeReplicationJob = activeJob;

    // Stop any orphaned jobs that no longer have a replication stream.
    // Termination happens below
    for (let job of existingJobs.values()) {
      // Old - stop and clean up
      try {
        await job.stop();
      } catch (e) {
        // This will be retried
        job.storage.logger.warn('Failed to stop old replication job', e);
      }
    }

    if (!this.acceptingReplicationJobs && this.replicationJobs.size == 0) {
      this.logger.info('No replication jobs remain; stopping replicator');
      this.abortController?.abort();
      return;
    }

    // Replication stream stopped previously, including by a different process.
    const stopped = await this.storage.getStoppedReplicationStreams();
    for (let replicationStream of stopped) {
      if (this.clearingJobs.has(replicationStream.replicationStreamId)) {
        // Already in progress
        continue;
      }

      // We clear storage asynchronously.
      // It is important to be able to continue running the refresh loop, otherwise we cannot
      // retry locked replication stream, for example.
      const syncRuleStorage = this.storage.getInstance(replicationStream, { skipLifecycleHooks: true });
      const promise = this.terminateSyncRules(syncRuleStorage)
        .catch((e) => {
          syncRuleStorage.logger.warn(`Failed clean up replication config`, e);
        })
        .finally(() => {
          this.clearingJobs.delete(replicationStream.replicationStreamId);
        });
      this.clearingJobs.set(replicationStream.replicationStreamId, promise);
    }
  }

  protected createJobId(syncRuleId: number) {
    return `${this.id}-${syncRuleId}`;
  }

  protected async terminateSyncRules(syncRuleStorage: storage.SyncRulesBucketStorage) {
    syncRuleStorage.logger.info(`Terminating replication stream...`);
    // This deletes postgres replication slots - should complete quickly.
    // It is safe to do before or after clearing the data in the storage.
    await this.cleanUp(syncRuleStorage);
    await syncRuleStorage.terminate({ signal: this.abortController?.signal, clearStorage: true });
    syncRuleStorage.logger.info(`Successfully terminated replication stream`);
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
  getReplicationLagMillis(): number | undefined {
    return this.activeReplicationJob?.getReplicationLagMillis();
  }
}
