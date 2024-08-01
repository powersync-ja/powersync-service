import * as storage from '../../storage/storage-index.js';

import { container, logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory } from '../../storage/BucketStorage.js';
import { SyncRulesProvider } from '../../util/config/sync-rules/sync-rules-provider.js';
import { ReplicationAdapter } from './ReplicationAdapter.js';
import { ReplicationJob } from './ReplicationJob.js';

export interface ReplicatorOptions {
  adapter: ReplicationAdapter;
  storage: BucketStorageFactory;
  sync_rule_provider: SyncRulesProvider;
}

/**
 *   A replicator manages the mechanics for replicating data from a data source to a storage bucket.
 *   This includes copying across the original data set and then keeping it in sync with the data source using ReplicationJobs.
 *   It also handles any changes to the sync rules.
 */
export class Replicator {
  private readonly adapter: ReplicationAdapter;
  private readonly storage: BucketStorageFactory;

  private replicationJobs = new Map<number, ReplicationJob>();
  private stopped = false;

  constructor(private options: ReplicatorOptions) {
    this.adapter = options.adapter;
    this.storage = options.storage;
  }

  public async start(): Promise<void> {
    this.runLoop().catch((e) => {
      logger.error(`Data source fatal replication error: ${this.adapter.name()}`, e);
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
    const syncRules = await this.options.sync_rule_provider.get();
    let configuredLock: storage.ReplicationLock | undefined = undefined;
    if (syncRules != null) {
      logger.info('Loaded sync rules');
      try {
        // Configure new sync rules, if they have changed.
        // In that case, also immediately take out a lock, so that another process doesn't start replication on it.
        const { lock } = await this.storage.configureSyncRules(syncRules, {
          lock: true
        });
        if (lock) {
          configuredLock = lock;
        }
      } catch (e) {
        // Log, but continue with previous sync rules
        logger.error(`Failed to update sync rules from configuration`, e);
      }
    } else {
      logger.info('No sync rules configured - configure via API');
    }
    while (!this.stopped) {
      await container.probes.touch();
      try {
        await this.refresh({ configured_lock: configuredLock });
        // The lock is only valid on the first refresh.
        configuredLock = undefined;
      } catch (e) {
        logger.error(`Failed to refresh replication jobs: ${this.adapter.name()}`, e);
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  private async refresh(options?: { configured_lock?: storage.ReplicationLock }) {
    if (this.stopped) {
      return;
    }

    let configuredLock = options?.configured_lock;

    const existingJobs = new Map<number, ReplicationJob>(this.replicationJobs.entries());
    const replicatingSyncRules = await this.storage.getReplicatingSyncRules();
    const newJobs = new Map<number, ReplicationJob>();
    for (let syncRules of replicatingSyncRules) {
      const existingJob = existingJobs.get(syncRules.id);
      if (existingJob && !existingJob.isStopped()) {
        // No change
        existingJobs.delete(syncRules.id);
        newJobs.set(syncRules.id, existingJob);
      } else if (existingJob && existingJob.isStopped()) {
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
          const parsed = syncRules.parsed();
          const storage = this.storage.getInstance(parsed);
          const newJob = new ReplicationJob({
            adapter: this.adapter,
            storage: storage,
            lock: lock
          });
          newJobs.set(syncRules.id, newJob);
          newJob.start();
        } catch (e) {
          // Could be a sync rules parse error,
          // for example from stricter validation that was added.
          // This will be retried every couple of seconds.
          // When new (valid) sync rules are deployed and processed, this one be disabled.
          logger.error(`Failed to start replication for ${this.adapter.name()} with new sync rules`, e);
        }
      }
    }

    this.replicationJobs = newJobs;

    // Terminate any orphaned jobs that no longer have sync rules
    for (let job of existingJobs.values()) {
      // Old - stop and clean up
      try {
        await job.terminate();
      } catch (e) {
        // This will be retried
        logger.warn(`Failed to terminate old ${this.adapter.name()} replication job}`, e);
      }
    }

    // Sync rules stopped previously or by a different process.
    const stopped = await this.storage.getStoppedSyncRules();
    for (let syncRules of stopped) {
      try {
        const lock = await syncRules.lock();
        try {
          await this.adapter.cleanupReplication(syncRules.id);
          const parsed = syncRules.parsed();
          const storage = this.storage.getInstance(parsed);
          await storage.terminate();
        } finally {
          await lock.release();
        }
      } catch (e) {
        logger.warn(`Failed to terminate ${syncRules.id}`, e);
      }
    }
  }
}
