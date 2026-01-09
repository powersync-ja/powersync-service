import { storage, replication } from '@powersync/service-core';
import { ChangeStreamReplicationJob, ReplicationStreamConfig } from './ChangeStreamReplicationJob.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { MongoErrorRateLimiter } from './MongoErrorRateLimiter.js';
import { MongoModule } from '../module/MongoModule.js';
import { MongoLSN } from '../common/MongoLSN.js';
import { timestampToDate } from './replication-utils.js';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';

export interface ChangeStreamReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class ChangeStreamReplicator extends replication.AbstractReplicator<ChangeStreamReplicationJob> {
  private readonly connectionFactory: ConnectionManagerFactory;
  private job: ChangeStreamReplicationJob | null = null;

  constructor(options: ChangeStreamReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  createJob(options: replication.CreateJobOptions): ChangeStreamReplicationJob {
    throw new ReplicationAssertionError('Not implemented here');
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    // TODO: Implement anything?
  }

  async refresh(options?: { configured_lock?: storage.ReplicationLock }) {
    if (this.stopped) {
      return;
    }

    let configuredLock = options?.configured_lock;

    const replicatingSyncRules = await this.storage.getReplicatingSyncRules();

    if (this.job?.isStopped) {
      this.job = null;
    }

    if (this.job != null && !this.job?.isDifferent(replicatingSyncRules)) {
      // No changes
      return;
    }

    // Stop existing job, if any
    await this.job?.stop();
    this.job = null;
    if (replicatingSyncRules.length === 0) {
      // No active replication
      return;
    }

    let streamConfig: ReplicationStreamConfig[] = [];
    try {
      for (let rules of replicatingSyncRules) {
        let lock: storage.ReplicationLock;
        if (configuredLock?.sync_rules_id == rules.id) {
          lock = configuredLock;
        } else {
          lock = await rules.lock();
        }
        streamConfig.push({ lock, syncRules: rules, storage: this.storage.getInstance(rules) });
      }
    } catch (e) {
      // Release any acquired locks
      for (let { lock } of streamConfig) {
        try {
          await lock.release();
        } catch (ex) {
          this.logger.warn('Failed to release replication lock after acquisition failure', ex);
        }
      }
      throw e;
    }

    const newJob = new ChangeStreamReplicationJob({
      id: this.createJobId(replicatingSyncRules[0].id), // FIXME: check the id
      storage: streamConfig[0].storage, // FIXME: multi-stream logic
      lock: streamConfig[0].lock, // FIXME: multi-stream logic
      streams: streamConfig,
      metrics: this.metrics,
      connectionFactory: this.connectionFactory,
      rateLimiter: new MongoErrorRateLimiter()
    });
    this.job = newJob;
    await newJob.start();
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.job?.stop();
    await this.connectionFactory.shutdown();
  }

  async testConnection() {
    return await MongoModule.testConnection(this.connectionFactory.dbConnectionConfig);
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    const lag = await super.getReplicationLagMillis();
    if (lag != null) {
      return lag;
    }

    // Booting or in an error loop. Check last active replication status.
    // This includes sync rules in an ERROR state.
    const content = await this.storage.getActiveSyncRulesContent();
    if (content == null) {
      return undefined;
    }
    // Measure the lag from the last resume token's time
    const lsn = content.last_checkpoint_lsn;
    if (lsn == null) {
      return undefined;
    }
    const { timestamp } = MongoLSN.fromSerialized(lsn);
    return Date.now() - timestampToDate(timestamp).getTime();
  }
}
