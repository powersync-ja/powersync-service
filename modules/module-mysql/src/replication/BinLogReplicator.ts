import { replication, storage } from '@powersync/service-core';
import { BinLogReplicationJob } from './BinLogReplicationJob.js';
import { MySQLConnectionManagerFactory } from './MySQLConnectionManagerFactory.js';
import { MySQLModule } from '../module/MySQLModule.js';

export interface BinLogReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: MySQLConnectionManagerFactory;
}

export class BinLogReplicator extends replication.AbstractReplicator<BinLogReplicationJob> {
  private readonly connectionFactory: MySQLConnectionManagerFactory;

  constructor(options: BinLogReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  createJob(options: replication.CreateJobOptions): BinLogReplicationJob {
    return new BinLogReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      metrics: this.metrics,
      lock: options.lock,
      connectionFactory: this.connectionFactory,
      rateLimiter: this.rateLimiter
    });
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    // The MySQL module does not create anything which requires cleanup on the MySQL server.
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }

  async testConnection() {
    return await MySQLModule.testConnection(this.connectionFactory.connectionConfig);
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
    // Measure the lag from the last commit or keepalive timestamp.
    // This is not 100% accurate since it is the commit time in the storage db rather than
    // the source db, but it's the best we currently have for mysql.
    const checkpointTs = content.last_checkpoint_ts?.getTime() ?? 0;
    const keepaliveTs = content.last_keepalive_ts?.getTime() ?? 0;
    const latestTs = Math.max(checkpointTs, keepaliveTs);
    if (latestTs != 0) {
      return Date.now() - latestTs;
    }

    return undefined;
  }
}
