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
}
