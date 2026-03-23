import { storage, replication } from '@powersync/service-core';
import { ChangeStreamReplicationJob } from './ChangeStreamReplicationJob.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { MongoErrorRateLimiter } from './MongoErrorRateLimiter.js';
import { MongoModule } from '../module/MongoModule.js';
import { MongoLSN } from '../common/MongoLSN.js';
import { timestampToDate } from './replication-utils.js';

export interface ChangeStreamReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class ChangeStreamReplicator extends replication.AbstractReplicator<ChangeStreamReplicationJob> {
  private readonly connectionFactory: ConnectionManagerFactory;

  constructor(options: ChangeStreamReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  createJob(options: replication.CreateJobOptions): ChangeStreamReplicationJob {
    return new ChangeStreamReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      metrics: this.metrics,
      connectionFactory: this.connectionFactory,
      lock: options.lock,
      rateLimiter: this.rateLimiter
    });
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    // TODO: Implement anything?
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }

  async testConnection() {
    return await MongoModule.testConnection(this.connectionFactory.dbConnectionConfig);
  }
}
