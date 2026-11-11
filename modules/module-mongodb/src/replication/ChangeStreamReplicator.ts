import { replication, storage } from '@powersync/service-core';
import { MongoModule } from '../module/MongoModule.js';
import { ChangeStreamReplicationJob } from './ChangeStreamReplicationJob.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { MongoReplicationQueryProviderFactory } from './MongoReplicationQueryProvider.js';

export interface ChangeStreamReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: ConnectionManagerFactory;
  createReplicationQueryProvider?: MongoReplicationQueryProviderFactory;
}

export class ChangeStreamReplicator extends replication.AbstractReplicator<ChangeStreamReplicationJob> {
  private readonly connectionFactory: ConnectionManagerFactory;
  private readonly createReplicationQueryProvider: MongoReplicationQueryProviderFactory | undefined;

  constructor(options: ChangeStreamReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.createReplicationQueryProvider = options.createReplicationQueryProvider;
  }

  createJob(options: replication.CreateJobOptions): ChangeStreamReplicationJob {
    return new ChangeStreamReplicationJob({
      id: this.createJobId(options.storage.replicationStreamId),
      storage: options.storage,
      metrics: this.metrics,
      connectionFactory: this.connectionFactory,
      createReplicationQueryProvider: this.createReplicationQueryProvider,
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
