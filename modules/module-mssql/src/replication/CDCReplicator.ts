import { replication, storage } from '@powersync/service-core';
import { MSSQLModule } from '../module/MSSQLModule.js';
import { AdditionalConfig } from '../types/types.js';
import { CDCReplicationJob } from './CDCReplicationJob.js';
import { MSSQLConnectionManagerFactory } from './MSSQLConnectionManagerFactory.js';

export interface CDCReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: MSSQLConnectionManagerFactory;
  additionalConfig: AdditionalConfig;
}

export class CDCReplicator extends replication.AbstractReplicator<CDCReplicationJob> {
  private readonly connectionFactory: MSSQLConnectionManagerFactory;
  private readonly cdcReplicatorOptions: CDCReplicatorOptions;

  constructor(options: CDCReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.cdcReplicatorOptions = options;
  }

  createJob(options: replication.CreateJobOptions): CDCReplicationJob {
    return new CDCReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      metrics: this.metrics,
      lock: options.lock,
      connectionFactory: this.connectionFactory,
      rateLimiter: this.rateLimiter,
      additionalConfig: this.cdcReplicatorOptions.additionalConfig
    });
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {}

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }

  async testConnection() {
    return await MSSQLModule.testConnection(this.connectionFactory.connectionConfig);
  }
}
