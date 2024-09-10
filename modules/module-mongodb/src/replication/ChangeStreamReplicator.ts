import { storage, replication } from '@powersync/service-core';
import { ChangeStreamReplicationJob } from './ChangeStreamReplicationJob.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

export interface WalStreamReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class ChangeStreamReplicator extends replication.AbstractReplicator<ChangeStreamReplicationJob> {
  private readonly connectionFactory: ConnectionManagerFactory;

  constructor(options: WalStreamReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  createJob(options: replication.CreateJobOptions): ChangeStreamReplicationJob {
    return new ChangeStreamReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      connectionFactory: this.connectionFactory,
      lock: options.lock
    });
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    // TODO: Implement anything?
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }
}
