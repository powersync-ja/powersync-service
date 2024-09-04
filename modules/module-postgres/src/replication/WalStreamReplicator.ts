import { AbstractReplicatorOptions, replication } from '@powersync/service-core';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { WalStreamReplicationJob } from './WalStreamReplicationJob.js';

export interface WalStreamReplicatorOptions extends AbstractReplicatorOptions {
  connectionFactory: ConnectionManagerFactory;
  eventManager: replication.ReplicationEventManager;
}

export class WalStreamReplicator extends replication.AbstractReplicator<WalStreamReplicationJob> {
  private readonly connectionFactory: ConnectionManagerFactory;
  private readonly eventManager: replication.ReplicationEventManager;

  constructor(options: WalStreamReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.eventManager = options.eventManager;
  }

  createJob(options: replication.CreateJobOptions): WalStreamReplicationJob {
    return new WalStreamReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      connectionFactory: this.connectionFactory,
      lock: options.lock,
      eventManager: this.eventManager
    });
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }
}
