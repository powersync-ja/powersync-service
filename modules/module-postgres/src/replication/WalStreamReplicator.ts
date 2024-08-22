import { AbstractReplicatorOptions, replication } from '@powersync/service-core';
import { WalStreamReplicationJob } from './WalStreamReplicationJob.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

export interface WalStreamReplicatorOptions extends AbstractReplicatorOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class WalStreamReplicator extends replication.AbstractReplicator<WalStreamReplicationJob> {
  private readonly connectionFactory: ConnectionManagerFactory;

  constructor(options: WalStreamReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  createJob(options: replication.CreateJobOptions): WalStreamReplicationJob {
    return new WalStreamReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      connectionFactory: this.connectionFactory,
      lock: options.lock
    });
  }
}
