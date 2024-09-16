import { replication, storage } from '@powersync/service-core';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { cleanUpReplicationSlot } from './replication-utils.js';
import { WalStreamReplicationJob } from './WalStreamReplicationJob.js';

export interface WalStreamReplicatorOptions extends replication.AbstractReplicatorOptions {
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

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    const connectionManager = this.connectionFactory.create({
      idleTimeout: 30_000,
      maxSize: 1
    });
    try {
      // TODO: Slot_name will likely have to come from a different source in the future
      await cleanUpReplicationSlot(syncRulesStorage.slot_name, connectionManager.pool);
    } finally {
      await connectionManager.end();
    }
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }
}
