import { replication, storage } from '@powersync/service-core';
import { PostgresModule } from '../module/PostgresModule.js';
import { getApplicationName } from '../utils/application-name.js';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { cleanUpReplicationSlot } from './replication-utils.js';
import { WalStreamReplicationJob } from './WalStreamReplicationJob.js';

export interface WalStreamReplicatorOptions extends replication.AbstractReplicatorOptions {
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
      metrics: this.metrics,
      connectionFactory: this.connectionFactory,
      lock: options.lock,
      rateLimiter: this.rateLimiter
    });
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    const connectionManager = this.connectionFactory.create({
      applicationName: getApplicationName(),
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

  async testConnection() {
    return await PostgresModule.testConnection(this.connectionFactory.dbConnectionConfig);
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
    // the source db, but it's the best we have for postgres.

    const checkpointTs = content.last_checkpoint_ts?.getTime() ?? 0;
    const keepaliveTs = content.last_keepalive_ts?.getTime() ?? 0;
    const latestTs = Math.max(checkpointTs, keepaliveTs);
    if (latestTs != 0) {
      return Date.now() - latestTs;
    }

    return undefined;
  }
}
