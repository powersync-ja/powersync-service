import { replication, storage } from '@powersync/service-core';
import { ConvexModule } from '../module/ConvexModule.js';
import { ConvexConnectionManagerFactory } from './ConvexConnectionManagerFactory.js';
import { ConvexReplicationJob } from './ConvexReplicationJob.js';

export interface ConvexReplicatorOptions extends replication.AbstractReplicatorOptions {
  connectionFactory: ConvexConnectionManagerFactory;
}

export class ConvexReplicator extends replication.AbstractReplicator<ConvexReplicationJob> {
  private readonly connectionFactory: ConvexConnectionManagerFactory;

  constructor(options: ConvexReplicatorOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
  }

  createJob(options: replication.CreateJobOptions): ConvexReplicationJob {
    return new ConvexReplicationJob({
      id: this.createJobId(options.storage.group_id),
      storage: options.storage,
      metrics: this.metrics,
      lock: options.lock,
      connectionFactory: this.connectionFactory,
      rateLimiter: this.rateLimiter
    });
  }

  async cleanUp(syncRulesStorage: storage.SyncRulesBucketStorage): Promise<void> {
    // No source-side cleanup needed for Convex.
  }

  async stop(): Promise<void> {
    await super.stop();
    await this.connectionFactory.shutdown();
  }

  async testConnection() {
    return await ConvexModule.testConnection(this.connectionFactory.connectionConfig);
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    const lag = await super.getReplicationLagMillis();
    if (lag != null) {
      return lag;
    }

    const content = await this.storage.getActiveSyncRulesContent();
    if (content == null) {
      return undefined;
    }

    const checkpointTs = content.last_checkpoint_ts?.getTime() ?? 0;
    const keepaliveTs = content.last_keepalive_ts?.getTime() ?? 0;
    const latestTs = Math.max(checkpointTs, keepaliveTs);
    if (latestTs == 0) {
      return undefined;
    }

    return Date.now() - latestTs;
  }
}
