import { replication, storage } from '@powersync/service-core';
import { SyntheticReplicationJob } from './SyntheticReplicationJob.js';
import { SyntheticReplicationSource } from './SyntheticReplicationSource.js';

export interface SyntheticReplicatorOptions extends replication.AbstractReplicatorOptions {
  readonly source: () => SyntheticReplicationSource;
}

export class SyntheticReplicator extends replication.AbstractReplicator<SyntheticReplicationJob> {
  constructor(private readonly syntheticOptions: SyntheticReplicatorOptions) {
    super(syntheticOptions);
  }

  createJob(options: replication.CreateJobOptions): SyntheticReplicationJob {
    return new SyntheticReplicationJob({
      id: this.createJobId(options.storage.replicationStreamId),
      storage: options.storage,
      metrics: this.metrics,
      lock: options.lock,
      rateLimiter: this.rateLimiter,
      source: this.syntheticOptions.source()
    });
  }

  async cleanUp(_storage: storage.SyncRulesBucketStorage): Promise<void> {}

  async testConnection(): Promise<replication.ConnectionTestResult> {
    return { connectionDescription: 'deterministic in-memory synthetic source' };
  }
}
