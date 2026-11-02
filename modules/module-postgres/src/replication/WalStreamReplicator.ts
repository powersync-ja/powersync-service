import { replication, storage } from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { ReplicationMetric } from '@powersync/service-types';
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

  public override async start(): Promise<void> {
    // Record replicated bytes using global jpgwire metrics. Only registered if this module is replicating.
    // Connection checks do not start replication and do not register replication metrics.
    const bytesReplicated = this.metrics.getCounter(ReplicationMetric.DATA_REPLICATED_BYTES);
    jpgwire.setMetricsRecorder({
      addBytesRead(bytes) {
        bytesReplicated.add(bytes);
      }
    });
    this.logger.info('Successfully set up connection metrics recorder for Postgres replication.');

    await super.start();
  }

  createJob(options: replication.CreateJobOptions): WalStreamReplicationJob {
    return new WalStreamReplicationJob({
      id: this.createJobId(options.storage.replicationStreamId),
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
      await cleanUpReplicationSlot(syncRulesStorage.replicationStreamName, connectionManager.pool);
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
}
