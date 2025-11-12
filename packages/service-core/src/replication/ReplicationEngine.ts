import { container, logger } from '@powersync/lib-services-framework';
import { AbstractReplicator } from './AbstractReplicator.js';
import { ConnectionTestResult } from './ReplicationModule.js';

export class ReplicationEngine {
  private readonly replicators: Map<string, AbstractReplicator> = new Map();
  private probeInterval: NodeJS.Timeout | null = null;

  /**
   *  Register a Replicator with the engine
   *
   *  @param replicator
   */
  public register(replicator: AbstractReplicator) {
    if (this.replicators.has(replicator.id)) {
      throw new Error(`Replicator with id: ${replicator.id} already registered`);
    }
    logger.info(`Successfully registered Replicator ${replicator.id} with ReplicationEngine`);
    this.replicators.set(replicator.id, replicator);
  }

  /**
   *  Start replication on all managed Replicators
   */
  public start(): void {
    logger.info('Starting Replication Engine...');
    for (const replicator of this.replicators.values()) {
      logger.info(`Starting Replicator: ${replicator.id}`);
      replicator.start();
    }
    if (this.replicators.size == 0) {
      // If a replicator is running, the replicators update the probes.
      // If no connections are configured, then no replicator is running
      // Typical when no connections are configured.
      // In this case, update the probe here to avoid liveness probe failures.
      this.probeInterval = setInterval(() => {
        container.probes.touch().catch((e) => {
          logger.error(`Failed to touch probe`, e);
        });
      }, 5_000);
    }
    logger.info('Successfully started Replication Engine.');
  }

  /**
   *  Stop replication on all managed Replicators
   */
  public async shutDown(): Promise<void> {
    logger.info('Shutting down Replication Engine...');
    for (const replicator of this.replicators.values()) {
      logger.info(`Stopping Replicator: ${replicator.id}`);
      await replicator.stop();
    }
    if (this.probeInterval) {
      clearInterval(this.probeInterval);
    }
    logger.info('Successfully shut down Replication Engine.');
  }

  public async testConnection(): Promise<ConnectionTestResult[]> {
    return await Promise.all([...this.replicators.values()].map((replicator) => replicator.testConnection()));
  }
}
