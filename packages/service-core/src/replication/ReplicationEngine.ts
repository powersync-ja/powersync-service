import { logger } from '@powersync/lib-services-framework';
import { AbstractReplicator } from './AbstractReplicator.js';

export class ReplicationEngine {
  private readonly replicators: Map<string, AbstractReplicator> = new Map();

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
    logger.info('Successfully shut down Replication Engine.');
  }
}
