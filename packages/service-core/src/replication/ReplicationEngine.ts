import { logger } from '@powersync/lib-services-framework';
import { AbstractReplicator } from './AbstractReplicator.js';
import { ReplicationEventManager } from './ReplicationEventManager.js';

export class ReplicationEngine {
  private readonly replicators: Map<string, AbstractReplicator> = new Map();

  readonly eventManager: ReplicationEventManager;

  constructor() {
    this.eventManager = new ReplicationEventManager();
  }

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
    for (const replicator of this.replicators.values()) {
      replicator.start();
    }
  }

  /**
   *  Stop replication on all managed Replicators
   */
  public async stop(): Promise<void> {
    for (const replicator of this.replicators.values()) {
      await replicator.stop();
    }
  }
}
