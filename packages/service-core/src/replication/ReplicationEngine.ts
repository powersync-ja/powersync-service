import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';
import { AbstractReplicator } from './AbstractReplicator.js';

export interface ReplicationEngineOptions {
  storage: storage.StorageFactoryProvider;
  config: utils.SyncRulesConfig;
}

export class ReplicationEngine {
  private readonly options: ReplicationEngineOptions;
  private readonly replicators: Map<string, AbstractReplicator> = new Map();

  constructor(options: ReplicationEngineOptions) {
    this.options = options;
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
