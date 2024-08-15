import { ReplicationAdapter } from './ReplicationAdapter.js';
import { Replicator } from './Replicator.js';

import * as storage from '../../storage/storage-index.js';
import * as utils from '.././../util/util-index.js';

export interface ReplicationEngineOptions {
  storage: storage.StorageFactoryProvider;
  config: utils.SyncRulesConfig;
}

export class ReplicationEngine {
  private readonly options: ReplicationEngineOptions;
  private readonly replicators: Map<string, Replicator> = new Map();

  constructor(options: ReplicationEngineOptions) {
    this.options = options;
  }

  /**
   *  Register a Replicator with the engine
   *
   *  @param replicator
   */
  public register(replicator: Replicator) {
    if (this.replicators.has(replicator.id)) {
      throw new Error(`Replicator for type ${replicator.id} already registered`);
    }
    this.replicators.set(replicator.id, replicator);
  }

  /**
   *  Start replication on all managed Replicators
   */
  public async start(): Promise<void> {
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
