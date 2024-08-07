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
  private readonly replicators: Map<ReplicationAdapter, Replicator> = new Map();

  constructor(options: ReplicationEngineOptions) {
    this.options = options;
  }

  /**
   *  Create a new Replicator from the provided ReplicationAdapter. Once started the Replicator will begin
   *  replicating data from the DataSource to PowerSync and keep it up to date.
   *
   *  @param adapter
   */
  public register(adapter: ReplicationAdapter) {
    if (this.replicators.has(adapter)) {
      throw new Error(`Replicator for type ${adapter.name} already registered`);
    }
    this.replicators.set(
      adapter,
      new Replicator({
        adapter: adapter,
        storage: this.options.storage,
        sync_rule_provider: new utils.ConfigurationFileSyncRulesProvider(this.options.config)
      })
    );
  }

  /**
   *  Start replication on all managed Replicators
   */
  public async start(): Promise<void> {
    for (const replicator of this.replicators.values()) {
      await replicator.start();
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
