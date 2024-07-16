import { ReplicationAdapter } from './ReplicationAdapter.js';
import { BucketStorageFactory } from '../../storage/BucketStorage.js';
import { Replicator } from './Replicator.js';

export interface ReplicationEngineOptions {
  storage: BucketStorageFactory;
}

export class ReplicationEngine {
  private readonly options: ReplicationEngineOptions;
  private readonly replicators: Map<ReplicationAdapter<any>, Replicator> = new Map();

  constructor(options: ReplicationEngineOptions) {
    this.options = options;
  }

  /**
   *  Create a new Replicator from the provided ReplicationAdapter. Once started the Replicator will begin
   *  replicating data from the DataSource to PowerSync and keep it up to date.
   *
   *  @param adapter
   */
  public register(adapter: ReplicationAdapter<any>) {
    if (this.replicators.has(adapter)) {
      throw new Error(`Replicator for type ${adapter.name} already registered`);
    }
    this.replicators.set(adapter, new Replicator(adapter, this.options.storage));
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
