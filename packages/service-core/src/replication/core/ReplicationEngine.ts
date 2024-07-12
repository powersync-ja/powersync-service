import { ReplicationAdapter } from './ReplicationAdapter.js';
import { BucketStorageFactory } from '../../storage/BucketStorage.js';
import { Replicator } from './Replicator.js';

export class ReplicationEngine {
  private readonly storage: BucketStorageFactory;
  private readonly replicators: Map<ReplicationAdapter<any>, Replicator> = new Map();

  constructor(storage: BucketStorageFactory) {
    this.storage = storage;
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
    this.replicators.set(adapter, new Replicator(this.storage, adapter));
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
