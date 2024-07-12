import { ReplicationAdapter } from './ReplicationAdapter.js';
import { BucketStorageFactory } from '../../storage/BucketStorage.js';

/**
 *   A replicator manages the mechanics for replicating data from a data source to a storage bucket.
 *   This includes copying across the original data set and then keeping it in sync with the data source.
 *   TODO: Implement this. This will replace the current WallStreamManager
 */
export class Replicator {
  private readonly adapter: ReplicationAdapter<any>;
  private storage: BucketStorageFactory;

  constructor(storage: BucketStorageFactory, adapter: ReplicationAdapter<any>) {
    this.adapter = adapter;
    this.storage = storage;
  }

  public async start(): Promise<void> {
    // start the replication
  }

  public async stop(): Promise<void> {
    // stop the replication
  }
}
