import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { CurrentDataStore } from './CurrentDataStore.js';
import { CurrentDataStoreV3 } from './CurrentDataStoreV3.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  private readonly store: CurrentDataStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new CurrentDataStoreV3(this.db, this.group_id, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get currentDataStore(): CurrentDataStore {
    return this.store;
  }
}
