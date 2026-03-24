import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { CurrentDataStore } from './CurrentDataStore.js';
import { CurrentDataStoreV1 } from './CurrentDataStoreV1.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  private readonly store: CurrentDataStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new CurrentDataStoreV1(this.db, this.group_id);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV1(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get currentDataStore(): CurrentDataStore {
    return this.store;
  }
}
