import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { SourceRecordStore } from './SourceRecordStore.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  private readonly store: SourceRecordStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new SourceRecordStoreV3(this.db, this.group_id, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }
}
