import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import { SourceRecordStoreV1 } from './SourceRecordStoreV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV1;

  private readonly store: SourceRecordStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new SourceRecordStoreV1(this.db, this.group_id);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV1(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(_sourceTables: import('@powersync/service-core').storage.SourceTable[]) {
    // No-op for V1: source records live in a shared collection.
  }
}
