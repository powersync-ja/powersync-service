import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV5 } from './PersistedBatchV5.js';
import { SourceRecordStoreV5 } from './SourceRecordStoreV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoBucketBatchV5 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV5;

  constructor(options: MongoBucketBatchOptions) {
    super({
      ...options,
      listSourceRecordCollections: (groupId) => (options.db as VersionedPowerSyncMongoV5).listSourceRecordCollectionsV5(groupId)
    });
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV5(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return new SourceRecordStoreV5(this.db, this.group_id, this.mapping);
  }
}
