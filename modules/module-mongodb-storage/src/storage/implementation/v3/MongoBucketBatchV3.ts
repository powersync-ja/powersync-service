import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV3;

  constructor(options: MongoBucketBatchOptions) {
    super({
      ...options,
      listSourceRecordCollections: (groupId) => (options.db as VersionedPowerSyncMongoV3).listSourceRecordCollectionsV3(groupId)
    });
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return new SourceRecordStoreV3(this.db, this.group_id, this.mapping);
  }
}
