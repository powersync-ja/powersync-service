import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { SourceRecordStoreImpl } from '../bucket-operations/source-record-store-impl.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

  constructor(options: MongoBucketBatchOptions) {
    super({
      ...options,
      listSourceRecordCollections: (groupId) =>
        (options.db as VersionedPowerSyncMongo).listSourceRecordCollections(groupId)
    });
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return new SourceRecordStoreImpl(
      (gid, tableId) => this.db.sourceRecords(gid, tableId),
      (gid) => this.db.sourceTables(gid),
      this.group_id,
      this.mapping
    );
  }
}
