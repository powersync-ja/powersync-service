import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { SourceRecordStoreImpl } from '../bucket-operations/source-record-store-impl.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';

export class SourceRecordStoreV3 extends SourceRecordStoreImpl {
  constructor(db: VersionedPowerSyncMongo, groupId: number, mapping: BucketDefinitionMapping) {
    super(
      (gid, tableId) => db.sourceRecords(gid, tableId),
      (gid) => db.sourceTables(gid),
      groupId,
      mapping
    );
  }
}
