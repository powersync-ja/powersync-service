import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { SourceRecordStoreImpl } from '../bucket-operations/source-record-store-impl.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class SourceRecordStoreV3 extends SourceRecordStoreImpl {
  constructor(db: VersionedPowerSyncMongoV3, groupId: number, mapping: BucketDefinitionMapping) {
    super(
      (gid, tableId) => db.sourceRecordsV3(gid, tableId),
      (gid) => db.sourceTablesV3(gid),
      groupId,
      mapping
    );
  }
}
