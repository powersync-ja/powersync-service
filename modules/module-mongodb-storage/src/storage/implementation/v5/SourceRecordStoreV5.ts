import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { SourceRecordStoreImpl } from '../bucket-operations/source-record-store-impl.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class SourceRecordStoreV5 extends SourceRecordStoreImpl {
  constructor(db: VersionedPowerSyncMongoV5, groupId: number, mapping: BucketDefinitionMapping) {
    super(
      (gid, tableId) => db.sourceRecordsV5(gid, tableId),
      (gid) => db.sourceTablesV5(gid),
      groupId,
      mapping
    );
  }
}
