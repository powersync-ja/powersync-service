import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import { MongoSyncBucketStorageOptions } from '../AbstractMongoSyncBucketStorage.js';
import { V3FormatAdapter } from '../document-formats/v3-format.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageBaseCallbacks } from '../MongoSyncBucketStorageBase.js';
import { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { MongoCompactorV3 } from './MongoCompactorV3.js';
import { MongoParameterCompactorV3 } from './MongoParameterCompactorV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoSyncBucketStorageV3 extends MongoSyncBucketStorage {
  declare readonly db: VersionedPowerSyncMongoV3;
  declare readonly checksums: MongoChecksumsV3;

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    const db = factory.db.versioned(sync_rules.getStorageConfig()) as VersionedPowerSyncMongoV3;
    const callbacks: MongoSyncBucketStorageBaseCallbacks = {
      bucketData: (gid, defId) => db.bucketDataV3(gid, defId),
      parameterIndex: (gid, idxId) => db.parameterIndexV3(gid, idxId),
      bucketState: (gid) => db.bucketStateV3(gid),
      sourceTables: (gid) => db.sourceTablesV3(gid),
      listBucketDataCollections: (gid) => db.listBucketDataCollectionsV3(gid),
      listParameterIndexCollections: (gid) => db.listParameterIndexCollectionsV3(gid),
      listSourceRecordCollections: (gid) => db.listSourceRecordCollectionsV3(gid),
      createChecksums: (d, gid, opts) => new MongoChecksumsV3(d as VersionedPowerSyncMongoV3, gid, opts),
      createCompactor: (storage, d, opts) => new MongoCompactorV3(storage, d as VersionedPowerSyncMongoV3, opts),
      createParameterCompactor: (d, gid, checkpoint, opts) =>
        new MongoParameterCompactorV3(d as VersionedPowerSyncMongoV3, gid, checkpoint, opts),
      createWriter: (batchOptions) => new MongoBucketBatchV3(batchOptions),
      formatAdapter: new V3FormatAdapter()
    };
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options, callbacks);
  }
}
