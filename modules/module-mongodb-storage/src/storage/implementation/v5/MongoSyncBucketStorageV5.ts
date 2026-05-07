import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import { MongoSyncBucketStorageOptions } from '../AbstractMongoSyncBucketStorage.js';
import { V5FormatAdapter } from '../document-formats/v5-format.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageBaseCallbacks } from '../MongoSyncBucketStorageBase.js';
import { MongoBucketBatchV5 } from './MongoBucketBatchV5.js';
import { MongoChecksumsV5 } from './MongoChecksumsV5.js';
import { MongoCompactorV5 } from './MongoCompactorV5.js';
import { MongoParameterCompactorV5 } from './MongoParameterCompactorV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoSyncBucketStorageV5 extends MongoSyncBucketStorage {
  declare readonly db: VersionedPowerSyncMongoV5;
  declare readonly checksums: MongoChecksumsV5;

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    const db = factory.db.versioned(sync_rules.getStorageConfig()) as VersionedPowerSyncMongoV5;
    const callbacks: MongoSyncBucketStorageBaseCallbacks = {
      bucketData: (gid, defId) => db.bucketDataV5(gid, defId),
      parameterIndex: (gid, idxId) => db.parameterIndexV5(gid, idxId),
      bucketState: (gid) => db.bucketStateV5(gid),
      sourceTables: (gid) => db.sourceTablesV5(gid),
      listBucketDataCollections: (gid) => db.listBucketDataCollectionsV5(gid),
      listParameterIndexCollections: (gid) => db.listParameterIndexCollectionsV5(gid),
      listSourceRecordCollections: (gid) => db.listSourceRecordCollectionsV5(gid),
      createChecksums: (d, gid, opts) => new MongoChecksumsV5(d as VersionedPowerSyncMongoV5, gid, opts),
      createCompactor: (storage, d, opts) => new MongoCompactorV5(storage, d as VersionedPowerSyncMongoV5, opts),
      createParameterCompactor: (d, gid, checkpoint, opts) =>
        new MongoParameterCompactorV5(d as VersionedPowerSyncMongoV5, gid, checkpoint, opts),
      createWriter: (batchOptions) => new MongoBucketBatchV5(batchOptions),
      formatAdapter: new V5FormatAdapter()
    };
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options, callbacks);
  }
}
