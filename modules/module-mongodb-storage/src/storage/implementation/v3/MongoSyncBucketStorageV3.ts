import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import { MongoSyncBucketStorageOptions } from '../AbstractMongoSyncBucketStorage.js';
import { MongoSyncBucketStorageCallbacks } from '../common/MongoSyncBucketStorageCallbacks.js';
import type { VersionedPowerSyncMongo } from '../db.js';
import { V3FormatAdapter } from '../document-formats/v3-format.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage } from '../MongoSyncBucketStorage.js';
import { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { MongoCompactorV3 } from './MongoCompactorV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

function assertVersionedPowerSyncMongoV3(db: VersionedPowerSyncMongo): VersionedPowerSyncMongoV3 {
  // The factory returns the base VersionedPowerSyncMongo type, but we know it's V3
  // based on the storage config (incrementalReprocessing flag).
  return db as VersionedPowerSyncMongoV3;
}

export class MongoSyncBucketStorageV3 extends MongoSyncBucketStorage {
  get db(): VersionedPowerSyncMongoV3 {
    // We know the concrete type because this class is only instantiated for V3 storage.
    return super.db as VersionedPowerSyncMongoV3;
  }

  get checksums(): MongoChecksumsV3 {
    // We know the concrete type because this class is only instantiated for V3 storage.
    return super.checksums as MongoChecksumsV3;
  }

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    const db = assertVersionedPowerSyncMongoV3(factory.db.versioned(sync_rules.getStorageConfig()));
    const callbacks: MongoSyncBucketStorageCallbacks = {
      bucketData: (gid, defId) => db.bucketDataV3(gid, defId),
      parameterIndex: (gid, idxId) => db.parameterIndexV3(gid, idxId),
      bucketState: (gid) => db.bucketStateV3(gid),
      sourceTables: (gid) => db.sourceTablesV3(gid),
      listBucketDataCollections: (gid) => db.listBucketDataCollectionsV3(gid),
      listParameterIndexCollections: (gid) => db.listParameterIndexCollectionsV3(gid),
      listSourceRecordCollections: (gid) => db.listSourceRecordCollectionsV3(gid),
      createChecksums: (d, gid, opts) => new MongoChecksumsV3(assertVersionedPowerSyncMongoV3(d), gid, opts),
      createCompactor: (storage, d, opts) => new MongoCompactorV3(storage, assertVersionedPowerSyncMongoV3(d), opts),
      createParameterCompactor: (d, gid, checkpoint, opts) =>
        new MongoParameterCompactor(assertVersionedPowerSyncMongoV3(d), gid, checkpoint, opts, () =>
          assertVersionedPowerSyncMongoV3(d)
            .listParameterIndexCollectionsV3(gid)
            .then((collections) =>
              // Cast from the version-specific collection type to the generic Document type
              // used by the parameter compactor base class.
              collections.map((c) => c.collection as unknown as mongo.Collection<mongo.Document>)
            )
        ),
      createWriter: (batchOptions) => new MongoBucketBatchV3(batchOptions),
      formatAdapter: new V3FormatAdapter()
    };
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options, callbacks);
  }
}
