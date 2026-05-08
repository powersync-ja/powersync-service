import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import { MongoSyncBucketStorageOptions } from '../AbstractMongoSyncBucketStorage.js';
import { MongoSyncBucketStorageCallbacks } from '../common/MongoSyncBucketStorageCallbacks.js';
import type { VersionedPowerSyncMongo } from '../db.js';
import { V5FormatAdapter } from '../document-formats/v5-format.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage } from '../MongoSyncBucketStorage.js';
import { MongoBucketBatchV5 } from './MongoBucketBatchV5.js';
import { MongoChecksumsV5 } from './MongoChecksumsV5.js';
import { MongoCompactorV5 } from './MongoCompactorV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

function assertVersionedPowerSyncMongoV5(db: VersionedPowerSyncMongo): VersionedPowerSyncMongoV5 {
  // The factory returns the base VersionedPowerSyncMongo type, but we know it's V5
  // based on the storage config (compressedBucketStorage flag).
  return db as VersionedPowerSyncMongoV5;
}

export class MongoSyncBucketStorageV5 extends MongoSyncBucketStorage {
  get db(): VersionedPowerSyncMongoV5 {
    // We know the concrete type because this class is only instantiated for V5 storage.
    return super.db as VersionedPowerSyncMongoV5;
  }

  get checksums(): MongoChecksumsV5 {
    // We know the concrete type because this class is only instantiated for V5 storage.
    return super.checksums as MongoChecksumsV5;
  }

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    const db = assertVersionedPowerSyncMongoV5(factory.db.versioned(sync_rules.getStorageConfig()));
    const callbacks: MongoSyncBucketStorageCallbacks = {
      bucketData: (gid, defId) => db.bucketDataV5(gid, defId),
      parameterIndex: (gid, idxId) => db.parameterIndexV5(gid, idxId),
      bucketState: (gid) => db.bucketStateV5(gid),
      sourceTables: (gid) => db.sourceTablesV5(gid),
      listBucketDataCollections: (gid) => db.listBucketDataCollectionsV5(gid),
      listParameterIndexCollections: (gid) => db.listParameterIndexCollectionsV5(gid),
      listSourceRecordCollections: (gid) => db.listSourceRecordCollectionsV5(gid),
      createChecksums: (d, gid, opts) => new MongoChecksumsV5(assertVersionedPowerSyncMongoV5(d), gid, opts),
      createCompactor: (storage, d, opts) => new MongoCompactorV5(storage, assertVersionedPowerSyncMongoV5(d), opts),
      createParameterCompactor: (d, gid, checkpoint, opts) =>
        new MongoParameterCompactor(assertVersionedPowerSyncMongoV5(d), gid, checkpoint, opts, () =>
          assertVersionedPowerSyncMongoV5(d)
            .listParameterIndexCollectionsV5(gid)
            .then((collections) =>
              // Cast from the version-specific collection type to the generic Document type
              // used by the parameter compactor base class.
              collections.map((c) => c.collection as unknown as mongo.Collection<mongo.Document>)
            )
        ),
      createWriter: (batchOptions) => new MongoBucketBatchV5(batchOptions),
      formatAdapter: new V5FormatAdapter()
    };
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options, callbacks);
  }
}
