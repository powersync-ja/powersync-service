import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { ReplicationStreamDocumentV3, SyncConfigDefinition } from '../storage-index.js';
import {
  MongoPersistedSyncConfigContentV1,
  MongoPersistedSyncConfigContentV3
} from './MongoPersistedSyncConfigContent.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig } from './models.js';
import { SyncRuleDocumentV1 } from './v1/models.js';

export class MongoPersistedReplicationStream extends storage.PersistedReplicationStream {
  public current_lock: MongoSyncRulesLock | null = null;

  constructor(
    private readonly db: PowerSyncMongo,
    private readonly doc: SyncRuleDocumentV1 | ReplicationStreamDocumentV3,
    private readonly configs: SyncConfigDefinition[] = []
  ) {
    const storageVersion = doc.storage_version ?? storage.LEGACY_STORAGE_VERSION;
    const replicationJobId =
      configs.length == 0
        ? String(doc._id)
        : `${doc._id}:${configs
            .map((config) => config._id.toHexString())
            .sort()
            .join(',')}`;

    super({
      id: doc._id,
      slot_name: doc.slot_name ?? `powersync_${doc._id}`,
      state: doc.state,
      storageVersion,
      replicationJobId
    });
  }

  getStorageConfig() {
    return getMongoStorageConfig(this.storageVersion);
  }

  toSyncConfigContent(): MongoPersistedSyncConfigContentV1 | MongoPersistedSyncConfigContentV3 {
    if (this.getStorageConfig().incrementalReprocessing) {
      if (this.configs.length == 0) {
        throw new ServiceAssertionError(`Cannot create v3 storage without sync config definitions`);
      }
      return new MongoPersistedSyncConfigContentV3(this.db, this.doc as ReplicationStreamDocumentV3, this.configs);
    }

    return new MongoPersistedSyncConfigContentV1(this.db, this.doc as SyncRuleDocumentV1);
  }

  async lock(session?: mongo.ClientSession) {
    const lock = await MongoSyncRulesLock.createLock(this.db.versioned(this.getStorageConfig()), this, session);
    this.current_lock = lock;
    return lock;
  }
}
