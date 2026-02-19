import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig, SyncRuleDocument } from './models.js';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';

export class MongoPersistedSyncRulesContent extends storage.PersistedSyncRulesContent {
  public current_lock: MongoSyncRulesLock | null = null;

  constructor(
    private db: PowerSyncMongo,
    doc: mongo.WithId<SyncRuleDocument>
  ) {
    super({
      id: doc._id,
      sync_rules_content: doc.content,
      compiled_plan: doc.serialized_plan ?? null,
      last_checkpoint_lsn: doc.last_checkpoint_lsn,
      // Handle legacy values
      slot_name: doc.slot_name ?? `powersync_${doc._id}`,
      last_fatal_error: doc.last_fatal_error,
      last_fatal_error_ts: doc.last_fatal_error_ts,
      last_checkpoint_ts: doc.last_checkpoint_ts,
      last_keepalive_ts: doc.last_keepalive_ts,
      active: doc.state == 'ACTIVE',
      storageVersion: doc.storage_version ?? storage.LEGACY_STORAGE_VERSION
    });
  }

  getStorageConfig() {
    const storageConfig = getMongoStorageConfig(this.storageVersion);
    if (storageConfig == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1005,
        `Unsupported storage version ${this.storageVersion} for sync rules ${this.id}`
      );
    }
    return storageConfig;
  }

  async lock() {
    const lock = await MongoSyncRulesLock.createLock(this.db, this);
    this.current_lock = lock;
    return lock;
  }
}
