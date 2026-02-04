import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { LEGACY_STORAGE_VERSION, STORAGE_VERSIONS, SyncRuleDocument } from './models.js';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';

export class MongoPersistedSyncRulesContent implements storage.PersistedSyncRulesContent {
  public readonly slot_name: string;

  public readonly id: number;
  public readonly sync_rules_content: string;
  public readonly last_checkpoint_lsn: string | null;
  public readonly last_fatal_error: string | null;
  public readonly last_fatal_error_ts: Date | null;
  public readonly last_keepalive_ts: Date | null;
  public readonly last_checkpoint_ts: Date | null;
  public readonly active: boolean;
  public readonly storage_version: number;

  public current_lock: MongoSyncRulesLock | null = null;

  constructor(
    private db: PowerSyncMongo,
    doc: mongo.WithId<SyncRuleDocument>
  ) {
    this.id = doc._id;
    this.sync_rules_content = doc.content;
    this.last_checkpoint_lsn = doc.last_checkpoint_lsn;
    // Handle legacy values
    this.slot_name = doc.slot_name ?? `powersync_${this.id}`;
    this.last_fatal_error = doc.last_fatal_error;
    this.last_fatal_error_ts = doc.last_fatal_error_ts;
    this.last_checkpoint_ts = doc.last_checkpoint_ts;
    this.last_keepalive_ts = doc.last_keepalive_ts;
    this.active = doc.state == 'ACTIVE';
    this.storage_version = doc.storage_version ?? LEGACY_STORAGE_VERSION;
  }

  /**
   * Load the storage config.
   *
   * This may throw if the persisted storage version is not supported.
   */
  getStorageConfig() {
    const storageConfig = STORAGE_VERSIONS[this.storage_version];
    if (storageConfig == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1403,
        `Unsupported storage version ${this.storage_version} for sync rules ${this.id}`
      );
    }
    return storageConfig;
  }

  parsed(options: storage.ParseSyncRulesOptions) {
    return new MongoPersistedSyncRules(
      this.id,
      SqlSyncRules.fromYaml(this.sync_rules_content, options),
      this.last_checkpoint_lsn,
      this.slot_name,
      this.getStorageConfig()
    );
  }

  async lock() {
    const lock = await MongoSyncRulesLock.createLock(this.db, this);
    this.current_lock = lock;
    return lock;
  }
}
