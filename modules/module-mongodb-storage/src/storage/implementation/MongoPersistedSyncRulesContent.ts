import { storage, SyncRuleState } from '@powersync/service-core';
import { SyncConfigDefinition, SyncRuleDocumentV3 } from '../storage-index.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig, SyncRuleDocument } from './models.js';

export class MongoPersistedSyncRulesContent extends storage.PersistedSyncRulesContent {
  public current_lock: MongoSyncRulesLock | null = null;
  public readonly mapping: BucketDefinitionMapping;

  constructor(
    private db: PowerSyncMongo,
    doc: SyncRuleDocument
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
      active: doc.state == SyncRuleState.ACTIVE,
      storageVersion: doc.storage_version ?? storage.LEGACY_STORAGE_VERSION
    });
    this.mapping = new BucketDefinitionMapping();
  }

  getStorageConfig() {
    return getMongoStorageConfig(this.storageVersion);
  }

  parsed(options: storage.ParseSyncRulesOptions): storage.PersistedSyncRules {
    const parsed = super.parsed(options);
    const storageConfig = this.getStorageConfig();

    return new MongoPersistedSyncRules(
      parsed.id,
      parsed.sync_rules,
      parsed.slot_name,
      storageConfig.incrementalReprocessing ? this.mapping : null,
      storageConfig
    );
  }

  async lock() {
    const lock = await MongoSyncRulesLock.createLock(this.db.versioned(this.getStorageConfig()), this);
    this.current_lock = lock;
    return lock;
  }
}

export class MongoPersistedSyncRulesContentV3 extends storage.PersistedSyncRulesContent {
  public current_lock: MongoSyncRulesLock | null = null;
  public readonly mapping: BucketDefinitionMapping;

  constructor(
    private db: PowerSyncMongo,
    doc: SyncRuleDocumentV3,
    config: SyncConfigDefinition
  ) {
    super({
      id: doc._id,
      sync_rules_content: config.content,
      compiled_plan: config.serialized_plan ?? null,

      // FIXME: Use the correct values here
      last_checkpoint_lsn: doc.last_checkpoint_lsn,
      slot_name: doc.slot_name ?? `powersync_${doc._id}`,
      last_fatal_error: doc.last_fatal_error,
      last_fatal_error_ts: doc.last_fatal_error_ts,
      last_checkpoint_ts: doc.last_checkpoint_ts,
      last_keepalive_ts: doc.last_keepalive_ts,
      active:
        doc.state == SyncRuleState.ACTIVE &&
        doc.sync_configs.find((c) => c._id == config._id)?.state == SyncRuleState.ACTIVE,
      storageVersion: doc.storage_version
    });
    this.mapping = BucketDefinitionMapping.fromSyncConfig(config);
  }

  getStorageConfig() {
    return getMongoStorageConfig(this.storageVersion);
  }

  parsed(options: storage.ParseSyncRulesOptions): storage.PersistedSyncRules {
    const parsed = super.parsed(options);
    const storageConfig = this.getStorageConfig();

    return new MongoPersistedSyncRules(
      parsed.id,
      parsed.sync_rules,
      parsed.slot_name,
      storageConfig.incrementalReprocessing ? this.mapping : null,
      storageConfig
    );
  }

  async lock() {
    const lock = await MongoSyncRulesLock.createLock(this.db.versioned(this.getStorageConfig()), this);
    this.current_lock = lock;
    return lock;
  }
}
