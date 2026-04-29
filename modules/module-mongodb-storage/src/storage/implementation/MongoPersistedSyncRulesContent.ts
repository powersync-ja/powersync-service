import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage, SyncRuleState } from '@powersync/service-core';
import * as bson from 'bson';
import { ReplicationStreamDocumentV3, SyncConfigDefinition } from '../storage-index.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig } from './models.js';
import { SyncRuleDocumentV1 } from './v1/models.js';

abstract class MongoPersistedSyncRulesContentBase extends storage.PersistedSyncRulesContent {
  public current_lock: MongoSyncRulesLock | null = null;
  public readonly mapping: BucketDefinitionMapping;
  public readonly syncConfigId: bson.ObjectId | null;

  protected constructor(
    protected readonly db: PowerSyncMongo,
    options: ConstructorParameters<typeof storage.PersistedSyncRulesContent>[0] & {
      mapping: BucketDefinitionMapping;
      syncConfigId: bson.ObjectId | null;
    }
  ) {
    const { mapping, syncConfigId, ...base } = options;
    super(base);
    this.mapping = mapping;
    this.syncConfigId = syncConfigId;
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

export class MongoPersistedSyncRulesContentV1 extends MongoPersistedSyncRulesContentBase {
  constructor(db: PowerSyncMongo, doc: SyncRuleDocumentV1) {
    super(db, {
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
      storageVersion: doc.storage_version ?? storage.LEGACY_STORAGE_VERSION,
      mapping: new BucketDefinitionMapping(),
      syncConfigId: null
    });
  }
}

export class MongoPersistedSyncRulesContentV3 extends MongoPersistedSyncRulesContentBase {
  declare public readonly syncConfigId: bson.ObjectId;

  constructor(db: PowerSyncMongo, doc: ReplicationStreamDocumentV3, config: SyncConfigDefinition) {
    const state = doc.sync_configs.find((c) => c._id.equals(config._id));
    if (state == null) {
      throw new ServiceAssertionError(`Cannot find sync config ${config._id} in replication stream ${doc._id}`);
    }
    super(db, {
      id: doc._id,
      sync_rules_content: config.content,
      compiled_plan: config.serialized_plan ?? null,

      last_checkpoint_lsn: state?.last_checkpoint_lsn ?? null,
      slot_name: doc.slot_name ?? `powersync_${doc._id}`,
      last_fatal_error: doc.last_fatal_error,
      last_fatal_error_ts: doc.last_fatal_error_ts,
      last_checkpoint_ts: doc.last_checkpoint_ts,
      last_keepalive_ts: doc.last_keepalive_ts,
      active: doc.state == SyncRuleState.ACTIVE && state.state == SyncRuleState.ACTIVE,
      storageVersion: doc.storage_version,
      mapping: BucketDefinitionMapping.fromSyncConfig(config),
      syncConfigId: config._id
    });
  }
}
