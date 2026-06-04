import { mongo } from '@powersync/lib-service-mongodb';
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

abstract class MongoPersistedSyncConfigContentBase extends storage.PersistedSyncConfigContent {
  public readonly mapping: BucketDefinitionMapping;
  public readonly syncConfigObjectId: bson.ObjectId | null;

  protected constructor(
    protected readonly db: PowerSyncMongo,
    options: Omit<storage.PersistedSyncConfigContentData, 'syncConfigId'> & {
      mapping: BucketDefinitionMapping;
      syncConfigId: bson.ObjectId | null;
    }
  ) {
    const { mapping, syncConfigId, ...base } = options;
    super({
      ...base,
      syncConfigId: syncConfigId?.toHexString() ?? null
    });
    this.mapping = mapping;
    this.syncConfigObjectId = syncConfigId;
  }

  getStorageConfig() {
    return getMongoStorageConfig(this.storageVersion);
  }

  parsed(options: storage.ParseSyncRulesOptions): storage.PersistedSyncRules {
    const parsed = super.parsed(options);
    const storageConfig = this.getStorageConfig();
    const [syncConfig] = parsed.syncConfigs;
    if (syncConfig == null) {
      throw new ServiceAssertionError(`Expected one parsed sync config`);
    }

    return new MongoPersistedSyncRules(parsed.id, storageConfig, parsed.slot_name, [
      { syncConfig, mapping: storageConfig.incrementalReprocessing ? this.mapping : null }
    ]);
  }
}

export class MongoPersistedSyncConfigContentV1 extends MongoPersistedSyncConfigContentBase {
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
      state: doc.state,
      storageVersion: doc.storage_version ?? storage.LEGACY_STORAGE_VERSION,
      mapping: new BucketDefinitionMapping(),
      syncConfigId: null
    });
  }
}

export class MongoPersistedSyncConfigContentV3 extends MongoPersistedSyncConfigContentBase {
  declare public readonly syncConfigObjectId: bson.ObjectId;
  private readonly doc: ReplicationStreamDocumentV3;
  private readonly configs: SyncConfigDefinition[];
  public readonly syncConfigIds: bson.ObjectId[];

  constructor(
    db: PowerSyncMongo,
    doc: ReplicationStreamDocumentV3,
    config: SyncConfigDefinition | SyncConfigDefinition[]
  ) {
    const configs = Array.isArray(config) ? config : [config];
    const selected = configs[0];
    const replicationJobId = `${doc._id}:${configs
      .map((config) => config._id.toHexString())
      .sort()
      .join(',')}`;
    const state = doc.sync_configs.find((c) => c._id.equals(selected._id));
    if (state == null) {
      throw new ServiceAssertionError(`Cannot find sync config ${selected._id} in replication stream ${doc._id}`);
    }
    super(db, {
      id: doc._id,
      sync_rules_content: selected.content,
      compiled_plan: selected.serialized_plan ?? null,

      last_checkpoint_lsn: state?.last_checkpoint_lsn ?? null,
      slot_name: doc.slot_name ?? `powersync_${doc._id}`,
      last_fatal_error: doc.last_fatal_error,
      last_fatal_error_ts: doc.last_fatal_error_ts,
      last_checkpoint_ts: doc.last_checkpoint_ts,
      last_keepalive_ts: doc.last_keepalive_ts,
      active: doc.state == SyncRuleState.ACTIVE && state.state == SyncRuleState.ACTIVE,
      state: state.state,
      storageVersion: doc.storage_version,
      mapping: BucketDefinitionMapping.fromSyncConfig(selected),
      syncConfigId: selected._id,
      replicationJobId
    });
    this.doc = doc;
    this.configs = configs;
    this.syncConfigIds = configs.map((config) => config._id);
  }

  parsed(options: storage.ParseSyncRulesOptions): storage.PersistedSyncRules {
    const storageConfig = this.getStorageConfig();
    const syncConfigs = this.configs.map((config) => {
      const content = new MongoPersistedSyncConfigContentV3(this.db, this.doc, config);
      const parsed = storage.PersistedSyncConfigContent.prototype.parsed.call(content, options);
      const [syncConfig] = parsed.syncConfigs;
      if (syncConfig == null) {
        throw new ServiceAssertionError(`Expected one parsed sync config`);
      }
      return {
        syncConfigId: config._id.toHexString(),
        syncConfig,
        mapping: BucketDefinitionMapping.fromSyncConfig(config)
      };
    });

    return new MongoPersistedSyncRules(this.id, storageConfig, this.slot_name, syncConfigs);
  }
}
