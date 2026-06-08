import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage, SyncRuleState } from '@powersync/service-core';
import * as bson from 'bson';
import { ReplicationStreamDocumentV3, SyncConfigDefinition, SyncRuleDocumentV1 } from '../storage-index.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig } from './models.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';

export abstract class MongoPersistedSyncConfigContentBase extends storage.PersistedSyncConfigContent {
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

  parsed(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    const parsed = super.parsed(options);
    const storageConfig = this.getStorageConfig();
    const [syncConfig] = parsed.syncConfigs;
    if (syncConfig == null) {
      throw new ServiceAssertionError(`Expected one parsed sync config`);
    }

    return new MongoParsedSyncConfigSet(parsed.replicationStreamId, storageConfig, parsed.replicationStreamName, [
      { syncConfig, mapping: storageConfig.incrementalReprocessing ? this.mapping : null }
    ]);
  }
}
export class MongoPersistedSyncConfigContentV1 extends MongoPersistedSyncConfigContentBase {
  constructor(db: PowerSyncMongo, doc: SyncRuleDocumentV1) {
    super(db, {
      replicationStreamId: doc._id,
      sync_rules_content: doc.content,
      compiled_plan: doc.serialized_plan ?? null,
      last_checkpoint_lsn: doc.last_checkpoint_lsn,
      // Handle legacy values
      replicationStreamName: doc.slot_name ?? `powersync_${doc._id}`,
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
      replicationStreamId: doc._id,
      sync_rules_content: selected.content,
      compiled_plan: selected.serialized_plan ?? null,

      last_checkpoint_lsn: state?.last_checkpoint_lsn ?? null,
      replicationStreamName: doc.slot_name ?? `powersync_${doc._id}`,
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

  parsed(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    const storageConfig = this.getStorageConfig();
    const syncConfigs = this.configs.map((config) => {
      return {
        syncConfigId: config._id.toHexString(),
        syncConfig: storage.parsePersistedSyncConfigContent({
          content: config.content,
          compiledPlan: config.serialized_plan ?? null,
          storageVersion: this.storageVersion,
          parseOptions: options
        }),
        mapping: BucketDefinitionMapping.fromSyncConfig(config)
      };
    });

    return new MongoParsedSyncConfigSet(
      this.replicationStreamId,
      storageConfig,
      this.replicationStreamName,
      syncConfigs
    );
  }
}
