import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import {
  ReplicationStreamDocumentV3,
  SyncConfigDefinition,
  SyncRuleConfigStateV3,
  SyncRuleDocumentV1
} from '../storage-index.js';
import { SingleSyncConfigBucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig } from './models.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';

export abstract class MongoPersistedSyncConfigContentBase extends storage.PersistedSyncConfigContent {
  public readonly mapping: SingleSyncConfigBucketDefinitionMapping;
  public readonly syncConfigObjectId: bson.ObjectId | null;

  protected constructor(
    protected readonly db: PowerSyncMongo,
    options: Omit<storage.PersistedSyncConfigContentData, 'syncConfigId'> & {
      mapping: SingleSyncConfigBucketDefinitionMapping;
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

    if (storageConfig.incrementalReprocessing) {
      const syncConfigId = this.syncConfigId;
      if (syncConfigId == null) {
        throw new ServiceAssertionError(`syncConfigId is required for v3 sync config content`);
      }

      return new MongoParsedSyncConfigSet(parsed.replicationStreamId, storageConfig, parsed.replicationStreamName, [
        { syncConfigId, syncConfig, mapping: this.mapping }
      ]);
    }

    return new MongoParsedSyncConfigSet(parsed.replicationStreamId, storageConfig, parsed.replicationStreamName, [
      { syncConfig, mapping: null }
    ]);
  }
}
export class MongoPersistedSyncConfigContentV1 extends MongoPersistedSyncConfigContentBase {
  constructor(db: PowerSyncMongo, doc: SyncRuleDocumentV1) {
    super(db, {
      replicationStreamId: doc._id,
      sync_rules_content: doc.content,
      compiled_plan: doc.serialized_plan ?? null,
      // Handle legacy values
      replicationStreamName: doc.slot_name ?? `powersync_${doc._id}`,
      storageVersion: doc.storage_version ?? storage.LEGACY_STORAGE_VERSION,
      mapping: new SingleSyncConfigBucketDefinitionMapping(),
      syncConfigId: null
    });
  }

  async getSyncConfigStatus(): Promise<storage.PersistedSyncConfigStatus | null> {
    const doc = await this.db.sync_rules.findOne<SyncRuleDocumentV1>({ _id: this.replicationStreamId });
    if (doc == null) {
      return null;
    }

    return syncConfigStatusFromV1(doc);
  }
}
export class MongoPersistedSyncConfigContentV3 extends MongoPersistedSyncConfigContentBase {
  declare public readonly syncConfigObjectId: bson.ObjectId;

  constructor(db: PowerSyncMongo, doc: ReplicationStreamDocumentV3, config: SyncConfigDefinition) {
    const state = doc.sync_configs.find((c) => c._id.equals(config._id));
    if (state == null) {
      throw new ServiceAssertionError(`Cannot find sync config ${config._id} in replication stream ${doc._id}`);
    }
    super(db, {
      replicationStreamId: doc._id,
      sync_rules_content: config.content,
      compiled_plan: config.serialized_plan ?? null,

      replicationStreamName: doc.slot_name ?? `powersync_${doc._id}`,
      storageVersion: doc.storage_version,
      mapping: SingleSyncConfigBucketDefinitionMapping.fromSyncConfig(config),
      syncConfigId: config._id
    });
  }

  async getSyncConfigStatus(): Promise<storage.PersistedSyncConfigStatus | null> {
    const doc = await this.db.sync_rules.findOne<ReplicationStreamDocumentV3>({
      _id: this.replicationStreamId,
      'sync_configs._id': this.syncConfigObjectId
    });
    if (doc == null) {
      return null;
    }

    const state = doc.sync_configs.find((c) => c._id.equals(this.syncConfigObjectId));
    if (state == null) {
      return null;
    }

    return syncConfigStatusFromV3(doc, state);
  }
}

function syncConfigStatusFromV1(doc: SyncRuleDocumentV1): storage.PersistedSyncConfigStatus {
  return {
    id: String(doc._id),
    replicationStreamId: doc._id,
    state: doc.state,
    snapshot_done: doc.snapshot_done,
    last_checkpoint_lsn: doc.last_checkpoint_lsn,
    last_fatal_error: doc.last_fatal_error,
    last_fatal_error_ts: doc.last_fatal_error_ts,
    last_keepalive_ts: doc.last_keepalive_ts,
    last_checkpoint_ts: doc.last_checkpoint_ts
  };
}

function syncConfigStatusFromV3(
  doc: ReplicationStreamDocumentV3,
  state: SyncRuleConfigStateV3
): storage.PersistedSyncConfigStatus {
  return {
    id: state._id.toHexString(),
    replicationStreamId: doc._id,
    state: state.state,
    snapshot_done: state.snapshot_done,
    last_checkpoint_lsn: state.last_checkpoint_lsn,
    last_fatal_error: doc.last_fatal_error,
    last_fatal_error_ts: doc.last_fatal_error_ts,
    last_keepalive_ts: doc.last_keepalive_ts,
    last_checkpoint_ts: doc.last_checkpoint_ts
  };
}
