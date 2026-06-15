import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { ReplicationStreamDocumentV3, SyncConfigDefinition } from '../storage-index.js';
import { SingleSyncConfigBucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';
import {
  MongoPersistedSyncConfigContentBase,
  MongoPersistedSyncConfigContentV1,
  MongoPersistedSyncConfigContentV3
} from './MongoPersistedSyncConfigContent.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { ReplicationStreamStorageIds } from './ReplicationStreamStorageIds.js';
import { PowerSyncMongo } from './db.js';
import { getMongoStorageConfig } from './models.js';
import { SyncRuleDocumentV1 } from './v1/models.js';

export class MongoPersistedReplicationStream extends storage.PersistedReplicationStream {
  public current_lock: MongoSyncRulesLock | null = null;
  public readonly syncConfigContent: readonly MongoPersistedSyncConfigContentBase[];

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
      replicationStreamId: doc._id,
      replicationStreamName: doc.slot_name ?? `powersync_${doc._id}`,
      state: doc.state,
      storageVersion,
      replicationJobId
    });

    this.syncConfigContent = this.createSyncConfigContent();
  }

  getStorageConfig() {
    return getMongoStorageConfig(this.storageVersion);
  }

  private createSyncConfigContent(): MongoPersistedSyncConfigContentBase[] {
    if (this.getStorageConfig().incrementalReprocessing) {
      if (this.configs.length == 0) {
        throw new ServiceAssertionError(`Cannot create v3 storage without sync config definitions`);
      }
      return this.configs.map(
        (config) => new MongoPersistedSyncConfigContentV3(this.db, this.doc as ReplicationStreamDocumentV3, config)
      );
    }

    return [new MongoPersistedSyncConfigContentV1(this.db, this.doc as SyncRuleDocumentV1)];
  }

  get syncConfigIds(): bson.ObjectId[] {
    return this.configs.map((config) => config._id);
  }

  #storageIds: ReplicationStreamStorageIds | undefined;

  /**
   * Persisted storage ids for all sync configs in this replication stream.
   *
   * Derived from the persisted rule_mapping documents - requires no parsed sync configs.
   */
  get storageIds(): ReplicationStreamStorageIds {
    this.#storageIds ??= new ReplicationStreamStorageIds(this.syncConfigContent.map((content) => content.mapping));
    return this.#storageIds;
  }

  /**
   * Parse the sync configs for this replication stream.
   *
   * This creates a fresh parse on every call. Do not call this from operational paths -
   * use {@link MongoSyncBucketStorage.getParsedSyncConfigSet} instead, which returns a
   * canonical instance so that parsed source objects and mappings stay associated.
   */
  parsed(options: storage.ParseSyncConfigOptions): MongoParsedSyncConfigSet {
    const storageConfig = this.getStorageConfig();
    if (!storageConfig.incrementalReprocessing) {
      return this.syncConfigContent[0].parsed(options);
    }

    const syncConfigs = this.configs.map((config) => {
      return {
        syncConfigId: config._id.toHexString(),
        syncConfig: storage.parsePersistedSyncConfigContent({
          content: config.content,
          compiledPlan: config.serialized_plan ?? null,
          storageVersion: this.storageVersion,
          parseOptions: options
        }),
        mapping: SingleSyncConfigBucketDefinitionMapping.fromSyncConfig(config)
      };
    });

    return new MongoParsedSyncConfigSet(
      this.replicationStreamId,
      storageConfig,
      this.replicationStreamName,
      syncConfigs
    );
  }

  async lock(session?: mongo.ClientSession) {
    const lock = await MongoSyncRulesLock.createLock(this.db.versioned(this.getStorageConfig()), this, session);
    this.current_lock = lock;
    return lock;
  }
}
