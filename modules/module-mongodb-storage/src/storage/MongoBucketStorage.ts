import { GetIntanceOptions, LEGACY_STORAGE_VERSION, storage } from '@powersync/service-core';

import { DO_NOT_LOG, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { v4 as uuid } from 'uuid';

import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';

import { CompatibilityContext } from '@powersync/service-sync-rules';
import { ObjectId } from 'bson';
import { generateReplicationStreamName } from '../utils/util.js';
import { BucketDefinitionMapping } from './implementation/BucketDefinitionMapping.js';
import type { MongoSyncBucketStorage } from './implementation/createMongoSyncBucketStorage.js';
import { createMongoSyncBucketStorage } from './implementation/createMongoSyncBucketStorage.js';
import { PowerSyncMongo } from './implementation/db.js';
import { getMongoStorageConfig, StorageConfig, SyncRuleDocumentBase } from './implementation/models.js';
import { MongoChecksumOptions } from './implementation/MongoChecksums.js';
import { MongoPersistedReplicationStream } from './implementation/MongoPersistedReplicationStream.js';
import { syncRuleStateUpdatePipeline } from './implementation/SyncRuleStateUpdate.js';
import { SyncRuleDocumentV1 } from './implementation/v1/models.js';
import { VersionedPowerSyncMongoV3 } from './implementation/v3/VersionedPowerSyncMongoV3.js';
import { ReplicationStreamDocumentV3, SyncConfigDefinition, SyncRuleConfigStateV3 } from './storage-index.js';

export interface MongoBucketStorageOptions {
  checksumOptions?: Omit<MongoChecksumOptions, 'storageConfig' | 'mapping'>;
}

export class MongoBucketStorage extends storage.BucketStorageFactory {
  [DO_NOT_LOG] = true;

  private readonly client: mongo.MongoClient;
  private readonly session: mongo.ClientSession;
  // TODO: This is still Postgres specific and needs to be reworked
  public readonly slot_name_prefix: string;

  private activeStorageCache: MongoSyncBucketStorage | undefined;

  public readonly db: PowerSyncMongo;

  constructor(
    db: PowerSyncMongo,
    options: {
      slot_name_prefix: string;
    },
    private internalOptions?: MongoBucketStorageOptions
  ) {
    super();
    this.client = db.client;
    this.db = db;
    this.session = this.client.startSession();
    this.slot_name_prefix = options.slot_name_prefix;
  }

  async [Symbol.asyncDispose]() {
    // No-op
  }

  getInstance(
    replicationStream: storage.PersistedReplicationStream,
    options?: GetIntanceOptions
  ): MongoSyncBucketStorage {
    if (!(replicationStream instanceof MongoPersistedReplicationStream)) {
      throw new Error(`Expected MongoPersistedReplicationStream`);
    }
    let { replicationStreamId, replicationStreamName } = replicationStream;
    if ((typeof replicationStreamId as any) == 'bigint') {
      replicationStreamId = Number(replicationStreamId);
    }
    const storageConfig = replicationStream.getStorageConfig();
    const syncRuleStorage = createMongoSyncBucketStorage(
      this,
      replicationStreamId,
      replicationStream,
      replicationStreamName,
      undefined,
      {
        ...this.internalOptions,
        storageConfig
      }
    );
    if (!options?.skipLifecycleHooks) {
      this.iterateListeners((cb) => cb.syncStorageCreated?.(syncRuleStorage));
    }

    syncRuleStorage.registerListener({
      batchStarted: (batch) => {
        batch.registerListener({
          replicationEvent: (payload) => this.iterateListeners((cb) => cb.replicationEvent?.(payload))
        });
      }
    });
    return syncRuleStorage;
  }

  async getSystemIdentifier(): Promise<storage.BucketStorageSystemIdentifier> {
    const { setName: id } = await this.db.db.command({
      hello: 1
    });
    if (id == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1342,
        'Standalone MongoDB instances are not supported - use a replicaset.'
      );
    }

    return {
      id,
      type: lib_mongo.MONGO_CONNECTION_TYPE
    };
  }

  async restartReplication(replicationStreamId: number) {
    const next = await this.getDeployingSyncConfig();
    const active = await this.getActiveSyncConfig();

    if (next != null && next.content.replicationStreamId == replicationStreamId) {
      // We need to redo the "next" replication stream
      await this.updateSyncRules(next.content.asUpdateOptions());
      // Pro-actively stop replicating
      await this.db.sync_rules.updateOne(
        {
          _id: next.content.replicationStreamId,
          state: storage.SyncRuleState.PROCESSING
        },
        syncRuleStateUpdatePipeline(storage.SyncRuleState.STOP)
      );
      await this.db.notifyCheckpoint();
    } else if (next == null && active?.content.replicationStreamId == replicationStreamId) {
      // Slot removed for "active" replication stream, while there is no "next" one.
      await this.updateSyncRules(active.content.asUpdateOptions());

      // In this case we keep the old one as active for clients, so that that existing clients
      // can still get the latest data while we replicate the new ones.
      // It will however not replicate anymore.

      await this.db.sync_rules.updateOne(
        {
          _id: active.content.replicationStreamId,
          state: storage.SyncRuleState.ACTIVE
        },
        syncRuleStateUpdatePipeline(storage.SyncRuleState.ERRORED)
      );
      await this.db.notifyCheckpoint();
    } else if (next != null && active?.content.replicationStreamId == replicationStreamId) {
      // Already have next replication stream, but need to stop replicating the active one.

      await this.db.sync_rules.updateOne(
        {
          _id: active.content.replicationStreamId,
          state: storage.SyncRuleState.ACTIVE
        },
        syncRuleStateUpdatePipeline(storage.SyncRuleState.ERRORED)
      );
      await this.db.notifyCheckpoint();
    }
  }

  private isCompatible(
    replicationStream: ReplicationStreamDocumentV3,
    existingConfig: SyncConfigDefinition[],
    options: storage.UpdateSyncRulesOptions
  ): boolean {
    if (options.config.plan == null) {
      // Only support sync streams with serialized plans
      return false;
    }

    if (replicationStream.storage_version !== options.storageVersion) {
      return false;
    }

    if (existingConfig.length == 0) {
      // Could technically be compatible, but there is no reason to re-use this stream.
      return false;
    }

    if (existingConfig.some((config) => config.serialized_plan == null)) {
      // Only support sync streams with serialized plans
      return false;
    }

    // Technically we can compare the serialized compatibility versions? But this does not add much overhead.
    const first = existingConfig[0];
    const streamCompatibility = CompatibilityContext.deserialize(first.serialized_plan!.compatibility);
    if (!streamCompatibility.equals(options.config.parsed.config.compatibility)) {
      // Compatibility options must match
      return false;
    }

    return true;
  }

  private async updateSyncRulesV3(
    options: storage.UpdateSyncRulesOptions,
    storageVersion: number,
    storageConfig: StorageConfig
  ): Promise<MongoPersistedReplicationStream> {
    let rules: MongoPersistedReplicationStream | undefined = undefined;
    const versioned = this.db.versioned(storageConfig) as VersionedPowerSyncMongoV3;

    const session = this.session;

    await session.withTransaction(async () => {
      const active = await this.db.sync_rules.findOne<ReplicationStreamDocumentV3>(
        {
          state: storage.SyncRuleState.ACTIVE,
          storage_version: storageVersion
        },
        { session, sort: { _id: -1 }, limit: 1 }
      );
      if (active != null) {
        const activeConfigs = active.sync_configs.filter((config) => config.state == storage.SyncRuleState.ACTIVE);
        const activeOnly = { ...active, sync_configs: activeConfigs };
        const existingConfigDocs = await this.loadSyncConfigDefinitions(versioned, activeOnly, session);

        if (this.isCompatible(activeOnly, existingConfigDocs, options)) {
          await this.db.sync_rules.updateMany(
            {
              state: storage.SyncRuleState.PROCESSING
            },
            syncRuleStateUpdatePipeline(storage.SyncRuleState.STOP),
            { session }
          );
          await this.stopEmbeddedDeployingConfigs(active, session);
          rules = await this.appendSyncConfigToStream({
            versioned,
            existing: activeOnly,
            existingConfigDocs,
            options,
            storageVersion,
            session
          });
          return;
        }

        await this.stopEmbeddedDeployingConfigs(active, session);
      }

      // Only have a single replication stream with PROCESSING.
      await this.db.sync_rules.updateMany(
        {
          state: storage.SyncRuleState.PROCESSING
        },
        syncRuleStateUpdatePipeline(storage.SyncRuleState.STOP),
        { session }
      );

      const id_doc = await this.db.op_id_sequence.findOneAndUpdate(
        {
          _id: 'sync_rules'
        },
        {
          $inc: {
            op_id: 1n
          }
        },
        {
          upsert: true,
          returnDocument: 'after',
          session
        }
      );

      const id = Number(id_doc!.op_id);
      const replicationStreamName = generateReplicationStreamName(this.slot_name_prefix, id);

      const mapping =
        options.config.plan == null
          ? // For legacy sync rules and streams, use the parsed config directly to create a mapping
            BucketDefinitionMapping.fromParsedSyncConfig(options.config.parsed)
          : // For new sync streams, always use the serialized version
            BucketDefinitionMapping.constructIncrementalMappingFromSerializedPlans([], options.config.plan.plan, []);

      const syncConfigDoc: SyncConfigDefinition = {
        _id: new ObjectId(),
        replication_stream_id: id,
        created_at: new Date(),
        storage_version: storageVersion,
        content: options.config.yaml,
        serialized_plan: options.config.plan,
        rule_mapping: mapping.serialize()
      };
      await versioned.syncConfigDefinitions.insertOne(syncConfigDoc, { session });

      const doc: ReplicationStreamDocumentV3 = {
        _id: id,
        storage_version: storageVersion,
        sync_configs: [
          {
            _id: syncConfigDoc._id,
            state: storage.SyncRuleState.PROCESSING,
            keepalive_op: null,
            last_checkpoint: null,
            last_checkpoint_lsn: null,
            no_checkpoint_before: null,
            snapshot_done: false
          }
        ],
        snapshot_lsn: undefined,
        state: storage.SyncRuleState.PROCESSING,
        slot_name: replicationStreamName,
        last_checkpoint_ts: null,
        last_fatal_error: null,
        last_fatal_error_ts: null,
        last_keepalive_ts: null
      };

      await this.db.sync_rules.insertOne(doc, { session });
      await this.db.notifyCheckpoint();
      rules = new MongoPersistedReplicationStream(this.db, doc, [syncConfigDoc]);
      if (options.lock) {
        // The lock is persisted on rules.current_lock
        await rules.lock(session);
      }
    });

    return rules!;
  }

  private async loadSyncConfigDefinitions(
    versioned: VersionedPowerSyncMongoV3,
    existing: ReplicationStreamDocumentV3,
    session: mongo.ClientSession
  ) {
    const activeConfigIds = existing.sync_configs
      .filter((config) => [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.PROCESSING].includes(config.state))
      .map((config) => config._id);

    return versioned.syncConfigDefinitions
      .find(
        {
          _id: { $in: activeConfigIds }
        },
        { session }
      )
      .toArray();
  }

  /**
   * Load _all_ definition mappings for a replication stream - used as a base to generate new ids.
   */
  private async loadHistoricalSyncConfigRuleMappings(
    versioned: VersionedPowerSyncMongoV3,
    replicationStreamId: number,
    session: mongo.ClientSession
  ) {
    return versioned.syncConfigDefinitions
      .find(
        {
          replication_stream_id: replicationStreamId
        },
        {
          session,
          projection: {
            rule_mapping: 1
          }
        }
      )
      .toArray();
  }

  private async stopEmbeddedDeployingConfigs(existing: ReplicationStreamDocumentV3, session: mongo.ClientSession) {
    const deployingConfigs = existing.sync_configs
      .filter((config) => config.state == storage.SyncRuleState.PROCESSING)
      .map((config) => config._id);
    if (deployingConfigs.length == 0) {
      return;
    }

    await this.db.sync_rules.updateOne(
      {
        _id: existing._id,
        'sync_configs._id': { $in: deployingConfigs }
      },
      {
        $set: {
          'sync_configs.$[config].state': storage.SyncRuleState.STOP
        }
      },
      {
        session,
        arrayFilters: [{ 'config._id': { $in: deployingConfigs } }]
      }
    );
  }

  private async appendSyncConfigToStream(options: {
    versioned: VersionedPowerSyncMongoV3;
    existing: ReplicationStreamDocumentV3;
    existingConfigDocs: SyncConfigDefinition[];
    options: storage.UpdateSyncRulesOptions;
    storageVersion: number;
    session: mongo.ClientSession;
  }): Promise<MongoPersistedReplicationStream> {
    const { versioned, existing, existingConfigDocs, options: updateOptions, storageVersion, session } = options;
    const compatibleConfigs = existingConfigDocs.map((doc) => ({
      plan: doc.serialized_plan!.plan,
      mapping: BucketDefinitionMapping.fromSyncConfig(doc)
    }));
    const historicalRuleMappings = await this.loadHistoricalSyncConfigRuleMappings(versioned, existing._id, session);
    const reservedMappings = historicalRuleMappings.map((doc) => BucketDefinitionMapping.fromSyncConfig(doc));
    const mapping = BucketDefinitionMapping.constructIncrementalMappingFromSerializedPlans(
      compatibleConfigs,
      updateOptions.config.plan!.plan,
      reservedMappings
    );

    const syncConfigDoc: SyncConfigDefinition = {
      _id: new ObjectId(),
      replication_stream_id: existing._id,
      created_at: new Date(),
      storage_version: storageVersion,
      content: updateOptions.config.yaml,
      serialized_plan: updateOptions.config.plan,
      rule_mapping: mapping.serialize()
    };
    await versioned.syncConfigDefinitions.insertOne(syncConfigDoc, { session });
    const syncConfigState: SyncRuleConfigStateV3 = {
      _id: syncConfigDoc._id,
      state: storage.SyncRuleState.PROCESSING,
      keepalive_op: null,
      last_checkpoint: null,
      last_checkpoint_lsn: null,
      no_checkpoint_before: null,
      snapshot_done: false
    };

    await this.db.sync_rules.updateOne(
      { _id: existing._id },
      {
        $push: {
          sync_configs: syncConfigState
        },
        $set: {
          last_fatal_error: null,
          last_fatal_error_ts: null
        }
      },
      { session }
    );
    await this.db.notifyCheckpoint();
    const stream = new MongoPersistedReplicationStream(
      this.db,
      {
        ...existing,
        sync_configs: [...existing.sync_configs, syncConfigState]
      },
      [...existingConfigDocs, syncConfigDoc]
    );
    if (updateOptions.lock) {
      await stream.lock(session);
    }
    return stream;
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<MongoPersistedReplicationStream> {
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;

    const storageConfig = getMongoStorageConfig(storageVersion);
    if (storageConfig.incrementalReprocessing) {
      return this.updateSyncRulesV3(options, storageVersion, storageConfig);
    }

    let rules: MongoPersistedReplicationStream | undefined = undefined;

    const session = this.session;

    await session.withTransaction(async () => {
      // Only have a single replication stream with PROCESSING.
      await this.db.sync_rules.updateMany(
        {
          state: storage.SyncRuleState.PROCESSING
        },
        syncRuleStateUpdatePipeline(storage.SyncRuleState.STOP),
        { session }
      );

      const id_doc = await this.db.op_id_sequence.findOneAndUpdate(
        {
          _id: 'sync_rules'
        },
        {
          $inc: {
            op_id: 1n
          }
        },
        {
          upsert: true,
          returnDocument: 'after',
          session
        }
      );

      const id = Number(id_doc!.op_id);
      const slot_name = generateReplicationStreamName(this.slot_name_prefix, id);

      const doc: SyncRuleDocumentV1 = {
        _id: id,
        storage_version: storageVersion,
        content: options.config.yaml,
        serialized_plan: options.config.plan,
        last_checkpoint: null,
        last_checkpoint_lsn: null,
        no_checkpoint_before: null,
        keepalive_op: null,
        snapshot_done: false,
        snapshot_lsn: undefined,
        state: storage.SyncRuleState.PROCESSING,
        slot_name: slot_name,
        last_checkpoint_ts: null,
        last_fatal_error: null,
        last_fatal_error_ts: null,
        last_keepalive_ts: null
      };

      await this.db.sync_rules.insertOne(doc, { session });
      await this.db.notifyCheckpoint();
      rules = new MongoPersistedReplicationStream(this.db, doc);
      if (options.lock) {
        // The lock is persisted on rules.current_lock
        await rules.lock(session);
      }
    });

    return rules!;
  }

  async getActiveSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
      },
      { sort: { _id: -1 }, limit: 1 }
    );

    return this.resolvedSyncConfigFromDoc(doc, [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED], {
      cacheActiveStorage: true
    });
  }

  private async replicationStreamFromDoc(doc: SyncRuleDocumentBase | null, stateFilter: storage.SyncRuleState[]) {
    if (doc == null) {
      return null;
    }
    const storageConfig = getMongoStorageConfig(doc.storage_version ?? LEGACY_STORAGE_VERSION);

    if (storageConfig.incrementalReprocessing) {
      const v3 = doc as ReplicationStreamDocumentV3;
      const matching = v3.sync_configs.filter((c) => stateFilter.includes(c.state));
      if (matching.length == 0) {
        return null;
      }

      // TODO: cache the config. It could specifically help for the main replication loop
      // that checks for active replication streams.
      // It is not a major bottleneck though, since it only runs once every couple of seconds at most.
      const db = this.db.versioned(storageConfig) as VersionedPowerSyncMongoV3;
      const syncConfigDocs = await db.syncConfigDefinitions
        .find({
          _id: { $in: matching.map((config) => config._id) }
        })
        .toArray();

      if (syncConfigDocs.length == 0) {
        return null;
      }
      return new MongoPersistedReplicationStream(this.db, v3, syncConfigDocs);
    }

    return new MongoPersistedReplicationStream(this.db, doc as SyncRuleDocumentV1);
  }

  async getDeployingSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        $or: [{ state: storage.SyncRuleState.PROCESSING }, { 'sync_configs.state': storage.SyncRuleState.PROCESSING }]
      },
      { sort: { _id: -1 }, limit: 1 }
    );

    return this.resolvedSyncConfigFromDoc(doc, [storage.SyncRuleState.PROCESSING]);
  }

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    const docs = await this.db.sync_rules
      .find({
        state: { $in: [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE] }
      })
      .toArray();

    return (
      await Promise.all(
        docs.map((doc) => {
          return this.replicationStreamFromDoc(doc, [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE]);
        })
      )
    ).filter((r) => r != null);
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    const docs = await this.db.sync_rules
      .find({
        state: storage.SyncRuleState.STOP
      })
      .toArray();

    return (
      await Promise.all(
        docs.map((doc) => {
          return this.replicationStreamFromDoc(doc, [storage.SyncRuleState.STOP]);
        })
      )
    ).filter((d) => d != null);
  }

  private async resolvedSyncConfigFromDoc(
    doc: SyncRuleDocumentBase | null,
    stateFilter: storage.SyncRuleState[],
    options: { cacheActiveStorage?: boolean } = {}
  ): Promise<storage.ResolvedSyncConfig | null> {
    const stream = await this.replicationStreamFromDoc(doc, stateFilter);
    if (stream == null) {
      return null;
    }

    const content = stream.syncConfigContent[0];
    const thisFactory = this;

    return {
      content,
      replicationStream: stream,
      get storage() {
        // It is important that this instance is cached.
        // Not for the instance construction itself, but to ensure that internal caches on the instance
        // are re-used properly.
        if (
          options.cacheActiveStorage &&
          thisFactory.activeStorageCache?.replicationStream.replicationJobId == stream.replicationJobId
        ) {
          return thisFactory.activeStorageCache;
        }

        const instance = thisFactory.getInstance(stream);
        if (options.cacheActiveStorage) {
          thisFactory.activeStorageCache = instance;
        }
        return instance;
      }
    };
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    const ignoreNotExisting = (e: unknown) => {
      if (lib_mongo.isMongoServerError(e) && e.codeName == 'NamespaceNotFound') {
        // Collection doesn't exist - return 0
        return [{ storageStats: { size: 0 } }];
      } else {
        return Promise.reject(e);
      }
    };

    // For now, we get storage metrics over all v1 and v3 collections.
    // In the future, we may split these metrics to report separately for active replication streams versus processing streams.

    const aggregateStaticCollection = async <T extends mongo.Document>(collection: mongo.Collection<T>) => {
      // We check whether the collection exists before getting the statistics. This avoids repeated
      // errors in the MongoDB logs if the collection hasn't been created yet.
      const exists =
        (await this.db.db.listCollections({ name: collection.collectionName }, { nameOnly: true }).toArray()).length >
        0;
      if (!exists) {
        return [{ storageStats: { size: 0 } }];
      }

      return collection
        .aggregate([
          {
            $collStats: {
              storageStats: {}
            }
          }
        ])
        .toArray()
        .catch(ignoreNotExisting);
    };

    const operations_aggregate = await aggregateStaticCollection(this.db.bucket_data);
    const v3_operation_aggregates = await Promise.all(
      (await this.db.listBucketDataCollectionsV3()).map((collection) =>
        collection
          .aggregate([
            {
              $collStats: {
                storageStats: {}
              }
            }
          ])
          .toArray()
          .catch(ignoreNotExisting)
      )
    );

    const parameters_aggregate = await aggregateStaticCollection(this.db.bucket_parameters);

    const v3_parameter_aggregates = await Promise.all(
      (await this.db.listAllParameterIndexCollectionsV3()).map((collection) =>
        collection
          .aggregate([
            {
              $collStats: {
                storageStats: {}
              }
            }
          ])
          .toArray()
          .catch(ignoreNotExisting)
      )
    );

    const v1_source_record_aggregate = await aggregateStaticCollection(this.db.current_data);

    const source_record_aggregates = await Promise.all(
      (await this.db.listAllSourceRecordCollectionsV3()).map((collection) =>
        collection
          .aggregate([
            {
              $collStats: {
                storageStats: {}
              }
            }
          ])
          .toArray()
          .catch(ignoreNotExisting)
      )
    );
    return {
      operations_size_bytes:
        Number(operations_aggregate[0].storageStats.size) +
        v3_operation_aggregates.reduce((total, aggregate) => total + Number(aggregate[0].storageStats.size), 0),
      parameters_size_bytes:
        Number(parameters_aggregate[0].storageStats.size) +
        v3_parameter_aggregates.reduce((total, aggregate) => total + Number(aggregate[0].storageStats.size), 0),
      replication_size_bytes:
        Number(v1_source_record_aggregate[0]?.storageStats?.size ?? 0) +
        source_record_aggregates.reduce((total, aggregate) => total + Number(aggregate[0]?.storageStats?.size ?? 0), 0)
    };
  }

  async getPowerSyncInstanceId(): Promise<string> {
    let instance = await this.db.instance.findOne({
      _id: { $exists: true }
    });

    if (!instance) {
      const manager = new lib_mongo.locks.MongoLockManager({
        collection: this.db.locks,
        name: `instance-id-insertion-lock`
      });

      await manager.lock(async () => {
        await this.db.instance.insertOne({
          _id: uuid()
        });
      });

      instance = await this.db.instance.findOne({
        _id: { $exists: true }
      });
    }

    return instance!._id;
  }
}
