import {
  GetIntanceOptions,
  LEGACY_STORAGE_VERSION,
  SingleSyncConfigBucketDefinitionMapping,
  storage
} from '@powersync/service-core';

import {
  DO_NOT_LOG,
  ErrorCode,
  logger,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import { v4 as uuid } from 'uuid';

import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';

import {
  describeIncrementalSyncConfigUpdate,
  formatIncrementalSyncConfigUpdateLog,
  isCompatible
} from '@powersync/service-core';
import { ObjectId } from 'bson';
import { DEFAULT_CLEAR_BATCH_THROTTLE_RATE } from '../types/types.js';
import { generateReplicationStreamName } from '../utils/util.js';
import type { MongoSyncBucketStorage } from './implementation/createMongoSyncBucketStorage.js';
import { createMongoSyncBucketStorage } from './implementation/createMongoSyncBucketStorage.js';
import { PowerSyncMongo } from './implementation/db.js';
import { getMongoStorageConfig, StorageConfig, SyncRuleDocumentBase } from './implementation/models.js';
import { MongoChecksumOptions } from './implementation/MongoChecksums.js';
import { MongoPersistedReplicationStream } from './implementation/MongoPersistedReplicationStream.js';
import { stopProcessingSyncRuleStateUpdatePipeline } from './implementation/SyncRuleStateUpdate.js';
import { SyncRuleDocumentV1 } from './implementation/v1/models.js';
import { ObjectStorage } from './implementation/v3/object-storage/ObjectStorage.js';
import { VersionedPowerSyncMongoV3 } from './implementation/v3/VersionedPowerSyncMongoV3.js';
import { ReplicationStreamDocumentV3, SyncConfigDefinition, SyncRuleConfigStateV3 } from './storage-index.js';

export interface MongoBucketStorageOptions {
  checksumOptions?: Omit<MongoChecksumOptions, 'storageConfig'>;
  objectStorage?: ObjectStorage;
  inlineThresholdBytes?: number;
  /**
   * Prefix for replication stream name and Postgres logical replication slot name.
   */
  replicationStreamNamePrefix: string;
  readPreference?: mongo.ReadPreference;
  checksumCacheTtlMs?: number;
  clearBatchThrottleRate?: number;
  /**
   * Reuse a compatible active replication stream by appending a new sync config.
   *
   * This currently requires source replication support. MongoDB sources can process multiple
   * sync configs in one replication stream, but other source connectors still expect a single
   * sync config per stream.
   */
  supportsMultipleSyncConfigs?: boolean;
}

export class MongoBucketStorage extends storage.BucketStorageFactory {
  [DO_NOT_LOG] = true;

  private readonly client: mongo.MongoClient;
  private readonly session: mongo.ClientSession;
  public readonly replicationStreamNamePrefix: string;

  private activeStorageCache: MongoSyncBucketStorage | undefined;

  public readonly db: PowerSyncMongo;

  constructor(
    db: PowerSyncMongo,
    private options: MongoBucketStorageOptions
  ) {
    super();
    this.client = db.client;
    this.db = db;
    this.session = this.client.startSession();
    this.replicationStreamNamePrefix = options.replicationStreamNamePrefix;
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
        checksumOptions: this.options.checksumOptions,
        readPreference: this.options.readPreference,
        checksumCacheTtlMs: this.options.checksumCacheTtlMs,
        storageConfig,
        objectStorage: this.options.objectStorage,
        inlineThresholdBytes: this.options.inlineThresholdBytes,
        clearBatchThrottleRate: this.options.clearBatchThrottleRate ?? DEFAULT_CLEAR_BATCH_THROTTLE_RATE
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
    await this.session.withTransaction(async () => {
      const next = await this.getDeployingSyncConfigInternal(this.session);
      const active = await this.getActiveSyncConfigInternal(this.session);

      if (next != null && next.content.replicationStreamId == replicationStreamId) {
        // We need to redo the "next" replication stream.
        // This creates a new stream.
        await this.updateSyncRulesInTransaction(
          next.content.asUpdateOptions({ forceNewReplicationStream: true }),
          this.session
        );
        const sharedActiveStream = active?.content.replicationStreamId == replicationStreamId;
        if (sharedActiveStream) {
          await this.errorActiveStreamForReplacement(active, this.session);
        } else {
          // A separate deploying stream no longer needs replication after its replacement exists.
          await this.db.sync_rules.updateOne(
            {
              _id: next.content.replicationStreamId,
              state: storage.SyncRuleState.PROCESSING
            },
            stopProcessingSyncRuleStateUpdatePipeline(),
            { session: this.session }
          );
        }
      } else if (next == null && active?.content.replicationStreamId == replicationStreamId) {
        // Slot removed for "active" replication stream, while there is no "next" one.
        await this.updateSyncRulesInTransaction(
          active.content.asUpdateOptions({ forceNewReplicationStream: true }),
          this.session
        );

        // In this case we keep the old one as active for clients, so that that existing clients
        // can still get the latest data while we replicate the new ones.
        // It will however not replicate anymore.
        await this.errorActiveStreamForReplacement(active, this.session);
      } else if (next != null && active?.content.replicationStreamId == replicationStreamId) {
        // Already have next replication stream, but need to stop replicating the active one.
        await this.errorActiveStreamForReplacement(active, this.session);
      } else {
        // replicationStreamId does not match the ACTIVE or PROCESSING streams - no-op.
      }
    });
    await this.db.notifyCheckpoint();
  }

  /**
   * Keep an active config readable after its replication stream is invalidated.
   *
   * Incremental V3 streams can retain stopped historical configs and embed a deploying config.
   * Read-side storage requires exactly one ACTIVE or ERRORED config, so only the active config
   * transitions to ERRORED; every other embedded config remains or becomes STOP.
   */
  private async errorActiveStreamForReplacement(active: storage.ResolvedSyncConfig, session: mongo.ClientSession) {
    const activeSyncConfigId = active.content.syncConfigId;
    if (activeSyncConfigId == null) {
      // V1 documents
      const result = await this.db.sync_rules.updateOne(
        {
          _id: active.content.replicationStreamId,
          state: storage.SyncRuleState.ACTIVE
        },
        {
          $set: {
            state: storage.SyncRuleState.ERRORED
          }
        },
        { session }
      );
      if (result.matchedCount != 1) {
        // This should generally not happen, since the entire process runs in a transaction.
        // The filters just double-check that the state is what we expect, and fails hard otherwise.
        throw new ReplicationAssertionError(
          `Active replication stream ${active.content.replicationStreamId} changed during restart`
        );
      }
      return;
    }

    const activeConfigObjectId = new ObjectId(activeSyncConfigId);
    const result = await this.db.sync_rules.updateOne(
      {
        _id: active.content.replicationStreamId,
        state: storage.SyncRuleState.ACTIVE,
        sync_configs: {
          // Confirm that this is still the ACTIVE one.
          $elemMatch: {
            _id: activeConfigObjectId,
            state: storage.SyncRuleState.ACTIVE
          }
        }
      },
      {
        $set: {
          state: storage.SyncRuleState.ERRORED,
          // There must be exactly one config with an ERRORED state: This will be used to serve
          // clients until there is a new ACTIVE one.
          'sync_configs.$[activeConfig].state': storage.SyncRuleState.ERRORED,
          // PROCESSING configs must stop. Historical STOP configs remain unchanged.
          'sync_configs.$[processingConfig].state': storage.SyncRuleState.STOP
        }
      },
      {
        session,
        arrayFilters: [
          { 'activeConfig._id': activeConfigObjectId, 'activeConfig.state': storage.SyncRuleState.ACTIVE },
          {
            'processingConfig._id': { $ne: activeConfigObjectId },
            'processingConfig.state': storage.SyncRuleState.PROCESSING
          }
        ]
      }
    );
    if (result.matchedCount != 1) {
      // This should generally not happen, since the entire process runs in a transaction.
      // The filters just double-check that the state is what we expect, and fails hard otherwise.
      throw new ReplicationAssertionError(
        `Active replication stream ${active.content.replicationStreamId} changed during restart`
      );
    }
  }

  private async updateSyncRulesInTransaction(options: storage.UpdateSyncRulesOptions, session: mongo.ClientSession) {
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
    const storageConfig = getMongoStorageConfig(storageVersion);
    if (storageConfig.incrementalReprocessing) {
      return this.updateSyncConfigV3InTransaction(options, storageVersion, storageConfig, session);
    }
    return this.updateSyncConfigV1InTransaction(options, storageVersion, session);
  }

  /**
   * Update to a V3 sync config.
   *
   * Does support cases where existing replication streams are on V1.
   */
  private async updateSyncConfigV3InTransaction(
    options: storage.UpdateSyncRulesOptions,
    storageVersion: number,
    storageConfig: StorageConfig,
    session: mongo.ClientSession
  ): Promise<MongoPersistedReplicationStream> {
    let rules: MongoPersistedReplicationStream | undefined = undefined;
    const versioned = this.db.versioned(storageConfig) as VersionedPowerSyncMongoV3;

    const active = await this.db.sync_rules.findOne<ReplicationStreamDocumentV3>(
      {
        state: storage.SyncRuleState.ACTIVE,
        storage_version: storageVersion
      },
      { session, sort: { _id: -1 }, limit: 1 }
    );
    if (active != null) {
      const existingConfigDocs = await this.loadSyncConfigDefinitions(versioned, active, session);

      if (
        !options.forceNewReplicationStream &&
        this.options.supportsMultipleSyncConfigs &&
        isCompatible(
          existingConfigDocs.map((d) => d.serialized_plan ?? null),
          options.config,
          logger
        )
      ) {
        logger.info(`Using incremental reprocessing`);
        await this.stopProcessingReplicationStreams(session);
        await this.stopEmbeddedProcessingConfigs(active, session);
        rules = await this.appendSyncConfigToStream({
          versioned,
          existing: active,
          existingConfigDocs,
          options,
          storageVersion,
          session
        });
        return rules;
      }

      await this.stopEmbeddedProcessingConfigs(active, session);
    }

    await this.stopProcessingReplicationStreams(session);

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
    const replicationStreamName = generateReplicationStreamName(this.replicationStreamNamePrefix, id);

    const mapping =
      options.config.plan == null
        ? // For legacy sync rules and streams, use the parsed config directly to create a mapping
          SingleSyncConfigBucketDefinitionMapping.fromParsedSyncConfig(options.config.parsed)
        : // For new sync streams, always use the serialized version
          SingleSyncConfigBucketDefinitionMapping.constructIncrementalMappingFromSerializedPlans(
            [],
            options.config.plan.plan,
            []
          );

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
    rules = new MongoPersistedReplicationStream(this.db, doc, [syncConfigDoc]);
    if (options.lock) {
      // The lock is persisted on rules.current_lock
      await rules.lock(session);
    }
    return rules!;
  }

  private async loadSyncConfigDefinitions(
    versioned: VersionedPowerSyncMongoV3,
    existing: ReplicationStreamDocumentV3,
    session: mongo.ClientSession
  ) {
    const activeConfigIds = existing.sync_configs
      .filter((config) => config.state == storage.SyncRuleState.ACTIVE)
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

  /**
   * An ACTIVE stream may contain sync configs that are PROCESSING. Stop them.
   */
  private async stopEmbeddedProcessingConfigs(existing: ReplicationStreamDocumentV3, session: mongo.ClientSession) {
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

  private logIncrementalDefinitionChanges(changes: ReturnType<typeof describeIncrementalSyncConfigUpdate>) {
    logger.info(`Incremental reprocessing sync config update:\n${formatIncrementalSyncConfigUpdateLog(changes)}`);
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
      mapping: SingleSyncConfigBucketDefinitionMapping.fromPersistedMapping(doc.rule_mapping)
    }));
    const historicalRuleMappings = await this.loadHistoricalSyncConfigRuleMappings(versioned, existing._id, session);
    const reservedMappings = historicalRuleMappings.map((doc) =>
      SingleSyncConfigBucketDefinitionMapping.fromPersistedMapping(doc.rule_mapping)
    );
    const mappingResult = SingleSyncConfigBucketDefinitionMapping.constructIncrementalMappingWithChanges(
      compatibleConfigs,
      updateOptions.config.plan!.plan,
      reservedMappings
    );
    const mapping = mappingResult.mapping;
    this.logIncrementalDefinitionChanges(
      describeIncrementalSyncConfigUpdate({
        activeMappings: existingConfigDocs.map((doc) =>
          SingleSyncConfigBucketDefinitionMapping.fromPersistedMapping(doc.rule_mapping)
        ),
        newMapping: mapping,
        newSyncConfig: updateOptions.config.parsed,
        mappingChanges: mappingResult.changes
      })
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
    const syncConfigStates = [
      ...existing.sync_configs.filter((config) => config.state == storage.SyncRuleState.ACTIVE),
      syncConfigState
    ];
    const stream = new MongoPersistedReplicationStream(
      this.db,
      {
        ...existing,
        sync_configs: syncConfigStates
      },
      [...existingConfigDocs, syncConfigDoc]
    );
    // The stream already exists, so an active replication job may already hold the stream lock.
    // Deployment only persists the appended sync config; replication job locking is handled by
    // the replicator.
    return stream;
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<MongoPersistedReplicationStream> {
    let rules: MongoPersistedReplicationStream | undefined;
    await this.session.withTransaction(async () => {
      rules = await this.updateSyncRulesInTransaction(options, this.session);
    });
    await this.db.notifyCheckpoint();
    return rules!;
  }

  /**
   * Update to a V1 sync config.
   *
   * Does support cases where existing replication streams are on V3.
   */
  private async updateSyncConfigV1InTransaction(
    options: storage.UpdateSyncRulesOptions,
    storageVersion: number,
    session: mongo.ClientSession
  ): Promise<MongoPersistedReplicationStream> {
    await this.stopProcessingReplicationStreams(session);

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
    const slot_name = generateReplicationStreamName(this.replicationStreamNamePrefix, id);

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
    const rules = new MongoPersistedReplicationStream(this.db, doc);
    if (options.lock) {
      // The lock is persisted on rules.current_lock
      await rules.lock(session);
    }
    return rules;
  }

  /**
   * When we create a new "PROCESSING" replication stream, we need to stop all others.
   */
  private async stopProcessingReplicationStreams(session: mongo.ClientSession) {
    await this.db.sync_rules.updateMany(
      {
        state: storage.SyncRuleState.PROCESSING
      },
      stopProcessingSyncRuleStateUpdatePipeline(),
      { session }
    );
  }

  async getActiveSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    return this.getActiveSyncConfigInternal();
  }

  private async getActiveSyncConfigInternal(session?: mongo.ClientSession): Promise<storage.ResolvedSyncConfig | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
      },
      { session, sort: { _id: -1 }, limit: 1 }
    );

    return this.resolvedSyncConfigFromDoc(
      doc,
      [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED],
      { cacheActiveStorage: session == null },
      session
    );
  }

  private async replicationStreamFromDoc(
    doc: SyncRuleDocumentBase | null,
    stateFilter: storage.SyncRuleState[],
    session?: mongo.ClientSession
  ) {
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
        .find(
          {
            _id: { $in: matching.map((config) => config._id) }
          },
          { session }
        )
        .toArray();

      if (syncConfigDocs.length == 0) {
        return null;
      }
      return new MongoPersistedReplicationStream(this.db, v3, syncConfigDocs);
    }

    return new MongoPersistedReplicationStream(this.db, doc as SyncRuleDocumentV1);
  }

  async getDeployingSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    return this.getDeployingSyncConfigInternal();
  }

  private async getDeployingSyncConfigInternal(
    session?: mongo.ClientSession
  ): Promise<storage.ResolvedSyncConfig | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        $or: [{ state: storage.SyncRuleState.PROCESSING }, { 'sync_configs.state': storage.SyncRuleState.PROCESSING }]
      },
      { session, sort: { _id: -1 }, limit: 1 }
    );

    return this.resolvedSyncConfigFromDoc(doc, [storage.SyncRuleState.PROCESSING], {}, session);
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
    options: { cacheActiveStorage?: boolean } = {},
    session?: mongo.ClientSession
  ): Promise<storage.ResolvedSyncConfig | null> {
    const stream = await this.replicationStreamFromDoc(doc, stateFilter, session);
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
