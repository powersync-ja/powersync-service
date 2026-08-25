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
import { stopReplicationStreamPipeline } from './implementation/SyncRuleStateUpdate.js';
import { SyncRuleDocumentV1 } from './implementation/v1/models.js';
import { ObjectStorage } from './implementation/v3/object-storage/ObjectStorage.js';
import { ObjectStorageUsage } from './implementation/v3/object-storage/ObjectStorageUsage.js';
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
  defaultStorageVersion?: number;
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
    await this.withTransaction(async (session) => {
      const next = await this.getDeployingSyncConfigInternal(session);
      const active = await this.getActiveSyncConfigInternal(session);

      if (next != null && next.content.replicationStreamId == replicationStreamId) {
        // We need to redo the "next" replication stream.
        // This creates a new stream, and stops the existing PROCESSING one.
        await this.updateSyncRulesInTransaction(
          next.content.asUpdateOptions({ forceNewReplicationStream: true }),
          session
        );
        const sharedActiveStream = active?.content.replicationStreamId == replicationStreamId;
        if (sharedActiveStream) {
          // The same stream was also used for the ACTIVE config. Transition that to ERRORED
          await this.errorActiveStreamForReplacement(active, session);
        }
      } else if (next == null && active?.content.replicationStreamId == replicationStreamId) {
        // Slot removed for "active" replication stream, while there is no "next" one.
        await this.updateSyncRulesInTransaction(
          active.content.asUpdateOptions({ forceNewReplicationStream: true }),
          session
        );

        // In this case we keep the old one as active for clients, so that that existing clients
        // can still get the latest data while we replicate the new ones.
        // It will however not replicate anymore.
        await this.errorActiveStreamForReplacement(active, session);
      } else if (next != null && active?.content.replicationStreamId == replicationStreamId) {
        // Already have next replication stream, but need to stop replicating the active one.
        await this.errorActiveStreamForReplacement(active, session);
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
          state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
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
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] },
        sync_configs: {
          // Confirm that this is still the ACTIVE/ERRORED one.
          $elemMatch: {
            _id: activeConfigObjectId,
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
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
          {
            'activeConfig._id': activeConfigObjectId,
            'activeConfig.state': { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
          },
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
      options.storageVersion ??
      options.config.parsed.config.storageVersion ??
      this.options.defaultStorageVersion ??
      storage.CURRENT_STORAGE_VERSION;
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
        await this.stopExistingProcessingWork(session);
        return await this.appendSyncConfigToStream({
          versioned,
          existing: active,
          existingConfigDocs,
          options,
          storageVersion,
          session
        });
      }
    }

    await this.stopExistingProcessingWork(session);

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
    const rules = new MongoPersistedReplicationStream(this.db, doc, [syncConfigDoc]);
    if (options.lock) {
      // We only lock when creating a new stream - otherwise we'll likely get lock contention
      // from an existing job on the stream, which would fail the entire transaction.
      // The lock is persisted on rules.current_lock
      await rules.lock(session);
    }
    return rules;
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
    await this.withTransaction(async (session) => {
      rules = await this.updateSyncRulesInTransaction(options, session);
    });
    await this.db.notifyCheckpoint();
    return rules!;
  }

  private async withTransaction<T>(callback: (session: mongo.ClientSession) => Promise<T>): Promise<T> {
    return this.client.withSession((session) => session.withTransaction(callback));
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
    await this.stopExistingProcessingWork(session);

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

    // All V1 replication streams share both the `main` op id sequence and the `bucket_parameters`
    // collection, so every parameter entry this stream writes gets an op id above the current
    // head. Seeding the parameter compaction cursor with that head keeps the stream's first
    // compaction from scanning other streams' history, which would otherwise be repeated for
    // every new deployment. A concurrent replication flush can only advance the head after this
    // read, which makes the seed conservative, never too high.
    const opSequence = await this.db.op_id_sequence.findOne({ _id: 'main' }, { session });

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
      last_keepalive_ts: null,
      parameter_compaction: { compacted_before: opSequence?.op_id ?? 0n }
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
   * Stop top-level and embedded processing work before creating a replacement stream.
   *
   * Use in the same transaction that the replacement stream is created.
   */
  private async stopExistingProcessingWork(session: mongo.ClientSession) {
    await this.db.sync_rules.updateMany(
      {
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] },
        'sync_configs.state': storage.SyncRuleState.PROCESSING
      },
      {
        $set: {
          'sync_configs.$[config].state': storage.SyncRuleState.STOP
        }
      },
      {
        session,
        arrayFilters: [{ 'config.state': storage.SyncRuleState.PROCESSING }]
      }
    );

    await this.db.sync_rules.updateMany(
      {
        state: storage.SyncRuleState.PROCESSING
      },
      stopReplicationStreamPipeline(),
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
      if (lib_mongo.isMongoNamespaceNotFoundError(e)) {
        // Collection doesn't exist - return 0
        return [{ storageStats: { size: 0 } }];
      } else {
        return Promise.reject(e);
      }
    };

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
    const v3OperationCollections = await this.db.listBucketDataCollectionsV3();
    const v3_operation_aggregates = await Promise.all(
      v3OperationCollections.map((collection) =>
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

    const v3ParameterCollections = await this.db.listAllParameterIndexCollectionsV3();
    const v3_parameter_aggregates = await Promise.all(
      v3ParameterCollections.map((collection) =>
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

    const v3SourceRecordCollections = await this.db.listAllSourceRecordCollectionsV3();
    const source_record_aggregates = await Promise.all(
      v3SourceRecordCollections.map((collection) =>
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

    const v3StorageConfig = getMongoStorageConfig(storage.STORAGE_VERSION_3) as StorageConfig & {
      incrementalReprocessing: true;
    };
    const v3Db = this.db.versioned(v3StorageConfig) as VersionedPowerSyncMongoV3;
    const objectStorageDefinitionUsage = await ObjectStorageUsage.readAllDefinitionUsage(v3Db);

    const v3StreamDocs = (await this.db.sync_rules
      .find({ storage_version: { $gte: storage.STORAGE_VERSION_3 } }, { projection: { _id: 1, sync_configs: 1 } })
      .toArray()) as unknown as Pick<ReplicationStreamDocumentV3, '_id' | 'sync_configs'>[];

    const collectionSizes = new Map<string, number>();
    const addCollectionSize = (
      collection: { collectionName: string },
      aggregate: { storageStats?: { size?: number | bigint } }[],
      prefix: string,
      sizes: Map<string, number>
    ) => {
      const match = collection.collectionName.match(new RegExp(`^${prefix}(\\d+)_(.+)$`));
      if (match == null) {
        return;
      }
      sizes.set(`${prefix}${match[1]}:${match[2]}`, Number(aggregate[0]?.storageStats?.size ?? 0));
    };

    v3OperationCollections.forEach((collection, index) =>
      addCollectionSize(collection, v3_operation_aggregates[index], 'bucket_data_', collectionSizes)
    );
    v3ParameterCollections.forEach((collection, index) =>
      addCollectionSize(collection, v3_parameter_aggregates[index], 'parameter_index_', collectionSizes)
    );
    v3SourceRecordCollections.forEach((collection, index) =>
      addCollectionSize(collection, source_record_aggregates[index], 'source_records_', collectionSizes)
    );

    const syncConfigIds = v3StreamDocs.flatMap((stream) => (stream.sync_configs ?? []).map((config) => config._id));
    const syncConfigDefinitions =
      syncConfigIds.length == 0
        ? []
        : await v3Db.syncConfigDefinitions
            .find({ _id: { $in: syncConfigIds } }, { projection: { _id: 1, rule_mapping: 1 } })
            .toArray();
    const syncConfigDefinitionsById = new Map(
      syncConfigDefinitions.map((definition) => [definition._id.toHexString(), definition])
    );

    const sourceTablesByStream = new Map<
      number,
      { _id: ObjectId; bucket_data_source_ids: string[]; parameter_lookup_source_ids: string[] }[]
    >();
    await Promise.all(
      v3StreamDocs.map(async (stream) => {
        const sourceTables = await v3Db
          .sourceTables(stream._id)
          .find({}, { projection: { _id: 1, bucket_data_source_ids: 1, parameter_lookup_source_ids: 1 } })
          .toArray();
        sourceTablesByStream.set(stream._id, sourceTables);
      })
    );

    const objectStorageSizeByDefinition = new Map<string, number>();
    for (const usage of objectStorageDefinitionUsage) {
      objectStorageSizeByDefinition.set(
        `${usage.replication_stream_id}:${usage.definition_id}`,
        Number(usage.active_bytes)
      );
    }

    const sumCollectionSizes = (prefix: string, streamId: number, ids: ReadonlySet<string>) =>
      [...ids].reduce((total, id) => total + (collectionSizes.get(`${prefix}${streamId}:${id}`) ?? 0), 0);

    const syncConfigMetrics: storage.StorageSyncConfigMetrics[] = [];
    for (const stream of v3StreamDocs) {
      const sourceTables = sourceTablesByStream.get(stream._id) ?? [];
      for (const syncConfig of stream.sync_configs ?? []) {
        const definition = syncConfigDefinitionsById.get(syncConfig._id.toHexString());
        if (definition == null) {
          continue;
        }

        const mapping = SingleSyncConfigBucketDefinitionMapping.fromPersistedMapping(definition.rule_mapping);
        const bucketDefinitionIds = mapping.allBucketDefinitionIds();
        const parameterIndexIds = mapping.allParameterIndexIds();
        const bucketDefinitionIdSet = new Set(bucketDefinitionIds);
        const parameterIndexIdSet = new Set(parameterIndexIds);
        const replicationSize = sourceTables.reduce((total, sourceTable) => {
          if (
            !(sourceTable.bucket_data_source_ids ?? []).some((id) => bucketDefinitionIdSet.has(id)) &&
            !(sourceTable.parameter_lookup_source_ids ?? []).some((id) => parameterIndexIdSet.has(id))
          ) {
            return total;
          }
          return total + (collectionSizes.get(`source_records_${stream._id}:${sourceTable._id.toHexString()}`) ?? 0);
        }, 0);

        syncConfigMetrics.push({
          sync_config_id: syncConfig._id.toHexString(),
          sync_config_state: String(syncConfig.state),
          attributed_bucket_data_bytes: sumCollectionSizes('bucket_data_', stream._id, bucketDefinitionIdSet),
          attributed_parameter_indexes_bytes: sumCollectionSizes('parameter_index_', stream._id, parameterIndexIdSet),
          attributed_source_records_bytes: replicationSize,
          attributed_object_storage_bytes: [...bucketDefinitionIdSet].reduce(
            (total, definitionId) => total + (objectStorageSizeByDefinition.get(`${stream._id}:${definitionId}`) ?? 0),
            0
          )
        });
      }
    }

    const totalObjectStorageSize = objectStorageDefinitionUsage.reduce(
      (total, usage) => total + usage.active_bytes,
      0n
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
        source_record_aggregates.reduce((total, aggregate) => total + Number(aggregate[0]?.storageStats?.size ?? 0), 0),
      object_storage_size_bytes: Number(totalObjectStorageSize),
      sync_config_metrics: syncConfigMetrics
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
