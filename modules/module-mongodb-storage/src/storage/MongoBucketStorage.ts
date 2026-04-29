import { GetIntanceOptions, LEGACY_STORAGE_VERSION, storage } from '@powersync/service-core';

import { DO_NOT_LOG, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { v4 as uuid } from 'uuid';

import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';

import { ObjectId } from 'bson';
import { generateSlotName } from '../utils/util.js';
import { BucketDefinitionMapping } from './implementation/BucketDefinitionMapping.js';
import type { MongoSyncBucketStorage } from './implementation/createMongoSyncBucketStorage.js';
import { createMongoSyncBucketStorage } from './implementation/createMongoSyncBucketStorage.js';
import { PowerSyncMongo } from './implementation/db.js';
import {
  getMongoStorageConfig,
  StorageConfig,
  SyncRuleDocumentBase,
  SyncRuleDocumentV1
} from './implementation/models.js';
import { MongoChecksumOptions } from './implementation/MongoChecksums.js';
import {
  MongoPersistedSyncRulesContentV1,
  MongoPersistedSyncRulesContentV3
} from './implementation/MongoPersistedSyncRulesContent.js';
import { VersionedPowerSyncMongoV3 } from './implementation/v3/VersionedPowerSyncMongoV3.js';
import { SyncConfigDefinition, SyncRuleDocumentV3 } from './storage-index.js';

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

  getInstance(syncRules: storage.PersistedSyncRulesContent, options?: GetIntanceOptions): MongoSyncBucketStorage {
    let { id, slot_name } = syncRules;
    if ((typeof id as any) == 'bigint') {
      id = Number(id);
    }
    const storageConfig = (syncRules as MongoPersistedSyncRulesContentV1).getStorageConfig();
    const storage = createMongoSyncBucketStorage(
      this,
      id,
      syncRules as MongoPersistedSyncRulesContentV1,
      slot_name,
      undefined,
      {
        ...this.internalOptions,
        storageConfig
      }
    );
    if (!options?.skipLifecycleHooks) {
      this.iterateListeners((cb) => cb.syncStorageCreated?.(storage));
    }

    storage.registerListener({
      batchStarted: (batch) => {
        batch.registerListener({
          replicationEvent: (payload) => this.iterateListeners((cb) => cb.replicationEvent?.(payload))
        });
      }
    });
    return storage;
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

  async restartReplication(sync_rules_group_id: number) {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next != null && next.id == sync_rules_group_id) {
      // We need to redo the "next" sync rules
      await this.updateSyncRules(next.asUpdateOptions());
      // Pro-actively stop replicating
      await this.db.sync_rules.updateOne(
        {
          _id: next.id,
          state: storage.SyncRuleState.PROCESSING
        },
        {
          $set: {
            state: storage.SyncRuleState.STOP,
            // v3 nested state
            'sync_configs.$[].state': storage.SyncRuleState.STOP
          }
        }
      );
      await this.db.notifyCheckpoint();
    } else if (next == null && active?.id == sync_rules_group_id) {
      // Slot removed for "active" sync rules, while there is no "next" one.
      await this.updateSyncRules(active.asUpdateOptions());

      // In this case we keep the old one as active for clients, so that that existing clients
      // can still get the latest data while we replicate the new ones.
      // It will however not replicate anymore.

      await this.db.sync_rules.updateOne(
        {
          _id: active.id,
          state: storage.SyncRuleState.ACTIVE
        },
        {
          $set: {
            state: storage.SyncRuleState.ERRORED,
            'sync_configs.$[].state': storage.SyncRuleState.ERRORED
          }
        }
      );
      await this.db.notifyCheckpoint();
    } else if (next != null && active?.id == sync_rules_group_id) {
      // Already have next sync rules, but need to stop replicating the active one.

      await this.db.sync_rules.updateOne(
        {
          _id: active.id,
          state: storage.SyncRuleState.ACTIVE
        },
        {
          $set: {
            state: storage.SyncRuleState.ERRORED,
            'sync_configs.$[].state': storage.SyncRuleState.ERRORED
          }
        }
      );
      await this.db.notifyCheckpoint();
    }
  }

  private async updateSyncRulesV3(
    options: storage.UpdateSyncRulesOptions,
    storageVersion: number,
    storageConfig: StorageConfig
  ): Promise<MongoPersistedSyncRulesContentV3> {
    let rules: MongoPersistedSyncRulesContentV3 | undefined = undefined;
    const versioned = this.db.versioned(storageConfig) as VersionedPowerSyncMongoV3;

    const session = this.session;

    await session.withTransaction(async () => {
      // Only have a single set of sync rules with PROCESSING.
      await this.db.sync_rules.updateMany(
        {
          state: storage.SyncRuleState.PROCESSING
        },
        { $set: { state: storage.SyncRuleState.STOP, 'sync_configs.$[].state': storage.SyncRuleState.STOP } },
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
      const slot_name = generateSlotName(this.slot_name_prefix, id);

      const mapping = BucketDefinitionMapping.fromParsedSyncRules(options.config.parsed);

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

      const doc: SyncRuleDocumentV3 = {
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
        slot_name: slot_name,
        last_checkpoint_ts: null,
        last_fatal_error: null,
        last_fatal_error_ts: null,
        last_keepalive_ts: null
      };

      await this.db.sync_rules.insertOne(doc, { session });
      await this.db.notifyCheckpoint();
      rules = new MongoPersistedSyncRulesContentV3(this.db, doc, syncConfigDoc);
      if (options.lock) {
        const lock = await rules.lock();
      }
    });

    return rules!;
  }

  async updateSyncRules(
    options: storage.UpdateSyncRulesOptions
  ): Promise<MongoPersistedSyncRulesContentV1 | MongoPersistedSyncRulesContentV3> {
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;

    const storageConfig = getMongoStorageConfig(storageVersion);
    if (storageConfig.incrementalReprocessing) {
      return this.updateSyncRulesV3(options, storageVersion, storageConfig);
    }

    let rules: MongoPersistedSyncRulesContentV1 | undefined = undefined;

    const session = this.session;

    await session.withTransaction(async () => {
      // Only have a single set of sync rules with PROCESSING.
      await this.db.sync_rules.updateMany(
        {
          state: storage.SyncRuleState.PROCESSING
        },
        { $set: { state: storage.SyncRuleState.STOP, 'sync_configs.$[].state': storage.SyncRuleState.STOP } },
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
      const slot_name = generateSlotName(this.slot_name_prefix, id);

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
      rules = new MongoPersistedSyncRulesContentV1(this.db, doc);
      if (options.lock) {
        const lock = await rules.lock();
      }
    });

    return rules!;
  }

  async getActiveSyncRulesContent(): Promise<
    MongoPersistedSyncRulesContentV1 | MongoPersistedSyncRulesContentV3 | null
  > {
    const doc = await this.db.sync_rules.findOne(
      {
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
      },
      { sort: { _id: -1 }, limit: 1 }
    );

    return this.getSyncRulesContent(doc, [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED]);
  }

  private async getSyncRulesContent(doc: SyncRuleDocumentBase | null, stateFilter: storage.SyncRuleState[]) {
    if (doc == null) {
      return null;
    }
    const storageConfig = getMongoStorageConfig(doc.storage_version ?? LEGACY_STORAGE_VERSION);

    if (storageConfig.incrementalReprocessing) {
      const v3 = doc as SyncRuleDocumentV3;
      const active = v3.sync_configs.find((c) => stateFilter.includes(c.state));
      if (active == null) {
        return null;
      }

      // TODO: cache this
      const db = this.db.versioned(storageConfig) as VersionedPowerSyncMongoV3;
      const syncConfigDoc = await db.syncConfigDefinitions.findOne({ _id: active._id });
      if (syncConfigDoc == null) {
        return null;
      }
      return new MongoPersistedSyncRulesContentV3(this.db, v3, syncConfigDoc);
    }

    return new MongoPersistedSyncRulesContentV1(this.db, doc as SyncRuleDocumentV1);
  }

  async getNextSyncRulesContent(): Promise<MongoPersistedSyncRulesContentV1 | MongoPersistedSyncRulesContentV3 | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: storage.SyncRuleState.PROCESSING
      },
      { sort: { _id: -1 }, limit: 1 }
    );

    return this.getSyncRulesContent(doc, [storage.SyncRuleState.PROCESSING]);
  }

  async getReplicatingSyncRules(): Promise<storage.PersistedSyncRulesContent[]> {
    const docs = await this.db.sync_rules
      .find({
        state: { $in: [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE] }
      })
      .toArray();

    return (
      await Promise.all(
        docs.map((doc) => {
          return this.getSyncRulesContent(doc, [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE]);
        })
      )
    ).filter((r) => r != null);
  }

  async getStoppedSyncRules(): Promise<storage.PersistedSyncRulesContent[]> {
    const docs = await this.db.sync_rules
      .find({
        state: storage.SyncRuleState.STOP
      })
      .toArray();

    return (
      await Promise.all(
        docs.map((doc) => {
          return this.getSyncRulesContent(doc, [storage.SyncRuleState.STOP]);
        })
      )
    ).filter((d) => d != null);
  }

  async getActiveStorage(): Promise<MongoSyncBucketStorage | null> {
    const content = await this.getActiveSyncRulesContent();
    if (content == null) {
      return null;
    }

    // It is important that this instance is cached.
    // Not for the instance construction itself, but to ensure that internal caches on the instance
    // are re-used properly.
    if (this.activeStorageCache?.group_id == content.id) {
      return this.activeStorageCache;
    } else {
      const instance = this.getInstance(content);
      this.activeStorageCache = instance;
      return instance;
    }
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
