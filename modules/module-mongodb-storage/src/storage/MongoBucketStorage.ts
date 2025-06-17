import { SqlSyncRules } from '@powersync/service-sync-rules';

import { GetIntanceOptions, storage } from '@powersync/service-core';

import { BaseObserver, ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { v4 as uuid } from 'uuid';

import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';

import { PowerSyncMongo } from './implementation/db.js';
import { SyncRuleDocument } from './implementation/models.js';
import { MongoPersistedSyncRulesContent } from './implementation/MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage } from './implementation/MongoSyncBucketStorage.js';
import { generateSlotName } from './implementation/util.js';

export class MongoBucketStorage
  extends BaseObserver<storage.BucketStorageFactoryListener>
  implements storage.BucketStorageFactory
{
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
    }
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
    const storage = new MongoSyncBucketStorage(this, id, syncRules, slot_name);
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

  async configureSyncRules(options: storage.UpdateSyncRulesOptions) {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next?.sync_rules_content == options.content) {
      logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else if (next == null && active?.sync_rules_content == options.content) {
      logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else {
      logger.info('Sync rules updated from configuration');
      const persisted_sync_rules = await this.updateSyncRules(options);
      return { updated: true, persisted_sync_rules, lock: persisted_sync_rules.current_lock ?? undefined };
    }
  }

  async restartReplication(sync_rules_group_id: number) {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next != null && next.id == sync_rules_group_id) {
      // We need to redo the "next" sync rules
      await this.updateSyncRules({
        content: next.sync_rules_content,
        validate: false
      });
      // Pro-actively stop replicating
      await this.db.sync_rules.updateOne(
        {
          _id: next.id,
          state: storage.SyncRuleState.PROCESSING
        },
        {
          $set: {
            state: storage.SyncRuleState.STOP
          }
        }
      );
      await this.db.notifyCheckpoint();
    } else if (next == null && active?.id == sync_rules_group_id) {
      // Slot removed for "active" sync rules, while there is no "next" one.
      await this.updateSyncRules({
        content: active.sync_rules_content,
        validate: false
      });

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
            state: storage.SyncRuleState.ERRORED
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
            state: storage.SyncRuleState.ERRORED
          }
        }
      );
      await this.db.notifyCheckpoint();
    }
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<MongoPersistedSyncRulesContent> {
    if (options.validate) {
      // Parse and validate before applying any changes
      SqlSyncRules.fromYaml(options.content, {
        // No schema-based validation at this point
        schema: undefined,
        defaultSchema: 'not_applicable', // Not needed for validation
        throwOnError: true
      });
    } else {
      // We do not validate sync rules at this point.
      // That is done when using the sync rules, so that the diagnostics API can report the errors.
    }

    let rules: MongoPersistedSyncRulesContent | undefined = undefined;

    await this.session.withTransaction(async () => {
      // Only have a single set of sync rules with PROCESSING.
      await this.db.sync_rules.updateMany(
        {
          state: storage.SyncRuleState.PROCESSING
        },
        { $set: { state: storage.SyncRuleState.STOP } }
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
          returnDocument: 'after'
        }
      );

      const id = Number(id_doc!.op_id);
      const slot_name = generateSlotName(this.slot_name_prefix, id);

      const doc: SyncRuleDocument = {
        _id: id,
        content: options.content,
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
        last_keepalive_ts: null
      };
      await this.db.sync_rules.insertOne(doc);
      await this.db.notifyCheckpoint();
      rules = new MongoPersistedSyncRulesContent(this.db, doc);
      if (options.lock) {
        const lock = await rules.lock();
      }
    });

    return rules!;
  }

  async getActiveSyncRulesContent(): Promise<MongoPersistedSyncRulesContent | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
      },
      { sort: { _id: -1 }, limit: 1 }
    );
    if (doc == null) {
      return null;
    }

    return new MongoPersistedSyncRulesContent(this.db, doc);
  }

  async getActiveSyncRules(options: storage.ParseSyncRulesOptions): Promise<storage.PersistedSyncRules | null> {
    const content = await this.getActiveSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  async getNextSyncRulesContent(): Promise<MongoPersistedSyncRulesContent | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: storage.SyncRuleState.PROCESSING
      },
      { sort: { _id: -1 }, limit: 1 }
    );
    if (doc == null) {
      return null;
    }

    return new MongoPersistedSyncRulesContent(this.db, doc);
  }

  async getNextSyncRules(options: storage.ParseSyncRulesOptions): Promise<storage.PersistedSyncRules | null> {
    const content = await this.getNextSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  async getReplicatingSyncRules(): Promise<storage.PersistedSyncRulesContent[]> {
    const docs = await this.db.sync_rules
      .find({
        state: { $in: [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE] }
      })
      .toArray();

    return docs.map((doc) => {
      return new MongoPersistedSyncRulesContent(this.db, doc);
    });
  }

  async getStoppedSyncRules(): Promise<storage.PersistedSyncRulesContent[]> {
    const docs = await this.db.sync_rules
      .find({
        state: storage.SyncRuleState.STOP
      })
      .toArray();

    return docs.map((doc) => {
      return new MongoPersistedSyncRulesContent(this.db, doc);
    });
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

    const active_sync_rules = await this.getActiveSyncRules({ defaultSchema: 'public' });
    if (active_sync_rules == null) {
      return {
        operations_size_bytes: 0,
        parameters_size_bytes: 0,
        replication_size_bytes: 0
      };
    }
    const operations_aggregate = await this.db.bucket_data

      .aggregate([
        {
          $collStats: {
            storageStats: {}
          }
        }
      ])
      .toArray()
      .catch(ignoreNotExisting);

    const parameters_aggregate = await this.db.bucket_parameters
      .aggregate([
        {
          $collStats: {
            storageStats: {}
          }
        }
      ])
      .toArray()
      .catch(ignoreNotExisting);

    const replication_aggregate = await this.db.current_data
      .aggregate([
        {
          $collStats: {
            storageStats: {}
          }
        }
      ])
      .toArray()
      .catch(ignoreNotExisting);

    return {
      operations_size_bytes: Number(operations_aggregate[0].storageStats.size),
      parameters_size_bytes: Number(parameters_aggregate[0].storageStats.size),
      replication_size_bytes: Number(replication_aggregate[0].storageStats.size)
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
