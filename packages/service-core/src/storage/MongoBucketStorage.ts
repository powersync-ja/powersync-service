import { SqlSyncRules } from '@powersync/service-sync-rules';
import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';
import { LRUCache } from 'lru-cache/min';
import * as mongo from 'mongodb';
import * as timers from 'timers/promises';

import * as locks from '../locks/locks-index.js';
import * as sync from '../sync/sync-index.js';
import * as util from '../util/util-index.js';

import { DisposableObserver, logger } from '@powersync/lib-services-framework';
import { v4 as uuid } from 'uuid';
import {
  ActiveCheckpoint,
  BucketStorageFactory,
  BucketStorageFactoryListener,
  ParseSyncRulesOptions,
  PersistedSyncRules,
  PersistedSyncRulesContent,
  StorageMetrics,
  UpdateSyncRulesOptions,
  WriteCheckpoint
} from './BucketStorage.js';
import { PowerSyncMongo, PowerSyncMongoOptions } from './mongo/db.js';
import { SyncRuleDocument, SyncRuleState } from './mongo/models.js';
import { MongoPersistedSyncRulesContent } from './mongo/MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage } from './mongo/MongoSyncBucketStorage.js';
import { MongoWriteCheckpointAPI } from './mongo/MongoWriteCheckpointAPI.js';
import { generateSlotName } from './mongo/util.js';
import {
  CustomWriteCheckpointOptions,
  DEFAULT_WRITE_CHECKPOINT_MODE,
  LastWriteCheckpointFilters,
  ManagedWriteCheckpointOptions,
  WriteCheckpointAPI,
  WriteCheckpointMode
} from './write-checkpoint.js';

export interface MongoBucketStorageOptions extends PowerSyncMongoOptions {}

export class MongoBucketStorage
  extends DisposableObserver<BucketStorageFactoryListener>
  implements BucketStorageFactory
{
  private readonly client: mongo.MongoClient;
  private readonly session: mongo.ClientSession;
  // TODO: This is still Postgres specific and needs to be reworked
  public readonly slot_name_prefix: string;

  readonly write_checkpoint_mode: WriteCheckpointMode;

  protected readonly writeCheckpointAPI: WriteCheckpointAPI;

  private readonly storageCache = new LRUCache<number, MongoSyncBucketStorage>({
    max: 3,
    fetchMethod: async (id) => {
      const doc2 = await this.db.sync_rules.findOne(
        {
          _id: id
        },
        { limit: 1 }
      );
      if (doc2 == null) {
        // Deleted in the meantime?
        return undefined;
      }
      const rules = new MongoPersistedSyncRulesContent(this.db, doc2);
      return this.getInstance(rules);
    },
    dispose: (storage) => {
      storage[Symbol.dispose]();
    }
  });

  public readonly db: PowerSyncMongo;

  constructor(
    db: PowerSyncMongo,
    options: {
      slot_name_prefix: string;
      /**
       * Initial Write Checkpoint Mode
       */
      write_checkpoint_mode?: WriteCheckpointMode;
    }
  ) {
    super();
    this.client = db.client;
    this.db = db;
    this.session = this.client.startSession();
    this.slot_name_prefix = options.slot_name_prefix;
    this.write_checkpoint_mode = options.write_checkpoint_mode ?? DEFAULT_WRITE_CHECKPOINT_MODE;
    this.writeCheckpointAPI = new MongoWriteCheckpointAPI({
      db,
      mode: this.write_checkpoint_mode
    });
  }

  getInstance(options: PersistedSyncRulesContent): MongoSyncBucketStorage {
    let { id, slot_name } = options;
    if ((typeof id as any) == 'bigint') {
      id = Number(id);
    }
    const storage = new MongoSyncBucketStorage(this, id, options, slot_name);
    this.iterateListeners((cb) => cb.syncStorageCreated?.(storage));
    storage.registerListener({
      batchStarted: (batch) => {
        // This nested listener will be automatically disposed when the storage is disposed
        batch.registerManagedListener(storage, {
          replicationEvent: (payload) => this.iterateListeners((cb) => cb.replicationEvent?.(payload))
        });
      }
    });
    return storage;
  }

  async configureSyncRules(sync_rules: string, options?: { lock?: boolean }) {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next?.sync_rules_content == sync_rules) {
      logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else if (next == null && active?.sync_rules_content == sync_rules) {
      logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else {
      logger.info('Sync rules updated from configuration');
      const persisted_sync_rules = await this.updateSyncRules({
        content: sync_rules,
        lock: options?.lock
      });
      return { updated: true, persisted_sync_rules, lock: persisted_sync_rules.current_lock ?? undefined };
    }
  }

  async slotRemoved(slot_name: string) {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    // In both the below cases, we create a new sync rules instance.
    // The current one will continue erroring until the next one has finished processing.
    // TODO: Update
    if (next != null && next.slot_name == slot_name) {
      // We need to redo the "next" sync rules
      await this.updateSyncRules({
        content: next.sync_rules_content
      });
      // Pro-actively stop replicating
      await this.db.sync_rules.updateOne(
        {
          _id: next.id,
          state: SyncRuleState.PROCESSING
        },
        {
          $set: {
            state: SyncRuleState.STOP
          }
        }
      );
    } else if (next == null && active?.slot_name == slot_name) {
      // Slot removed for "active" sync rules, while there is no "next" one.
      await this.updateSyncRules({
        content: active.sync_rules_content
      });

      // Pro-actively stop replicating
      await this.db.sync_rules.updateOne(
        {
          _id: active.id,
          state: SyncRuleState.ACTIVE
        },
        {
          $set: {
            state: SyncRuleState.STOP
          }
        }
      );
    }
  }

  async updateSyncRules(options: UpdateSyncRulesOptions): Promise<MongoPersistedSyncRulesContent> {
    // Parse and validate before applying any changes
    const parsed = SqlSyncRules.fromYaml(options.content, {
      // No schema-based validation at this point
      schema: undefined,
      defaultSchema: 'not_applicable', // Not needed for validation
      throwOnError: true
    });

    let rules: MongoPersistedSyncRulesContent | undefined = undefined;

    await this.session.withTransaction(async () => {
      // Only have a single set of sync rules with PROCESSING.
      await this.db.sync_rules.updateMany(
        {
          state: SyncRuleState.PROCESSING
        },
        { $set: { state: SyncRuleState.STOP } }
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
        snapshot_done: false,
        state: SyncRuleState.PROCESSING,
        slot_name: slot_name,
        last_checkpoint_ts: null,
        last_fatal_error: null,
        last_keepalive_ts: null
      };
      await this.db.sync_rules.insertOne(doc);
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
        state: SyncRuleState.ACTIVE
      },
      { sort: { _id: -1 }, limit: 1 }
    );
    if (doc == null) {
      return null;
    }

    return new MongoPersistedSyncRulesContent(this.db, doc);
  }

  async getActiveSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null> {
    const content = await this.getActiveSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  async getNextSyncRulesContent(): Promise<MongoPersistedSyncRulesContent | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: SyncRuleState.PROCESSING
      },
      { sort: { _id: -1 }, limit: 1 }
    );
    if (doc == null) {
      return null;
    }

    return new MongoPersistedSyncRulesContent(this.db, doc);
  }

  async getNextSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null> {
    const content = await this.getNextSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  async getReplicatingSyncRules(): Promise<PersistedSyncRulesContent[]> {
    const docs = await this.db.sync_rules
      .find({
        $or: [{ state: SyncRuleState.ACTIVE }, { state: SyncRuleState.PROCESSING }]
      })
      .toArray();

    return docs.map((doc) => {
      return new MongoPersistedSyncRulesContent(this.db, doc);
    });
  }

  async getStoppedSyncRules(): Promise<PersistedSyncRulesContent[]> {
    const docs = await this.db.sync_rules
      .find({
        state: SyncRuleState.STOP
      })
      .toArray();

    return docs.map((doc) => {
      return new MongoPersistedSyncRulesContent(this.db, doc);
    });
  }

  async batchCreateCustomWriteCheckpoints(checkpoints: CustomWriteCheckpointOptions[]): Promise<void> {
    return this.writeCheckpointAPI.batchCreateCustomWriteCheckpoints(checkpoints);
  }

  setWriteCheckpointMode(mode: WriteCheckpointMode): void {
    return this.writeCheckpointAPI.setWriteCheckpointMode(mode);
  }

  async createCustomWriteCheckpoint(options: CustomWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createCustomWriteCheckpoint(options);
  }

  async createManagedWriteCheckpoint(options: ManagedWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createManagedWriteCheckpoint(options);
  }

  async lastWriteCheckpoint(filters: LastWriteCheckpointFilters): Promise<bigint | null> {
    return this.writeCheckpointAPI.lastWriteCheckpoint(filters);
  }

  async getActiveCheckpoint(): Promise<ActiveCheckpoint> {
    const doc = await this.db.sync_rules.findOne(
      {
        state: SyncRuleState.ACTIVE
      },
      {
        sort: { _id: -1 },
        limit: 1,
        projection: { _id: 1, last_checkpoint: 1, last_checkpoint_lsn: 1 }
      }
    );

    return this.makeActiveCheckpoint(doc);
  }

  async getStorageMetrics(): Promise<StorageMetrics> {
    const operations_aggregate = await this.db.bucket_data

      .aggregate([
        {
          $collStats: {
            storageStats: {},
            count: {}
          }
        }
      ])
      .toArray();

    const parameters_aggregate = await this.db.bucket_parameters
      .aggregate([
        {
          $collStats: {
            storageStats: {},
            count: {}
          }
        }
      ])
      .toArray();

    const replication_aggregate = await this.db.current_data
      .aggregate([
        {
          $collStats: {
            storageStats: {},
            count: {}
          }
        }
      ])
      .toArray();

    return {
      operations_size_bytes: operations_aggregate[0].storageStats.size,
      parameters_size_bytes: parameters_aggregate[0].storageStats.size,
      replication_size_bytes: replication_aggregate[0].storageStats.size
    };
  }

  async getPowerSyncInstanceId(): Promise<string> {
    let instance = await this.db.instance.findOne({
      _id: { $exists: true }
    });

    if (!instance) {
      const manager = locks.createMongoLockManager(this.db.locks, {
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

  private makeActiveCheckpoint(doc: SyncRuleDocument | null) {
    return {
      checkpoint: util.timestampToOpId(doc?.last_checkpoint ?? 0n),
      lsn: doc?.last_checkpoint_lsn ?? null,
      hasSyncRules() {
        return doc != null;
      },
      getBucketStorage: async () => {
        if (doc == null) {
          return null;
        }
        return (await this.storageCache.fetch(doc._id)) ?? null;
      },
      syncRules: doc
        ? new MongoPersistedSyncRulesContent(this.db, doc).parsed({
            defaultSchema: ''
          })
        : null
    } satisfies ActiveCheckpoint;
  }

  /**
   * Instance-wide watch on the latest available checkpoint (op_id + lsn).
   */
  private async *_watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<ActiveCheckpoint> {
    const pipeline: mongo.Document[] = [
      {
        $match: {
          'fullDocument.state': 'ACTIVE',
          operationType: { $in: ['insert', 'update'] }
        }
      },
      {
        $project: {
          operationType: 1,
          'fullDocument._id': 1,
          'fullDocument.last_checkpoint': 1,
          'fullDocument.last_checkpoint_lsn': 1,
          'fullDocument.content': 1
        }
      }
    ];

    // Use this form instead of (doc: SyncRuleDocument | null = null),
    // otherwise we get weird "doc: never" issues.
    let doc = null as SyncRuleDocument | null;
    let clusterTime = null as mongo.Timestamp | null;

    await this.client.withSession(async (session) => {
      doc = await this.db.sync_rules.findOne(
        {
          state: SyncRuleState.ACTIVE
        },
        {
          session,
          sort: { _id: -1 },
          limit: 1,
          projection: {
            _id: 1,
            last_checkpoint: 1,
            last_checkpoint_lsn: 1,
            content: 1
          }
        }
      );
      const time = session.clusterTime?.clusterTime ?? null;
      clusterTime = time;
    });
    if (clusterTime == null) {
      throw new Error('Could not get clusterTime');
    }

    if (signal.aborted) {
      return;
    }

    if (doc) {
      yield this.makeActiveCheckpoint(doc);
    }

    const stream = this.db.sync_rules.watch(pipeline, {
      fullDocument: 'updateLookup',
      // Start at the cluster time where we got the initial doc, to make sure
      // we don't skip any updates.
      // This may result in the first operation being a duplicate, but we filter
      // it out anyway.
      startAtOperationTime: clusterTime
    });

    signal.addEventListener(
      'abort',
      () => {
        stream.close();
      },
      { once: true }
    );

    let lastOp: ActiveCheckpoint | null = null;

    for await (const update of stream.stream()) {
      if (signal.aborted) {
        break;
      }
      if (update.operationType != 'insert' && update.operationType != 'update') {
        continue;
      }
      const doc = update.fullDocument!;
      if (doc == null) {
        continue;
      }

      const op = this.makeActiveCheckpoint(doc);
      // Check for LSN / checkpoint changes - ignore other metadata changes
      if (lastOp == null || op.lsn != lastOp.lsn || op.checkpoint != lastOp.checkpoint) {
        lastOp = op;
        yield op;
      }
    }
  }

  // Nothing is done here until a subscriber starts to iterate
  private readonly sharedIter = new sync.BroadcastIterable((signal) => {
    return this._watchActiveCheckpoint(signal);
  });

  /**
   * Watch changes to the active sync rules and checkpoint.
   */
  watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<ActiveCheckpoint> {
    return wrapWithAbort(this.sharedIter, signal);
  }

  /**
   * User-specific watch on the latest checkpoint and/or write checkpoint.
   */
  async *watchWriteCheckpoint(user_id: string, signal: AbortSignal): AsyncIterable<WriteCheckpoint> {
    let lastCheckpoint: util.OpId | null = null;
    let lastWriteCheckpoint: bigint | null = null;

    const iter = wrapWithAbort(this.sharedIter, signal);
    for await (const cp of iter) {
      const { checkpoint, lsn } = cp;

      // lsn changes are not important by itself.
      // What is important is:
      // 1. checkpoint (op_id) changes.
      // 2. write checkpoint changes for the specific user
      const bucketStorage = await cp.getBucketStorage();

      const lsnFilters: Record<string, string> = lsn ? { 1: lsn } : {};

      const currentWriteCheckpoint = await this.lastWriteCheckpoint({
        user_id,
        sync_rules_id: bucketStorage?.group_id,
        heads: {
          ...lsnFilters
        }
      });

      if (currentWriteCheckpoint == lastWriteCheckpoint && checkpoint == lastCheckpoint) {
        // No change - wait for next one
        // In some cases, many LSNs may be produced in a short time.
        // Add a delay to throttle the write checkpoint lookup a bit.
        await timers.setTimeout(20 + 10 * Math.random());
        continue;
      }

      lastWriteCheckpoint = currentWriteCheckpoint;
      lastCheckpoint = checkpoint;

      yield { base: cp, writeCheckpoint: currentWriteCheckpoint };
    }
  }
}
