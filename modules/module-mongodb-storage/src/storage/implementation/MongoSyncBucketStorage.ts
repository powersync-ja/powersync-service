import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  BaseObserver,
  logger,
  ReplicationAbortedError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
import {
  BroadcastIterable,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  GetCheckpointChangesOptions,
  InternalOpId,
  maxLsn,
  mergeAsyncIterables,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  ReplicationCheckpoint,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { HydratedSyncRules, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { LRUCache } from 'lru-cache';
import * as timers from 'timers/promises';
import { idPrefixFilter, retryOnMongoMaxTimeMSExpired } from '../../utils/util.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { VersionedPowerSyncMongo } from './db.js';
import { BucketDataKeyV1, BucketStateDocument, CommonSourceTableDocument, SourceKey, StorageConfig } from './models.js';
import { MongoBucketBatchV1 } from './v1/MongoBucketBatchV1.js';
import { MongoBucketBatchV3 } from './v3/MongoBucketBatchV3.js';
import { MongoChecksumOptions, MongoChecksums } from './MongoChecksums.js';
import { MongoCompactor } from './MongoCompactor.js';
import { MongoParameterCompactor } from './MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from './MongoPersistedSyncRulesContent.js';
import { MongoWriteCheckpointAPI } from './MongoWriteCheckpointAPI.js';
import {
  getBucketDataBatchV1,
  getDataBucketChangesV1,
  getParameterBucketChangesV1,
  getParameterSetsV1
} from './v1/MongoSyncBucketStorageV1.js';
import {
  getBucketDataBatchV3,
  getDataBucketChangesV3,
  getParameterBucketChangesV3,
  getParameterSetsV3
} from './v3/MongoSyncBucketStorageV3.js';
import { MongoSyncBucketStorageContext } from './common/MongoSyncBucketStorageContext.js';

export interface MongoSyncBucketStorageOptions {
  checksumOptions?: Omit<MongoChecksumOptions, 'storageConfig' | 'mapping'>;
  storageConfig: StorageConfig;
}

/**
 * Only keep checkpoints around for a minute, before fetching a fresh one.
 *
 * The reason is that we keep a MongoDB snapshot reference (clusterTime) with the checkpoint,
 * and they expire after 5 minutes by default. This is an issue if the checkpoint stream is idle,
 * but new clients connect and use an outdated checkpoint snapshot for parameter queries.
 *
 * These will be filtered out for existing clients, so should not create significant overhead.
 */
const CHECKPOINT_TIMEOUT_MS = 60_000;

export class MongoSyncBucketStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  readonly db: VersionedPowerSyncMongo;
  readonly checksums: MongoChecksums;

  private parsedSyncRulesCache: { parsed: HydratedSyncRules; options: storage.ParseSyncRulesOptions } | undefined;
  private writeCheckpointAPI: MongoWriteCheckpointAPI;
  #storageInitialized = false;

  constructor(
    public readonly factory: MongoBucketStorage,
    public readonly group_id: number,
    private readonly sync_rules: MongoPersistedSyncRulesContent,
    public readonly slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super();
    this.db = factory.db.versioned(sync_rules.getStorageConfig());
    this.checksums = new MongoChecksums(this.db, this.group_id, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: sync_rules.mapping
    });
    this.writeCheckpointAPI = new MongoWriteCheckpointAPI({
      db: this.db,
      mode: writeCheckpointMode ?? storage.WriteCheckpointMode.MANAGED,
      sync_rules_id: group_id
    });
  }

  get writeCheckpointMode() {
    return this.writeCheckpointAPI.writeCheckpointMode;
  }

  get mapping() {
    return this.sync_rules.mapping;
  }

  private get versionContext(): MongoSyncBucketStorageContext {
    return {
      db: this.db,
      group_id: this.group_id,
      mapping: this.mapping
    };
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.writeCheckpointAPI.setWriteCheckpointMode(mode);
  }

  createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createManagedWriteCheckpoint(checkpoint);
  }

  lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    return this.writeCheckpointAPI.lastWriteCheckpoint({
      ...filters,
      sync_rules_id: this.group_id
    });
  }

  getParsedSyncRules(options: storage.ParseSyncRulesOptions): HydratedSyncRules {
    const { parsed, options: cachedOptions } = this.parsedSyncRulesCache ?? {};
    /**
     * Check if the cached sync rules, if present, had the same options.
     * Parse sync rules if the options are different or if there is no cached value.
     */
    if (!parsed || options.defaultSchema != cachedOptions?.defaultSchema) {
      this.parsedSyncRulesCache = { parsed: this.sync_rules.parsed(options).hydratedSyncRules(), options };
    }

    return this.parsedSyncRulesCache!.parsed;
  }

  async getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    return (await this.getCheckpointInternal()) ?? new EmptyReplicationCheckpoint();
  }

  async getCheckpointInternal(): Promise<storage.ReplicationCheckpoint | null> {
    return await this.db.client.withSession({ snapshot: true }, async (session) => {
      const doc = await this.db.sync_rules.findOne(
        { _id: this.group_id },
        {
          session,
          projection: { _id: 1, state: 1, last_checkpoint: 1, last_checkpoint_lsn: 1, snapshot_done: 1 }
        }
      );
      if (!doc?.snapshot_done || !['ACTIVE', 'ERRORED'].includes(doc.state)) {
        // Sync rules not active - return null
        return null;
      }

      // Specifically using operationTime instead of clusterTime
      // There are 3 fields in the response:
      // 1. operationTime, not exposed for snapshot sessions (used for causal consistency)
      // 2. clusterTime (used for connection management)
      // 3. atClusterTime, which is session.snapshotTime
      // We use atClusterTime, to match the driver's internal snapshot handling.
      // There are cases where clusterTime > operationTime and atClusterTime,
      // which could cause snapshot queries using this as the snapshotTime to timeout.
      // This was specifically observed on MongoDB 6.0 and 7.0.
      const snapshotTime = (session as any).snapshotTime as bson.Timestamp | undefined;
      if (snapshotTime == null) {
        throw new ServiceAssertionError('Missing snapshotTime in getCheckpoint()');
      }
      return new MongoReplicationCheckpoint(
        this,
        // null/0n is a valid checkpoint in some cases, for example if the initial snapshot was empty
        doc.last_checkpoint ?? 0n,
        doc.last_checkpoint_lsn ?? null,
        snapshotTime
      );
    });
  }

  private async initializeStorage() {
    if (this.#storageInitialized) {
      return;
    }

    await this.db.initializeStreamStorage(this.group_id);

    const mapping = this.sync_rules.mapping;
    for (let source of mapping.allBucketDefinitionIds()) {
      const collection = this.db.bucket_data_v3(this.group_id, source).collectionName;
      await this.db.db
        .createCollection(collection, { clusteredIndex: { name: '_id', unique: true, key: { _id: 1 } } })
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceExists') {
            return;
          }
          throw error;
        });
    }
    for (let indexId of mapping.allParameterIndexIds()) {
      await this.db.parameterIndexV3(this.group_id, indexId).createIndex(
        {
          lookup: 1,
          key: 1,
          _id: -1
        },
        {
          name: 'lookup_op_id'
        }
      );
    }
    this.#storageInitialized = true;
  }

  async createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    await this.initializeStorage();

    const doc = await this.db.sync_rules.findOne(
      {
        _id: this.group_id
      },
      { projection: { last_checkpoint_lsn: 1, no_checkpoint_before: 1, keepalive_op: 1, snapshot_lsn: 1 } }
    );
    const checkpoint_lsn = doc?.last_checkpoint_lsn ?? null;

    const batchOptions = {
      logger: options.logger,
      db: this.db,
      syncRules: this.sync_rules.parsed(options).hydratedSyncRules(),
      mapping: this.sync_rules.mapping,
      groupId: this.group_id,
      slotName: this.slot_name,
      lastCheckpointLsn: checkpoint_lsn,
      resumeFromLsn: maxLsn(checkpoint_lsn, doc?.snapshot_lsn),
      keepaliveOp: doc?.keepalive_op ? BigInt(doc.keepalive_op) : null,
      storeCurrentData: options.storeCurrentData,
      skipExistingRows: options.skipExistingRows ?? false,
      markRecordUnavailable: options.markRecordUnavailable
    };
    const writer = this.db.storageConfig.incrementalReprocessing
      ? new MongoBucketBatchV3(batchOptions)
      : new MongoBucketBatchV1(batchOptions);
    this.iterateListeners((cb) => cb.batchStarted?.(writer));
    return writer;
  }

  /**
   * @deprecated Use `createWriter()` with `await using` instead.
   */
  async startBatch(
    options: storage.CreateWriterOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    await using writer = await this.createWriter(options);
    await callback(writer);
    await writer.flush();
    return writer.last_flushed_op != null ? { flushed_op: writer.last_flushed_op } : null;
  }

  async resolveTable(options: storage.ResolveTableOptions): Promise<storage.ResolveTableResult> {
    const { group_id, connection_id, connection_tag, entity_descriptor } = options;

    const { schema, name, objectId, replicaIdColumns } = entity_descriptor;

    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));
    const mapping = this.sync_rules.mapping;
    let result: storage.ResolveTableResult | null = null;
    let initializeSourceRecordsFor: bson.ObjectId | null = null;

    const baseId: Partial<CommonSourceTableDocument> = this.db.storageConfig.incrementalReprocessing
      ? {}
      : { group_id };
    await this.db.client.withSession(async (session) => {
      const col = this.db.commonSourceTables(group_id);
      let filter: Partial<CommonSourceTableDocument> = {
        ...baseId,
        connection_id: connection_id,
        schema_name: schema,
        table_name: name,
        replica_id_columns2: normalizedReplicaIdColumns
      };

      if (objectId != null) {
        filter.relation_id = objectId;
      }
      let doc = await col.findOne(filter, { session });
      if (doc == null) {
        const candidateSourceTable = new storage.SourceTable({
          id: new bson.ObjectId(),
          connectionTag: connection_tag,
          objectId: objectId,
          schema: schema,
          name: name,
          replicaIdColumns: replicaIdColumns,
          snapshotComplete: false
        });
        const createDoc: CommonSourceTableDocument = {
          _id: candidateSourceTable.id as bson.ObjectId,
          ...(baseId as any),
          connection_id: connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: null,
          replica_id_columns2: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined
        };
        if (this.db.storageConfig.incrementalReprocessing) {
          const bucketDataSourceIds = options.sync_rules.definition.bucketDataSources
            .filter((source) => source.tableSyncsData(candidateSourceTable))
            .map((source) => mapping.bucketSourceId(source));
          const parameterLookupSourceIds = options.sync_rules.definition.bucketParameterLookupSources
            .filter((source) => source.tableSyncsParameters(candidateSourceTable))
            .map((source) => mapping.parameterLookupId(source));

          Object.assign(createDoc, {
            bucket_data_source_ids: bucketDataSourceIds,
            parameter_lookup_source_ids: parameterLookupSourceIds
          });
        }
        doc = createDoc;

        await col.insertOne(doc, { session });
        if (this.db.storageConfig.incrementalReprocessing) {
          initializeSourceRecordsFor = doc._id;
        }
      }
      const sourceTable = new storage.SourceTable({
        id: doc._id,
        connectionTag: connection_tag,
        objectId: objectId,
        schema: schema,
        name: name,
        replicaIdColumns: replicaIdColumns,
        snapshotComplete: doc.snapshot_done ?? true
      });
      sourceTable.syncEvent = options.sync_rules.tableTriggersEvent(sourceTable);
      sourceTable.syncData = options.sync_rules.tableSyncsData(sourceTable);
      sourceTable.syncParameters = options.sync_rules.tableSyncsParameters(sourceTable);
      sourceTable.snapshotStatus =
        doc.snapshot_status == null
          ? undefined
          : {
              lastKey: doc.snapshot_status.last_key?.buffer ?? null,
              totalEstimatedCount: doc.snapshot_status.total_estimated_count,
              replicatedCount: doc.snapshot_status.replicated_count
            };

      let dropTables: storage.SourceTable[] = [];
      // Detect tables that are either renamed, or have different replica_id_columns
      let truncateFilter = [{ schema_name: schema, table_name: name }] as any[];
      if (objectId != null) {
        // Only detect renames if the source uses relation ids.
        truncateFilter.push({ relation_id: objectId });
      }
      const truncate = await col
        .find(
          {
            ...baseId,
            connection_id: connection_id,
            _id: { $ne: doc._id },
            $or: truncateFilter
          },
          { session }
        )
        .toArray();
      dropTables = truncate.map(
        (doc) =>
          new storage.SourceTable({
            id: doc._id,
            connectionTag: connection_tag,
            objectId: doc.relation_id,
            schema: doc.schema_name,
            name: doc.table_name,
            replicaIdColumns:
              doc.replica_id_columns2?.map((c) => ({ name: c.name, typeOid: c.type_oid, type: c.type })) ?? [],
            snapshotComplete: doc.snapshot_done ?? true
          })
      );

      result = {
        table: sourceTable,
        dropTables: dropTables
      };
    });
    if (initializeSourceRecordsFor != null) {
      await this.db.initializeSourceRecordsCollection(group_id, initializeSourceRecordsFor);
    }
    return result!;
  }

  async getParameterSets(
    checkpoint: MongoReplicationCheckpoint,
    lookups: ScopedParameterLookup[]
  ): Promise<SqliteJsonRow[]> {
    if (this.db.storageConfig.incrementalReprocessing) {
      return getParameterSetsV3(this.versionContext, checkpoint, lookups);
    }
    return getParameterSetsV1(this.versionContext, checkpoint, lookups);
  }

  async *getBucketDataBatch(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    if (this.db.storageConfig.incrementalReprocessing) {
      yield* getBucketDataBatchV3(this.versionContext, checkpoint, dataBuckets, options);
      return;
    }
    yield* getBucketDataBatchV1(this.versionContext, checkpoint, dataBuckets, options);
  }

  async getChecksums(
    checkpoint: utils.InternalOpId,
    buckets: storage.BucketChecksumRequest[]
  ): Promise<utils.ChecksumMap> {
    return this.checksums.getChecksums(checkpoint, buckets);
  }

  clearChecksumCache() {
    this.checksums.clearCache();
  }

  async terminate(options?: storage.TerminateOptions) {
    // Default is to clear the storage except when explicitly requested not to.
    if (!options || options?.clearStorage) {
      await this.clear(options);
    }
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          state: storage.SyncRuleState.TERMINATED,
          persisted_lsn: null,
          snapshot_done: false
        }
      }
    );
    await this.db.notifyCheckpoint();
  }

  async getStatus(): Promise<storage.SyncRuleStatus> {
    const doc = await this.db.sync_rules.findOne(
      {
        _id: this.group_id
      },
      {
        projection: {
          snapshot_done: 1,
          last_checkpoint_lsn: 1,
          state: 1,
          snapshot_lsn: 1
        }
      }
    );
    if (doc == null) {
      throw new ServiceAssertionError('Cannot find sync rules status');
    }

    return {
      snapshot_done: doc.snapshot_done,
      snapshot_lsn: doc.snapshot_lsn ?? null,
      active: doc.state == 'ACTIVE',
      checkpoint_lsn: doc.last_checkpoint_lsn
    };
  }

  async clear(options?: storage.ClearStorageOptions): Promise<void> {
    const signal = options?.signal;

    if (signal?.aborted) {
      throw new ReplicationAbortedError('Aborted clearing data', signal.reason);
    }

    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          snapshot_done: false,
          persisted_lsn: null,
          last_checkpoint_lsn: null,
          last_checkpoint: null,
          no_checkpoint_before: null
        },
        $unset: {
          snapshot_lsn: 1
        }
      },
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
    );
    if (this.db.storageConfig.incrementalReprocessing) {
      for (const collection of await this.db.listBucketDataCollectionsV3(this.group_id)) {
        await collection.drop();
      }
    } else {
      await this.clearDeleteMany(
        'bucket data',
        () =>
          this.db.bucket_data.deleteMany(
            {
              _id: idPrefixFilter<BucketDataKeyV1>({ g: this.group_id }, ['b', 'o'])
            },
            { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
          ),
        signal
      );
    }
    if (this.db.storageConfig.incrementalReprocessing) {
      for (const collection of await this.db.listParameterIndexCollectionsV3(this.group_id)) {
        await collection.collection.drop();
      }
    } else {
      await this.clearDeleteMany(
        'parameter index',
        () =>
          this.db.parameterIndexV1.deleteMany(
            {
              'key.g': this.group_id
            },
            { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
          ),
        signal
      );
    }

    for (const collection of await this.db.listSourceRecordCollectionsV3(this.group_id)) {
      await collection.drop();
    }

    if (this.db.storageConfig.incrementalReprocessing) {
      await this.db
        .bucketStateV3(this.group_id)
        .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
            return;
          }
          throw error;
        });
    } else {
      await this.clearDeleteMany(
        'bucket state',
        () =>
          this.db.bucketStateV1.deleteMany(
            {
              _id: idPrefixFilter<BucketStateDocument['_id']>({ g: this.group_id }, ['b'])
            },
            { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
          ),
        signal
      );
    }

    if (this.db.storageConfig.incrementalReprocessing) {
      await this.db
        .sourceTablesV3(this.group_id)
        .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
            return;
          }
          throw error;
        });
    } else {
      await this.clearDeleteMany(
        'source tables',
        () =>
          this.db.commonSourceTables(this.group_id).deleteMany(
            {
              group_id: this.group_id
            },
            { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
          ),
        signal
      );
    }

    this.#storageInitialized = false;
  }

  private async clearDeleteMany(
    label: string,
    operation: () => Promise<mongo.DeleteResult>,
    signal?: AbortSignal
  ): Promise<void> {
    await retryOnMongoMaxTimeMSExpired(operation, {
      signal,
      abortMessage: 'Aborted clearing data',
      retryDelayMs: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS / 5,
      onRetry: () => {
        logger.info(
          `${this.slot_name} Cleared batch of ${label} in ${lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS}ms, continuing...`
        );
      }
    });
  }

  async reportError(e: any): Promise<void> {
    const message = String(e.message ?? 'Replication failure');
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          last_fatal_error: message,
          last_fatal_error_ts: new Date()
        }
      }
    );
  }

  async compact(options?: storage.CompactOptions) {
    let maxOpId = options?.maxOpId;
    if (maxOpId == null) {
      const checkpoint = await this.getCheckpointInternal();
      maxOpId = checkpoint?.checkpoint ?? undefined;
    }
    await new MongoCompactor(this, this.db, { ...options, maxOpId }).compact();

    if (maxOpId != null && options?.compactParameterData) {
      await new MongoParameterCompactor(this.db, this.group_id, maxOpId, options).compact();
    }
  }

  async populatePersistentChecksumCache(options: PopulateChecksumCacheOptions): Promise<PopulateChecksumCacheResults> {
    logger.info(`Populating persistent checksum cache...`);
    const start = Date.now();
    // We do a minimal compact here.
    // We can optimize this in the future.
    const compactor = new MongoCompactor(this, this.db, {
      ...options,
      // Don't track updates for MOVE compacting
      memoryLimitMB: 0
    });

    const result = await compactor.populateChecksums({
      // There are cases with millions of small buckets, in which case it can take very long to
      // populate the checksums, with minimal benefit. We skip the small buckets here.
      minBucketChanges: options.minBucketChanges ?? 10
    });
    const duration = Date.now() - start;
    logger.info(`Populated persistent checksum cache in ${(duration / 1000).toFixed(1)}s`);
    return result;
  }

  /**
   * Instance-wide watch on the latest available checkpoint (op_id + lsn).
   */
  private async *watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<ReplicationCheckpoint> {
    if (signal.aborted) {
      return;
    }

    // If the stream is idle, we wait a max of a minute (CHECKPOINT_TIMEOUT_MS) before we get another checkpoint,
    // to avoid stale checkpoint snapshots. This is what checkpointTimeoutStream() is for.
    // Essentially, even if there are no actual checkpoint changes, we want a new snapshotTime every minute or so,
    // to ensure that any new clients connecting will get a valid snapshotTime.
    const stream = mergeAsyncIterables(
      [this.checkpointChangesStream(signal), this.checkpointTimeoutStream(signal)],
      signal
    );

    // We only watch changes to the active sync rules.
    // If it changes to inactive, we abort and restart with the new sync rules.
    for await (const _ of stream) {
      if (signal.aborted) {
        // Would likely have been caught by the signal on the timeout or the upstream stream, but we check here anyway
        break;
      }

      const op = await this.getCheckpointInternal();
      if (op == null) {
        // Sync rules have changed - abort and restart.
        // We do a soft close of the stream here - no error
        break;
      }

      // Previously, we only yielded when the checkpoint or lsn changed.
      // However, we always want to use the latest snapshotTime, so we skip that filtering here.
      // That filtering could be added in the per-user streams if needed, but in general the capped collection
      // should already only contain useful changes in most cases.
      yield op;
    }
  }

  // Nothing is done here until a subscriber starts to iterate
  private readonly sharedIter = new BroadcastIterable((signal) => {
    return this.watchActiveCheckpoint(signal);
  });

  /**
   * User-specific watch on the latest checkpoint and/or write checkpoint.
   */
  async *watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<storage.StorageCheckpointUpdate> {
    let lastCheckpoint: ReplicationCheckpoint | null = null;

    const iter = this.sharedIter[Symbol.asyncIterator](options.signal);

    let writeCheckpoint: bigint | null = null;
    // true if we queried the initial write checkpoint, even if it doesn't exist
    let queriedInitialWriteCheckpoint = false;

    for await (const nextCheckpoint of iter) {
      // lsn changes are not important by itself.
      // What is important is:
      // 1. checkpoint (op_id) changes.
      // 2. write checkpoint changes for the specific user

      if (nextCheckpoint.lsn != null && !queriedInitialWriteCheckpoint) {
        // Lookup the first write checkpoint for the user when we can.
        // There will not actually be one in all cases.
        writeCheckpoint = await this.writeCheckpointAPI.lastWriteCheckpoint({
          sync_rules_id: this.group_id,
          user_id: options.user_id,
          heads: {
            '1': nextCheckpoint.lsn
          }
        });
        queriedInitialWriteCheckpoint = true;
      }

      if (
        lastCheckpoint != null &&
        lastCheckpoint.checkpoint == nextCheckpoint.checkpoint &&
        lastCheckpoint.lsn == nextCheckpoint.lsn
      ) {
        // No change - wait for next one
        // In some cases, many LSNs may be produced in a short time.
        // Add a delay to throttle the loop a bit.
        await timers.setTimeout(20 + 10 * Math.random());
        continue;
      }

      if (lastCheckpoint == null) {
        // First message for this stream - "INVALIDATE_ALL" means it will lookup all data
        yield {
          base: nextCheckpoint,
          writeCheckpoint,
          update: CHECKPOINT_INVALIDATE_ALL
        };
      } else {
        const updates = await this.getCheckpointChanges({
          lastCheckpoint,
          nextCheckpoint
        });

        let updatedWriteCheckpoint = updates.updatedWriteCheckpoints.get(options.user_id) ?? null;
        if (updates.invalidateWriteCheckpoints) {
          // Invalidated means there were too many updates to track the individual ones,
          // so we switch to "polling" (querying directly in each stream).
          updatedWriteCheckpoint = await this.writeCheckpointAPI.lastWriteCheckpoint({
            sync_rules_id: this.group_id,
            user_id: options.user_id,
            heads: {
              '1': nextCheckpoint.lsn!
            }
          });
        }
        if (updatedWriteCheckpoint != null && (writeCheckpoint == null || updatedWriteCheckpoint > writeCheckpoint)) {
          writeCheckpoint = updatedWriteCheckpoint;
          // If it happened that we haven't queried a write checkpoint at this point,
          // then we don't need to anymore, since we got an updated one.
          queriedInitialWriteCheckpoint = true;
        }

        yield {
          base: nextCheckpoint,
          writeCheckpoint,
          update: {
            updatedDataBuckets: updates.updatedDataBuckets,
            invalidateDataBuckets: updates.invalidateDataBuckets,
            updatedParameterLookups: updates.updatedParameterLookups,
            invalidateParameterBuckets: updates.invalidateParameterBuckets
          }
        };
      }

      lastCheckpoint = nextCheckpoint;
    }
  }

  /**
   * This watches the checkpoint_events capped collection for new documents inserted,
   * and yields whenever one or more documents are inserted.
   *
   * The actual checkpoint must be queried on the sync_rules collection after this.
   */
  private async *checkpointChangesStream(signal: AbortSignal): AsyncGenerator<void> {
    if (signal.aborted) {
      return;
    }

    const query = () => {
      return this.db.checkpoint_events.find(
        {},
        { tailable: true, awaitData: true, maxAwaitTimeMS: 10_000, batchSize: 1000 }
      );
    };

    let cursor = query();

    signal.addEventListener('abort', () => {
      cursor.close().catch(() => {});
    });

    // Yield once on start, regardless of whether there are documents in the cursor.
    // This is to ensure that the first iteration of the generator yields immediately.
    yield;

    try {
      while (!signal.aborted) {
        const doc = await cursor.tryNext().catch((e) => {
          if (lib_mongo.isMongoServerError(e) && e.codeName === 'CappedPositionLost') {
            // Cursor position lost, potentially due to a high rate of notifications
            cursor = query();
            // Treat as an event found, before querying the new cursor again
            return {};
          } else {
            return Promise.reject(e);
          }
        });
        if (cursor.closed) {
          return;
        }
        // Skip buffered documents, if any. We don't care about the contents,
        // we only want to know when new documents are inserted.
        cursor.readBufferedDocuments();
        if (doc != null) {
          yield;
        }
      }
    } catch (e) {
      if (signal.aborted) {
        return;
      }
      throw e;
    } finally {
      await cursor.close();
    }
  }

  private async *checkpointTimeoutStream(signal: AbortSignal): AsyncGenerator<void> {
    while (!signal.aborted) {
      try {
        await timers.setTimeout(CHECKPOINT_TIMEOUT_MS, undefined, { signal });
      } catch (e) {
        if (e.name == 'AbortError') {
          // This is how we typically abort this stream, when all listeners are done
          return;
        }
        throw e;
      }

      if (!signal.aborted) {
        yield;
      }
    }
  }

  private async getDataBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    if (this.db.storageConfig.incrementalReprocessing) {
      return getDataBucketChangesV3(this.versionContext, options);
    }
    return getDataBucketChangesV1(this.versionContext, options);
  }

  private async getParameterBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    if (this.db.storageConfig.incrementalReprocessing) {
      return getParameterBucketChangesV3(this.versionContext, options);
    }
    return getParameterBucketChangesV1(this.versionContext, options);
  }

  // If we processed all connections together for each checkpoint, we could do a single lookup for all connections.
  // In practice, specific connections may fall behind. So instead, we just cache the results of each specific lookup.
  // TODO (later):
  // We can optimize this by implementing it like ChecksumCache: We can use partial cache results to do
  // more efficient lookups in some cases.
  private checkpointChangesCache = new LRUCache<
    string,
    InternalCheckpointChanges,
    { options: GetCheckpointChangesOptions }
  >({
    // Limit to 50 cache entries, or 10MB, whichever comes first.
    // Some rough calculations:
    // If we process 10 checkpoints per second, and a connection may be 2 seconds behind, we could have
    // up to 20 relevant checkpoints. That gives us 20*20 = 400 potentially-relevant cache entries.
    // That is a worst-case scenario, so we don't actually store that many. In real life, the cache keys
    // would likely be clustered around a few values, rather than spread over all 400 potential values.
    max: 50,
    maxSize: 12 * 1024 * 1024,
    sizeCalculation: (value: InternalCheckpointChanges) => {
      // Estimate of memory usage
      const paramSize = [...value.updatedParameterLookups].reduce<number>((a, b) => a + b.length, 0);
      const bucketSize = [...value.updatedDataBuckets].reduce<number>((a, b) => a + b.length, 0);
      const writeCheckpointSize = value.updatedWriteCheckpoints.size * 30; // estiamte for user_id + bigint
      return 100 + paramSize + bucketSize + writeCheckpointSize;
    },
    fetchMethod: async (_key, _staleValue, options) => {
      return this.getCheckpointChangesInternal(options.context.options);
    }
  });

  async getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<InternalCheckpointChanges> {
    const key = `${options.lastCheckpoint.checkpoint}_${options.lastCheckpoint.lsn}__${options.nextCheckpoint.checkpoint}_${options.nextCheckpoint.lsn}`;
    const result = await this.checkpointChangesCache.fetch(key, { context: { options } });
    return result!;
  }

  private async getCheckpointChangesInternal(options: GetCheckpointChangesOptions): Promise<InternalCheckpointChanges> {
    const dataUpdates = await this.getDataBucketChanges(options);
    const parameterUpdates = await this.getParameterBucketChanges(options);
    const writeCheckpointUpdates = await this.writeCheckpointAPI.getWriteCheckpointChanges(options);

    return {
      ...dataUpdates,
      ...parameterUpdates,
      ...writeCheckpointUpdates
    };
  }
}

interface InternalCheckpointChanges extends CheckpointChanges {
  updatedWriteCheckpoints: Map<string, bigint>;
  invalidateWriteCheckpoints: boolean;
}

class MongoReplicationCheckpoint implements ReplicationCheckpoint {
  constructor(
    private storage: MongoSyncBucketStorage,
    public readonly checkpoint: InternalOpId,
    public readonly lsn: string | null,
    public snapshotTime: mongo.Timestamp
  ) {}

  async getParameterSets(lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
    return this.storage.getParameterSets(this, lookups);
  }
}

class EmptyReplicationCheckpoint implements ReplicationCheckpoint {
  readonly checkpoint: InternalOpId = 0n;
  readonly lsn: string | null = null;

  async getParameterSets(lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
    return [];
  }
}
