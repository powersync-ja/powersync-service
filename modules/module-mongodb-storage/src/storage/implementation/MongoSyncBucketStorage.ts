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
  BucketDataRequest,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  deserializeParameterLookup,
  GetCheckpointChangesOptions,
  InternalOpId,
  internalToExternalOpId,
  maxLsn,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  ProtocolOpId,
  ReplicationCheckpoint,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import {
  BucketDataSource,
  HydratedSyncRules,
  ScopedParameterLookup,
  SqliteJsonRow
} from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { LRUCache } from 'lru-cache';
import * as timers from 'timers/promises';
import { idPrefixFilter, mapOpEntry, readSingleBatch, setSessionSnapshotTime } from '../../utils/util.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey, BucketStateDocument, SourceKey, SourceTableDocument } from './models.js';
import { MongoBucketBatch } from './MongoBucketBatch.js';
import { MongoChecksumOptions, MongoChecksums } from './MongoChecksums.js';
import { MongoCompactor } from './MongoCompactor.js';
import { MongoParameterCompactor } from './MongoParameterCompactor.js';
import { MongoWriteCheckpointAPI } from './MongoWriteCheckpointAPI.js';
import { MongoPersistedSyncRulesContent } from './MongoPersistedSyncRulesContent.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';

export interface MongoSyncBucketStorageOptions {
  checksumOptions?: MongoChecksumOptions;
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
  private readonly db: PowerSyncMongo;
  readonly checksums: MongoChecksums;

  private parsedSyncRulesCache: { parsed: HydratedSyncRules; options: storage.ParseSyncRulesOptions } | undefined;
  private writeCheckpointAPI: MongoWriteCheckpointAPI;
  private mapping: BucketDefinitionMapping;

  constructor(
    public readonly factory: MongoBucketStorage,
    public readonly group_id: number,
    private readonly sync_rules: MongoPersistedSyncRulesContent,
    public readonly slot_name: string,
    writeCheckpointMode?: storage.WriteCheckpointMode,
    options?: MongoSyncBucketStorageOptions
  ) {
    super();
    this.db = factory.db;
    this.mapping = this.sync_rules.mapping;
    this.checksums = new MongoChecksums(this.db, this.group_id, options?.checksumOptions);
    this.writeCheckpointAPI = new MongoWriteCheckpointAPI({
      db: this.db,
      mode: writeCheckpointMode ?? storage.WriteCheckpointMode.MANAGED,
      sync_rules_id: group_id
    });
  }

  get writeCheckpointMode() {
    return this.writeCheckpointAPI.writeCheckpointMode;
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

  async createWriter(options: storage.StartBatchOptions): Promise<MongoBucketBatch> {
    const doc = await this.db.sync_rules.findOne(
      {
        _id: this.group_id
      },
      { projection: { last_checkpoint_lsn: 1, no_checkpoint_before: 1, keepalive_op: 1, snapshot_lsn: 1 } }
    );
    const checkpoint_lsn = doc?.last_checkpoint_lsn ?? null;

    const parsedSyncRules = this.sync_rules.parsed(options);

    const batch = new MongoBucketBatch({
      logger: options.logger,
      db: this.db,
      syncRules: parsedSyncRules,
      groupId: this.group_id,
      slotName: this.slot_name,
      lastCheckpointLsn: checkpoint_lsn,
      resumeFromLsn: maxLsn(checkpoint_lsn, doc?.snapshot_lsn),
      keepaliveOp: doc?.keepalive_op ? BigInt(doc.keepalive_op) : null,
      storeCurrentData: options.storeCurrentData,
      skipExistingRows: options.skipExistingRows ?? false,
      markRecordUnavailable: options.markRecordUnavailable
    });
    this.iterateListeners((cb) => cb.batchStarted?.(batch));
    return batch;
  }

  async startBatch(
    options: storage.StartBatchOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    await using batch = await this.createWriter(options);

    await callback(batch);
    await batch.flush();
    if (batch.last_flushed_op != null) {
      return { flushed_op: batch.last_flushed_op };
    } else {
      return null;
    }
  }

  async resolveTable(options: storage.ResolveTableOptions): Promise<storage.ResolveTableResult> {
    const { group_id, connection_id, connection_tag, entity_descriptor } = options;

    const { schema, name, objectId, replicaIdColumns } = entity_descriptor;

    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));
    let result: storage.ResolveTableResult | null = null;
    await this.db.client.withSession(async (session) => {
      const col = this.db.source_tables;
      let filter: mongo.Filter<SourceTableDocument> = {
        sync_rules_ids: group_id,
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
        doc = {
          _id: new bson.ObjectId(),
          sync_rules_ids: [group_id],
          connection_id: connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: null,
          replica_id_columns2: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined
        };

        await col.insertOne(doc, { session });
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
            group_id: group_id,
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
    return result!;
  }

  async getParameterSets(
    checkpoint: MongoReplicationCheckpoint,
    lookups: ScopedParameterLookup[]
  ): Promise<SqliteJsonRow[]> {
    return this.db.client.withSession({ snapshot: true }, async (session) => {
      // Set the session's snapshot time to the checkpoint's snapshot time.
      // An alternative would be to create the session when the checkpoint is created, but managing
      // the session lifetime would become more complex.
      // Starting and ending sessions are cheap (synchronous when no transactions are used),
      // so this should be fine.
      // This is a roundabout way of setting {readConcern: {atClusterTime: clusterTime}}, since
      // that is not exposed directly by the driver.
      // Future versions of the driver may change the snapshotTime behavior, so we need tests to
      // validate that this works as expected. We test this in the compacting tests.
      setSessionSnapshotTime(session, checkpoint.snapshotTime);
      const lookupFilter = lookups.map((lookup) => {
        return storage.serializeLookup(lookup);
      });
      // This query does not use indexes super efficiently, apart from the lookup filter.
      // From some experimentation I could do individual lookups more efficient using an index
      // on {'key.g': 1, lookup: 1, 'key.t': 1, 'key.k': 1, _id: -1},
      // but could not do the same using $group.
      // For now, just rely on compacting to remove extraneous data.
      // For a description of the data format, see the `/docs/parameters-lookups.md` file.
      const rows = await this.db.bucket_parameters
        .aggregate(
          [
            {
              $match: {
                'key.g': this.group_id,
                lookup: { $in: lookupFilter },
                _id: { $lte: checkpoint.checkpoint }
              }
            },
            {
              $sort: {
                _id: -1
              }
            },
            {
              $group: {
                _id: { key: '$key', lookup: '$lookup' },
                bucket_parameters: {
                  $first: '$bucket_parameters'
                }
              }
            }
          ],
          {
            session,
            readConcern: 'snapshot',
            // Limit the time for the operation to complete, to avoid getting connection timeouts
            maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
          }
        )
        .toArray()
        .catch((e) => {
          throw lib_mongo.mapQueryError(e, 'while evaluating parameter queries');
        });
      const groupedParameters = rows.map((row) => {
        return row.bucket_parameters;
      });
      return groupedParameters.flat();
    });
  }

  async *getBucketDataBatch(
    checkpoint: utils.InternalOpId,
    dataBuckets: BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    if (dataBuckets.length == 0) {
      return;
    }
    let filters: mongo.Filter<BucketDataDocument>[] = [];
    const bucketMap = new Map<string, InternalOpId>(dataBuckets.map((d) => [d.bucket, d.start]));

    if (checkpoint == null) {
      throw new ServiceAssertionError('checkpoint is null');
    }
    const end = checkpoint;
    for (let { bucket: name, start, source } of dataBuckets) {
      const sourceDefinitionId = this.mapping.bucketSourceId(source);
      filters.push({
        _id: {
          $gt: {
            g: sourceDefinitionId,
            b: name,
            o: start
          },
          $lte: {
            g: sourceDefinitionId,
            b: name,
            o: end as any
          }
        }
      });
    }

    // Internal naming:
    // We do a query for one "batch", which may consist of multiple "chunks".
    // Each chunk is limited to single bucket, and is limited in length and size.
    // There are also overall batch length and size limits.

    const batchLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
    const chunkSizeLimitBytes = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;

    const cursor = this.db.bucket_data.find(
      {
        $or: filters
      },
      {
        session: undefined,
        sort: { _id: 1 },
        limit: batchLimit,
        // Increase batch size above the default 101, so that we can fill an entire batch in
        // one go.
        // batchSize is 1 more than limit to auto-close the cursor.
        // See https://github.com/mongodb/node-mongodb-native/pull/4580
        batchSize: batchLimit + 1,
        // Raw mode is returns an array of Buffer instead of parsed documents.
        // We use it so that:
        // 1. We can calculate the document size accurately without serializing again.
        // 2. We can delay parsing the results until it's needed.
        // We manually use bson.deserialize below
        raw: true,

        // Limit the time for the operation to complete, to avoid getting connection timeouts
        maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
      }
    ) as unknown as mongo.FindCursor<Buffer>;

    // We want to limit results to a single batch to avoid high memory usage.
    // This approach uses MongoDB's batch limits to limit the data here, which limits
    // to the lower of the batch count and size limits.
    // This is similar to using `singleBatch: true` in the find options, but allows
    // detecting "hasMore".
    let { data, hasMore: batchHasMore } = await readSingleBatch(cursor).catch((e) => {
      throw lib_mongo.mapQueryError(e, 'while reading bucket data');
    });
    if (data.length == batchLimit) {
      // Limit reached - could have more data, despite the cursor being drained.
      batchHasMore = true;
    }

    let chunkSizeBytes = 0;
    let currentChunk: utils.SyncBucketData | null = null;
    let targetOp: InternalOpId | null = null;

    // Ordered by _id, meaning buckets are grouped together
    for (let rawData of data) {
      const row = bson.deserialize(rawData, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS) as BucketDataDocument;
      const bucket = row._id.b;

      if (currentChunk == null || currentChunk.bucket != bucket || chunkSizeBytes >= chunkSizeLimitBytes) {
        // We need to start a new chunk
        let start: ProtocolOpId | undefined = undefined;
        if (currentChunk != null) {
          // There is an existing chunk we need to yield
          if (currentChunk.bucket == bucket) {
            // Current and new chunk have the same bucket, so need has_more on the current one.
            // If currentChunk.bucket != bucket, then we reached the end of the previous bucket,
            // and has_more = false in that case.
            currentChunk.has_more = true;
            start = currentChunk.next_after;
          }

          const yieldChunk = currentChunk;
          currentChunk = null;
          chunkSizeBytes = 0;
          yield { chunkData: yieldChunk, targetOp: targetOp };
          targetOp = null;
        }

        if (start == null) {
          const startOpId = bucketMap.get(bucket);
          if (startOpId == null) {
            throw new ServiceAssertionError(`data for unexpected bucket: ${bucket}`);
          }
          start = internalToExternalOpId(startOpId);
        }
        currentChunk = {
          bucket,
          after: start,
          has_more: false,
          data: [],
          next_after: start
        };
        targetOp = null;
      }

      const entry = mapOpEntry(row);

      if (row.target_op != null) {
        // MOVE, CLEAR
        if (targetOp == null || row.target_op > targetOp) {
          targetOp = row.target_op;
        }
      }

      currentChunk.data.push(entry);
      currentChunk.next_after = entry.op_id;

      chunkSizeBytes += rawData.byteLength;
    }

    if (currentChunk != null) {
      const yieldChunk = currentChunk;
      currentChunk = null;
      // This is the final chunk in the batch.
      // There may be more data if and only if the batch we retrieved isn't complete.
      yieldChunk.has_more = batchHasMore;
      yield { chunkData: yieldChunk, targetOp: targetOp };
      targetOp = null;
    }
  }

  async getChecksums(checkpoint: utils.InternalOpId, buckets: string[]): Promise<utils.ChecksumMap> {
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
    const signal = options?.signal ?? new AbortController().signal;

    const doc = await this.db.sync_rules.findOneAndUpdate(
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
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS, returnDocument: 'after' }
    );

    if (doc?.rule_mapping != null) {
      for (let [name, id] of Object.entries(doc.rule_mapping.definitions)) {
        await this.retriedDelete(`deleting bucket data for ${name}`, signal, () =>
          this.db.bucket_data.deleteMany(
            {
              _id: idPrefixFilter<BucketDataKey>({ g: id }, ['b', 'o'])
            },
            { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
          )
        );
      }

      for (let [name, id] of Object.entries(doc.rule_mapping.parameter_lookups)) {
        await this.retriedDelete(`deleting parameter lookup data for ${name}`, signal, () =>
          this.db.bucket_parameters.deleteMany(
            {
              'key.g': id
            },
            { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
          )
        );
      }
    }

    await this.retriedDelete('deleting bucket data', signal, () =>
      this.db.bucket_data.deleteMany(
        {
          _id: idPrefixFilter<BucketDataKey>({ g: this.group_id }, ['b', 'o'])
        },
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      )
    );
    await this.retriedDelete('deleting bucket parameter lookup values', signal, () =>
      this.db.bucket_parameters.deleteMany(
        {
          'key.g': this.group_id
        },
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      )
    );

    // FIXME: handle refactored current_data structure
    await this.retriedDelete('deleting current data records', signal, () =>
      this.db.current_data.deleteMany(
        {
          _id: idPrefixFilter<SourceKey>({ g: this.group_id }, ['t', 'k'])
        },
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      )
    );

    await this.retriedDelete('deleting bucket state records', signal, () =>
      this.db.bucket_state.deleteMany(
        {
          _id: idPrefixFilter<BucketStateDocument['_id']>({ g: this.group_id }, ['b'])
        },
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      )
    );

    // First remove the reference
    this.db.source_tables.updateMany({ sync_rules_ids: this.group_id }, { $pull: { sync_rules_ids: this.group_id } });

    // Then delete any source tables no longer referenced
    await this.retriedDelete('deleting source table records', signal, () =>
      this.db.source_tables.deleteMany(
        {
          sync_rules_ids: []
        },
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      )
    );
  }

  private async retriedDelete(
    message: string,
    signal: AbortSignal,
    deleteFunc: () => Promise<mongo.DeleteResult>
  ): Promise<void> {
    // Individual operations here may time out with the maxTimeMS option.
    // It is expected to still make progress, and continue on the next try.

    let i = 0;
    while (!signal.aborted) {
      try {
        const result = await deleteFunc();
        if (result.deletedCount > 0) {
          logger.info(`${this.slot_name} ${message} - done`);
        }
        return;
      } catch (e: unknown) {
        if (lib_mongo.isMongoServerError(e) && e.codeName == 'MaxTimeMSExpired') {
          i += 1;
          logger.info(`${this.slot_name} ${message} iteration ${i}, continuing...`);
          await timers.setTimeout(lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS / 5);
        } else {
          throw e;
        }
      }
    }
    throw new ReplicationAbortedError('Aborted clearing data', signal.reason);
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
    const stream = this.checkpointChangesStream(signal);

    if (signal.aborted) {
      return;
    }

    // We only watch changes to the active sync rules.
    // If it changes to inactive, we abort and restart with the new sync rules.
    try {
      while (true) {
        // If the stream is idle, we wait a max of a minute (CHECKPOINT_TIMEOUT_MS)
        // before we get another checkpoint, to avoid stale checkpoint snapshots.
        const timeout = timers
          .setTimeout(CHECKPOINT_TIMEOUT_MS, { done: false }, { signal })
          .catch(() => ({ done: true }));
        try {
          const result = await Promise.race([stream.next(), timeout]);
          if (result.done) {
            break;
          }
        } catch (e) {
          if (e.name == 'AbortError') {
            break;
          }
          throw e;
        }

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
    } finally {
      await stream.return(null);
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

  private async getDataBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    const limit = 1000;
    const bucketStateUpdates = await this.db.bucket_state
      .find(
        {
          // We have an index on (_id.g, last_op).
          '_id.g': this.group_id,
          last_op: { $gt: options.lastCheckpoint.checkpoint }
        },
        {
          projection: {
            '_id.b': 1
          },
          limit: limit + 1,
          // batchSize is 1 more than limit to auto-close the cursor.
          // See https://github.com/mongodb/node-mongodb-native/pull/4580
          batchSize: limit + 2,
          singleBatch: true
        }
      )
      .toArray();

    const buckets = bucketStateUpdates.map((doc) => doc._id.b);
    const invalidateDataBuckets = buckets.length > limit;

    return {
      invalidateDataBuckets: invalidateDataBuckets,
      updatedDataBuckets: invalidateDataBuckets ? new Set<string>() : new Set(buckets)
    };
  }

  private async getParameterBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    const limit = 1000;
    const parameterUpdates = await this.db.bucket_parameters
      .find(
        {
          _id: { $gt: options.lastCheckpoint.checkpoint, $lte: options.nextCheckpoint.checkpoint },
          'key.g': this.group_id
        },
        {
          projection: {
            lookup: 1
          },
          limit: limit + 1,
          // batchSize is 1 more than limit to auto-close the cursor.
          // See https://github.com/mongodb/node-mongodb-native/pull/4580
          batchSize: limit + 2,
          singleBatch: true
        }
      )
      .toArray();
    const invalidateParameterUpdates = parameterUpdates.length > limit;

    return {
      invalidateParameterBuckets: invalidateParameterUpdates,
      updatedParameterLookups: invalidateParameterUpdates
        ? new Set<string>()
        : new Set<string>(parameterUpdates.map((p) => JSONBig.stringify(deserializeParameterLookup(p.lookup))))
    };
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
