import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  BaseObserver,
  ErrorCode,
  logger,
  ServiceAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  BroadcastIterable,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  GetCheckpointChangesOptions,
  ReplicationCheckpoint,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { SqliteJsonRow, SqliteJsonValue, SqlSyncRules } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';
import * as timers from 'timers/promises';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey, SourceKey, SyncRuleCheckpointState, SyncRuleDocument } from './models.js';
import { MongoBucketBatch } from './MongoBucketBatch.js';
import { MongoCompactor } from './MongoCompactor.js';
import { MongoWriteCheckpointAPI } from './MongoWriteCheckpointAPI.js';
import { idPrefixFilter, mapOpEntries, readSingleBatch } from './util.js';

export class MongoSyncBucketStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  private readonly db: PowerSyncMongo;
  private checksumCache = new storage.ChecksumCache({
    fetchChecksums: (batch) => {
      return this.getChecksumsInternal(batch);
    }
  });

  private parsedSyncRulesCache: { parsed: SqlSyncRules; options: storage.ParseSyncRulesOptions } | undefined;
  private writeCheckpointAPI: storage.WriteCheckpointAPI;

  constructor(
    public readonly factory: MongoBucketStorage,
    public readonly group_id: number,
    private readonly sync_rules: storage.PersistedSyncRulesContent,
    public readonly slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode = storage.WriteCheckpointMode.MANAGED
  ) {
    super();
    this.db = factory.db;
    this.writeCheckpointAPI = new MongoWriteCheckpointAPI({
      db: this.db,
      mode: writeCheckpointMode
    });
  }

  get writeCheckpointMode() {
    return this.writeCheckpointAPI.writeCheckpointMode;
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.writeCheckpointAPI.setWriteCheckpointMode(mode);
  }

  batchCreateCustomWriteCheckpoints(checkpoints: storage.BatchedCustomWriteCheckpointOptions[]): Promise<void> {
    return this.writeCheckpointAPI.batchCreateCustomWriteCheckpoints(
      checkpoints.map((checkpoint) => ({ ...checkpoint, sync_rules_id: this.group_id }))
    );
  }

  createCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createCustomWriteCheckpoint({
      ...checkpoint,
      sync_rules_id: this.group_id
    });
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

  getParsedSyncRules(options: storage.ParseSyncRulesOptions): SqlSyncRules {
    const { parsed, options: cachedOptions } = this.parsedSyncRulesCache ?? {};
    /**
     * Check if the cached sync rules, if present, had the same options.
     * Parse sync rules if the options are different or if there is no cached value.
     */
    if (!parsed || options.defaultSchema != cachedOptions?.defaultSchema) {
      this.parsedSyncRulesCache = { parsed: this.sync_rules.parsed(options).sync_rules, options };
    }

    return this.parsedSyncRulesCache!.parsed;
  }

  async getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    const doc = await this.db.sync_rules.findOne(
      { _id: this.group_id },
      {
        projection: { last_checkpoint: 1, last_checkpoint_lsn: 1 }
      }
    );
    return {
      checkpoint: utils.timestampToOpId(doc?.last_checkpoint ?? 0n),
      lsn: doc?.last_checkpoint_lsn ?? null
    };
  }

  async startBatch(
    options: storage.StartBatchOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    const doc = await this.db.sync_rules.findOne(
      {
        _id: this.group_id
      },
      { projection: { last_checkpoint_lsn: 1, no_checkpoint_before: 1, keepalive_op: 1 } }
    );
    const checkpoint_lsn = doc?.last_checkpoint_lsn ?? null;

    await using batch = new MongoBucketBatch({
      db: this.db,
      syncRules: this.sync_rules.parsed(options).sync_rules,
      groupId: this.group_id,
      slotName: this.slot_name,
      lastCheckpointLsn: checkpoint_lsn,
      noCheckpointBeforeLsn: doc?.no_checkpoint_before ?? options.zeroLSN,
      keepaliveOp: doc?.keepalive_op ?? null,
      storeCurrentData: options.storeCurrentData,
      skipExistingRows: options.skipExistingRows ?? false
    });
    this.iterateListeners((cb) => cb.batchStarted?.(batch));

    await callback(batch);
    await batch.flush();
    if (batch.last_flushed_op) {
      return { flushed_op: String(batch.last_flushed_op) };
    } else {
      return null;
    }
  }

  async resolveTable(options: storage.ResolveTableOptions): Promise<storage.ResolveTableResult> {
    const { group_id, connection_id, connection_tag, entity_descriptor } = options;

    const { schema, name: table, objectId, replicationColumns } = entity_descriptor;

    const columns = replicationColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));
    let result: storage.ResolveTableResult | null = null;
    await this.db.client.withSession(async (session) => {
      const col = this.db.source_tables;
      let doc = await col.findOne(
        {
          group_id: group_id,
          connection_id: connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: table,
          replica_id_columns2: columns
        },
        { session }
      );
      if (doc == null) {
        doc = {
          _id: new bson.ObjectId(),
          group_id: group_id,
          connection_id: connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: table,
          replica_id_columns: null,
          replica_id_columns2: columns,
          snapshot_done: false
        };

        await col.insertOne(doc, { session });
      }
      const sourceTable = new storage.SourceTable(
        doc._id,
        connection_tag,
        objectId,
        schema,
        table,
        replicationColumns,
        doc.snapshot_done ?? true
      );
      sourceTable.syncEvent = options.sync_rules.tableTriggersEvent(sourceTable);
      sourceTable.syncData = options.sync_rules.tableSyncsData(sourceTable);
      sourceTable.syncParameters = options.sync_rules.tableSyncsParameters(sourceTable);

      const truncate = await col
        .find(
          {
            group_id: group_id,
            connection_id: connection_id,
            _id: { $ne: doc._id },
            $or: [{ relation_id: objectId }, { schema_name: schema, table_name: table }]
          },
          { session }
        )
        .toArray();
      result = {
        table: sourceTable,
        dropTables: truncate.map(
          (doc) =>
            new storage.SourceTable(
              doc._id,
              connection_tag,
              doc.relation_id ?? 0,
              doc.schema_name,
              doc.table_name,
              doc.replica_id_columns2?.map((c) => ({ name: c.name, typeOid: c.type_oid, type: c.type })) ?? [],
              doc.snapshot_done ?? true
            )
        )
      };
    });
    return result!;
  }

  async getParameterSets(checkpoint: utils.OpId, lookups: SqliteJsonValue[][]): Promise<SqliteJsonRow[]> {
    const lookupFilter = lookups.map((lookup) => {
      return storage.serializeLookup(lookup);
    });
    const rows = await this.db.bucket_parameters
      .aggregate([
        {
          $match: {
            'key.g': this.group_id,
            lookup: { $in: lookupFilter },
            _id: { $lte: BigInt(checkpoint) }
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
      ])
      .toArray();
    const groupedParameters = rows.map((row) => {
      return row.bucket_parameters;
    });
    return groupedParameters.flat();
  }

  async *getBucketDataBatch(
    checkpoint: utils.OpId,
    dataBuckets: Map<string, string>,
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataBatch> {
    if (dataBuckets.size == 0) {
      return;
    }
    let filters: mongo.Filter<BucketDataDocument>[] = [];

    const end = checkpoint ? BigInt(checkpoint) : new bson.MaxKey();
    for (let [name, start] of dataBuckets.entries()) {
      filters.push({
        _id: {
          $gt: {
            g: this.group_id,
            b: name,
            o: BigInt(start)
          },
          $lte: {
            g: this.group_id,
            b: name,
            o: end as any
          }
        }
      });
    }

    const limit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
    const sizeLimit = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;

    const cursor = this.db.bucket_data.find(
      {
        $or: filters
      },
      {
        session: undefined,
        sort: { _id: 1 },
        limit: limit,
        // Increase batch size above the default 101, so that we can fill an entire batch in
        // one go.
        batchSize: limit,
        // Raw mode is returns an array of Buffer instead of parsed documents.
        // We use it so that:
        // 1. We can calculate the document size accurately without serializing again.
        // 2. We can delay parsing the results until it's needed.
        // We manually use bson.deserialize below
        raw: true,

        // Since we're using raw: true and parsing ourselves later, we don't need bigint
        // support here.
        // Disabling due to https://jira.mongodb.org/browse/NODE-6165, and the fact that this
        // is one of our most common queries.
        useBigInt64: false
      }
    ) as unknown as mongo.FindCursor<Buffer>;

    // We want to limit results to a single batch to avoid high memory usage.
    // This approach uses MongoDB's batch limits to limit the data here, which limits
    // to the lower of the batch count and size limits.
    // This is similar to using `singleBatch: true` in the find options, but allows
    // detecting "hasMore".
    let { data, hasMore } = await readSingleBatch(cursor);
    if (data.length == limit) {
      // Limit reached - could have more data, despite the cursor being drained.
      hasMore = true;
    }

    let batchSize = 0;
    let currentBatch: utils.SyncBucketData | null = null;
    let targetOp: bigint | null = null;

    // Ordered by _id, meaning buckets are grouped together
    for (let rawData of data) {
      const row = bson.deserialize(rawData, storage.BSON_DESERIALIZE_OPTIONS) as BucketDataDocument;
      const bucket = row._id.b;

      if (currentBatch == null || currentBatch.bucket != bucket || batchSize >= sizeLimit) {
        let start: string | undefined = undefined;
        if (currentBatch != null) {
          if (currentBatch.bucket == bucket) {
            currentBatch.has_more = true;
          }

          const yieldBatch = currentBatch;
          start = currentBatch.after;
          currentBatch = null;
          batchSize = 0;
          yield { batch: yieldBatch, targetOp: targetOp };
          targetOp = null;
        }

        start ??= dataBuckets.get(bucket);
        if (start == null) {
          throw new ServiceAssertionError(`data for unexpected bucket: ${bucket}`);
        }
        currentBatch = {
          bucket,
          after: start,
          has_more: hasMore,
          data: [],
          next_after: start
        };
        targetOp = null;
      }

      if (row.target_op != null) {
        // MOVE, CLEAR
        if (targetOp == null || row.target_op > targetOp) {
          targetOp = row.target_op;
        }
      }

      const entries = mapOpEntries(row);
      currentBatch.data.push(...entries);
      currentBatch.next_after = String(row._id.o);

      batchSize += rawData.byteLength;
    }

    if (currentBatch != null) {
      const yieldBatch = currentBatch;
      currentBatch = null;
      yield { batch: yieldBatch, targetOp: targetOp };
      targetOp = null;
    }
  }

  async getChecksums(checkpoint: utils.OpId, buckets: string[]): Promise<utils.ChecksumMap> {
    return this.checksumCache.getChecksumMap(checkpoint, buckets);
  }

  private async getChecksumsInternal(batch: storage.FetchPartialBucketChecksum[]): Promise<storage.PartialChecksumMap> {
    if (batch.length == 0) {
      return new Map();
    }

    const filters: any[] = [];
    for (let request of batch) {
      filters.push({
        _id: {
          $gt: {
            g: this.group_id,
            b: request.bucket,
            o: request.start ? BigInt(request.start) : new bson.MinKey()
          },
          $lte: {
            g: this.group_id,
            b: request.bucket,
            o: BigInt(request.end)
          }
        }
      });
    }

    const aggregate = await this.db.bucket_data
      .aggregate(
        [
          {
            $match: {
              $or: filters
            }
          },
          {
            $group: {
              _id: '$_id.b',
              checksum_total: { $sum: '$checksum' },
              count: { $sum: 1 },
              has_clear_op: {
                $max: {
                  $cond: [{ $eq: ['$op', 'CLEAR'] }, 1, 0]
                }
              }
            }
          }
        ],
        { session: undefined, readConcern: 'snapshot' }
      )
      .toArray();

    return new Map<string, storage.PartialChecksum>(
      aggregate.map((doc) => {
        return [
          doc._id,
          {
            bucket: doc._id,
            partialCount: doc.count,
            partialChecksum: Number(BigInt(doc.checksum_total) & 0xffffffffn) & 0xffffffff,
            isFullChecksum: doc.has_clear_op == 1
          } satisfies storage.PartialChecksum
        ];
      })
    );
  }

  async terminate(options?: storage.TerminateOptions) {
    // Default is to clear the storage except when explicitly requested not to.
    if (!options || options?.clearStorage) {
      await this.clear();
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
          state: 1
        }
      }
    );
    if (doc == null) {
      throw new ServiceAssertionError('Cannot find sync rules status');
    }

    return {
      snapshot_done: doc.snapshot_done,
      active: doc.state == 'ACTIVE',
      checkpoint_lsn: doc.last_checkpoint_lsn
    };
  }

  async clear(): Promise<void> {
    while (true) {
      try {
        await this.clearIteration();

        logger.info(`${this.slot_name} Done clearing data`);
        return;
      } catch (e: unknown) {
        if (lib_mongo.isMongoServerError(e) && e.codeName == 'MaxTimeMSExpired') {
          logger.info(
            `${this.slot_name} Cleared batch of data in ${lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS}ms, continuing...`
          );
          await timers.setTimeout(lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS / 5);
          continue;
        } else {
          throw e;
        }
      }
    }
  }

  private async clearIteration(): Promise<void> {
    // Individual operations here may time out with the maxTimeMS option.
    // It is expected to still make progress, and continue on the next try.

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
        }
      },
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
    );
    await this.db.bucket_data.deleteMany(
      {
        _id: idPrefixFilter<BucketDataKey>({ g: this.group_id }, ['b', 'o'])
      },
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
    );
    await this.db.bucket_parameters.deleteMany(
      {
        key: idPrefixFilter<SourceKey>({ g: this.group_id }, ['t', 'k'])
      },
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
    );

    await this.db.current_data.deleteMany(
      {
        _id: idPrefixFilter<SourceKey>({ g: this.group_id }, ['t', 'k'])
      },
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
    );

    await this.db.source_tables.deleteMany(
      {
        group_id: this.group_id
      },
      { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
    );
  }

  async autoActivate(): Promise<void> {
    await this.db.client.withSession(async (session) => {
      await session.withTransaction(async () => {
        const doc = await this.db.sync_rules.findOne({ _id: this.group_id }, { session });
        if (doc && doc.state == 'PROCESSING') {
          await this.db.sync_rules.updateOne(
            {
              _id: this.group_id
            },
            {
              $set: {
                state: storage.SyncRuleState.ACTIVE
              }
            },
            { session }
          );

          await this.db.sync_rules.updateMany(
            {
              _id: { $ne: this.group_id },
              state: storage.SyncRuleState.ACTIVE
            },
            {
              $set: {
                state: storage.SyncRuleState.STOP
              }
            },
            { session }
          );
        }
      });
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
          last_fatal_error: message
        }
      }
    );
  }

  async compact(options?: storage.CompactOptions) {
    return new MongoCompactor(this.db, this.group_id, options).compact();
  }

  private makeActiveCheckpoint(doc: SyncRuleCheckpointState | null) {
    return {
      checkpoint: utils.timestampToOpId(doc?.last_checkpoint ?? 0n),
      lsn: doc?.last_checkpoint_lsn ?? null
    };
  }

  /**
   * Instance-wide watch on the latest available checkpoint (op_id + lsn).
   */
  private async *watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<ReplicationCheckpoint> {
    // Use this form instead of (doc: SyncRuleCheckpointState | null = null),
    // otherwise we get weird "doc: never" issues.
    let doc = null as SyncRuleCheckpointState | null;
    let clusterTime = null as mongo.Timestamp | null;
    const syncRulesId = this.group_id;

    await this.db.client.withSession(async (session) => {
      doc = await this.db.sync_rules.findOne(
        {
          _id: syncRulesId,
          state: storage.SyncRuleState.ACTIVE
        },
        {
          session,
          sort: { _id: -1 },
          limit: 1,
          projection: {
            _id: 1,
            state: 1,
            last_checkpoint: 1,
            last_checkpoint_lsn: 1
          }
        }
      );
      const time = session.clusterTime?.clusterTime ?? null;
      clusterTime = time;
    });
    if (clusterTime == null) {
      throw new ServiceError(ErrorCode.PSYNC_S2401, 'Could not get clusterTime');
    }

    if (signal.aborted) {
      return;
    }

    if (doc == null) {
      // Sync rules not present or not active.
      // Abort the connections - clients will have to retry later.
      // Should this error instead?
      return;
    }

    yield this.makeActiveCheckpoint(doc);

    // We only watch changes to the active sync rules.
    // If it changes to inactive, we abort and restart with the new sync rules.

    const pipeline = this.getChangeStreamPipeline();

    const stream = this.db.sync_rules.watch(pipeline, {
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

    let lastOp: storage.ReplicationCheckpoint | null = null;
    let lastDoc: SyncRuleCheckpointState | null = doc;

    for await (const update of stream.stream()) {
      if (signal.aborted) {
        break;
      }
      if (update.operationType != 'insert' && update.operationType != 'update' && update.operationType != 'replace') {
        continue;
      }

      const doc = await this.getOperationDoc(lastDoc, update as lib_mongo.mongo.ChangeStreamDocument<SyncRuleDocument>);
      if (doc == null) {
        // Irrelevant update
        continue;
      }
      if (doc.state != storage.SyncRuleState.ACTIVE) {
        // Sync rules have changed - abort and restart.
        // Should this error instead?
        break;
      }

      lastDoc = doc;

      const op = this.makeActiveCheckpoint(doc);
      // Check for LSN / checkpoint changes - ignore other metadata changes
      if (lastOp == null || op.lsn != lastOp.lsn || op.checkpoint != lastOp.checkpoint) {
        lastOp = op;
        yield op;
      }
    }
  }

  // Nothing is done here until a subscriber starts to iterate
  private readonly sharedIter = new BroadcastIterable((signal) => {
    return this.watchActiveCheckpoint(signal);
  });

  /**
   * User-specific watch on the latest checkpoint and/or write checkpoint.
   */
  async *watchWriteCheckpoint(options: WatchWriteCheckpointOptions): AsyncIterable<storage.StorageCheckpointUpdate> {
    const { user_id, signal } = options;
    let lastCheckpoint: utils.OpId | null = null;
    let lastWriteCheckpoint: bigint | null = null;

    const iter = wrapWithAbort(this.sharedIter, signal);
    for await (const event of iter) {
      const { checkpoint, lsn } = event;

      // lsn changes are not important by itself.
      // What is important is:
      // 1. checkpoint (op_id) changes.
      // 2. write checkpoint changes for the specific user

      const lsnFilters: Record<string, string> = lsn ? { 1: lsn } : {};

      const currentWriteCheckpoint = await this.lastWriteCheckpoint({
        user_id,
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

      const updates: CheckpointChanges =
        lastCheckpoint == null
          ? {
              invalidateDataBuckets: true,
              invalidateParameterBuckets: true,
              updatedDataBuckets: [],
              updatedParameterBucketDefinitions: []
            }
          : await this.getCheckpointChanges({
              lastCheckpoint: lastCheckpoint,
              nextCheckpoint: checkpoint
            });

      lastWriteCheckpoint = currentWriteCheckpoint;
      lastCheckpoint = checkpoint;

      yield {
        base: event,
        writeCheckpoint: currentWriteCheckpoint,
        update: updates
      };
    }
  }

  private async getOperationDoc(
    lastDoc: SyncRuleCheckpointState,
    update: lib_mongo.mongo.ChangeStreamDocument<SyncRuleDocument>
  ): Promise<SyncRuleCheckpointState | null> {
    if (update.operationType == 'insert' || update.operationType == 'replace') {
      return update.fullDocument;
    } else if (update.operationType == 'update') {
      const updatedFields = update.updateDescription.updatedFields ?? {};
      if (lastDoc._id != update.documentKey._id) {
        throw new ServiceAssertionError(`Sync rules id mismatch: ${lastDoc._id} != ${update.documentKey._id}`);
      }

      const mergedDoc: SyncRuleCheckpointState = {
        _id: lastDoc._id,
        last_checkpoint: updatedFields.last_checkpoint ?? lastDoc.last_checkpoint,
        last_checkpoint_lsn: updatedFields.last_checkpoint_lsn ?? lastDoc.last_checkpoint_lsn,
        state: updatedFields.state ?? lastDoc.state
      };

      return mergedDoc;
    } else {
      // Unknown event type
      return null;
    }
  }

  private getChangeStreamPipeline() {
    const syncRulesId = this.group_id;
    const pipeline: mongo.Document[] = [
      {
        $match: {
          'documentKey._id': syncRulesId,
          operationType: { $in: ['insert', 'update', 'replace'] }
        }
      },
      {
        $project: {
          operationType: 1,
          'documentKey._id': 1,
          'updateDescription.updatedFields.state': 1,
          'updateDescription.updatedFields.last_checkpoint': 1,
          'updateDescription.updatedFields.last_checkpoint_lsn': 1,
          'fullDocument._id': 1,
          'fullDocument.state': 1,
          'fullDocument.last_checkpoint': 1,
          'fullDocument.last_checkpoint_lsn': 1
        }
      }
    ];
    return pipeline;
  }

  async getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<CheckpointChanges> {
    return CHECKPOINT_INVALIDATE_ALL;
  }
}
