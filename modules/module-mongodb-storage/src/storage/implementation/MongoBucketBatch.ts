import { mongo } from '@powersync/lib-service-mongodb';
import { SqlEventDescriptor, SqliteRow, SqlSyncRules } from '@powersync/service-sync-rules';
import * as bson from 'bson';

import {
  container,
  DisposableObserver,
  ErrorCode,
  errors,
  logger,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import { SaveOperationTag, storage, utils } from '@powersync/service-core';
import * as timers from 'node:timers/promises';
import { PowerSyncMongo } from './db.js';
import { CurrentBucket, CurrentDataDocument, SourceKey, SyncRuleDocument } from './models.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { batchCreateCustomWriteCheckpoints } from './MongoWriteCheckpointAPI.js';
import { cacheKey, OperationBatch, RecordOperation } from './OperationBatch.js';
import { PersistedBatch } from './PersistedBatch.js';
import { idPrefixFilter } from './util.js';

/**
 * 15MB
 */
const MAX_ROW_SIZE = 15 * 1024 * 1024;

// Currently, we can only have a single flush() at a time, since it locks the op_id sequence.
// While the MongoDB transaction retry mechanism handles this okay, using an in-process Mutex
// makes it more fair and has less overhead.
//
// In the future, we can investigate allowing multiple replication streams operating independently.
const replicationMutex = new utils.Mutex();

export interface MongoBucketBatchOptions {
  db: PowerSyncMongo;
  syncRules: SqlSyncRules;
  groupId: number;
  slotName: string;
  lastCheckpointLsn: string | null;
  keepaliveOp: string | null;
  noCheckpointBeforeLsn: string;
  storeCurrentData: boolean;
  /**
   * Set to true for initial replication.
   */
  skipExistingRows: boolean;
}

export class MongoBucketBatch
  extends DisposableObserver<storage.BucketBatchStorageListener>
  implements storage.BucketStorageBatch
{
  private readonly client: mongo.MongoClient;
  public readonly db: PowerSyncMongo;
  public readonly session: mongo.ClientSession;
  private readonly sync_rules: SqlSyncRules;

  private readonly group_id: number;

  private readonly slot_name: string;
  private readonly storeCurrentData: boolean;
  private readonly skipExistingRows: boolean;

  private batch: OperationBatch | null = null;
  private write_checkpoint_batch: storage.CustomWriteCheckpointOptions[] = [];

  /**
   * Last LSN received associated with a checkpoint.
   *
   * This could be either:
   * 1. A commit LSN.
   * 2. A keepalive message LSN.
   */
  private last_checkpoint_lsn: string | null = null;

  private no_checkpoint_before_lsn: string;

  private persisted_op: bigint | null = null;

  /**
   * For tests only - not for persistence logic.
   */
  public last_flushed_op: bigint | null = null;

  constructor(options: MongoBucketBatchOptions) {
    super();
    this.client = options.db.client;
    this.db = options.db;
    this.group_id = options.groupId;
    this.last_checkpoint_lsn = options.lastCheckpointLsn;
    this.no_checkpoint_before_lsn = options.noCheckpointBeforeLsn;
    this.session = this.client.startSession();
    this.slot_name = options.slotName;
    this.sync_rules = options.syncRules;
    this.storeCurrentData = options.storeCurrentData;
    this.skipExistingRows = options.skipExistingRows;
    this.batch = new OperationBatch();

    if (options.keepaliveOp) {
      this.persisted_op = BigInt(options.keepaliveOp);
    }
  }

  addCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): void {
    this.write_checkpoint_batch.push({
      ...checkpoint,
      sync_rules_id: this.group_id
    });
  }

  get lastCheckpointLsn() {
    return this.last_checkpoint_lsn;
  }

  async flush(): Promise<storage.FlushedResult | null> {
    let result: storage.FlushedResult | null = null;
    // One flush may be split over multiple transactions.
    // Each flushInner() is one transaction.
    while (this.batch != null) {
      let r = await this.flushInner();
      if (r) {
        result = r;
      }
    }
    await batchCreateCustomWriteCheckpoints(this.db, this.write_checkpoint_batch);
    this.write_checkpoint_batch = [];
    return result;
  }

  private async flushInner(): Promise<storage.FlushedResult | null> {
    const batch = this.batch;
    if (batch == null) {
      return null;
    }

    let last_op: bigint | null = null;
    let resumeBatch: OperationBatch | null = null;

    await this.withReplicationTransaction(`Flushing ${batch.length} ops`, async (session, opSeq) => {
      resumeBatch = await this.replicateBatch(session, batch, opSeq);

      last_op = opSeq.last();
    });

    // null if done, set if we need another flush
    this.batch = resumeBatch;

    if (last_op == null) {
      throw new ReplicationAssertionError('Unexpected last_op == null');
    }

    this.persisted_op = last_op;
    this.last_flushed_op = last_op;
    return { flushed_op: String(last_op) };
  }

  private async replicateBatch(
    session: mongo.ClientSession,
    batch: OperationBatch,
    op_seq: MongoIdSequence
  ): Promise<OperationBatch | null> {
    let sizes: Map<string, number> | undefined = undefined;
    if (this.storeCurrentData && !this.skipExistingRows) {
      // We skip this step if we don't store current_data, since the sizes will
      // always be small in that case.

      // With skipExistingRows, we don't load the full documents into memory,
      // so we can also skip the size lookup step.

      // Find sizes of current_data documents, to assist in intelligent batching without
      // exceeding memory limits.
      //
      // A previous attempt tried to do batching by the results of the current_data query
      // (automatically limited to 48MB(?) per batch by MongoDB). The issue is that it changes
      // the order of processing, which then becomes really tricky to manage.
      // This now takes 2+ queries, but doesn't have any issues with order of operations.
      const sizeLookups: SourceKey[] = batch.batch.map((r) => {
        return { g: this.group_id, t: r.record.sourceTable.id, k: r.beforeId };
      });

      sizes = new Map<string, number>();

      const sizeCursor: mongo.AggregationCursor<{ _id: SourceKey; size: number }> = this.db.current_data.aggregate(
        [
          {
            $match: {
              _id: { $in: sizeLookups }
            }
          },
          {
            $project: {
              _id: 1,
              size: { $bsonSize: '$$ROOT' }
            }
          }
        ],
        { session }
      );
      for await (let doc of sizeCursor.stream()) {
        const key = cacheKey(doc._id.t, doc._id.k);
        sizes.set(key, doc.size);
      }
    }

    // If set, we need to start a new transaction with this batch.
    let resumeBatch: OperationBatch | null = null;
    let transactionSize = 0;

    // Now batch according to the sizes
    // This is a single batch if storeCurrentData == false
    for await (let b of batch.batched(sizes)) {
      if (resumeBatch) {
        for (let op of b) {
          resumeBatch.push(op);
        }
        continue;
      }
      const lookups: SourceKey[] = b.map((r) => {
        return { g: this.group_id, t: r.record.sourceTable.id, k: r.beforeId };
      });
      let current_data_lookup = new Map<string, CurrentDataDocument>();
      // With skipExistingRows, we only need to know whether or not the row exists.
      const projection = this.skipExistingRows ? { _id: 1 } : undefined;
      const cursor = this.db.current_data.find(
        {
          _id: { $in: lookups }
        },
        { session, projection }
      );
      for await (let doc of cursor.stream()) {
        current_data_lookup.set(cacheKey(doc._id.t, doc._id.k), doc);
      }

      let persistedBatch: PersistedBatch | null = new PersistedBatch(this.group_id, transactionSize);

      for (let op of b) {
        if (resumeBatch) {
          resumeBatch.push(op);
          continue;
        }
        const currentData = current_data_lookup.get(op.internalBeforeKey) ?? null;
        if (currentData != null) {
          // If it will be used again later, it will be set again using nextData below
          current_data_lookup.delete(op.internalBeforeKey);
        }
        const nextData = this.saveOperation(persistedBatch!, op, currentData, op_seq);
        if (nextData != null) {
          // Update our current_data and size cache
          current_data_lookup.set(op.internalAfterKey!, nextData);
          sizes?.set(op.internalAfterKey!, nextData.data.length());
        }

        if (persistedBatch!.shouldFlushTransaction()) {
          // Transaction is getting big.
          // Flush, and resume in a new transaction.
          await persistedBatch!.flush(this.db, this.session);
          persistedBatch = null;
          // Computing our current progress is a little tricky here, since
          // we're stopping in the middle of a batch.
          // We create a new batch, and push any remaining operations to it.
          resumeBatch = new OperationBatch();
        }
      }

      if (persistedBatch) {
        transactionSize = persistedBatch.currentSize;
        await persistedBatch.flush(this.db, this.session);
      }
    }

    return resumeBatch;
  }

  private saveOperation(
    batch: PersistedBatch,
    operation: RecordOperation,
    current_data: CurrentDataDocument | null,
    opSeq: MongoIdSequence
  ) {
    const record = operation.record;
    const beforeId = operation.beforeId;
    const afterId = operation.afterId;
    let after = record.after;
    const sourceTable = record.sourceTable;

    let existing_buckets: CurrentBucket[] = [];
    let new_buckets: CurrentBucket[] = [];
    let existing_lookups: bson.Binary[] = [];
    let new_lookups: bson.Binary[] = [];

    const before_key: SourceKey = { g: this.group_id, t: record.sourceTable.id, k: beforeId };

    if (this.skipExistingRows) {
      if (record.tag == SaveOperationTag.INSERT) {
        if (current_data != null) {
          // Initial replication, and we already have the record.
          // This may be a different version of the record, but streaming replication
          // will take care of that.
          // Skip the insert here.
          return null;
        }
      } else {
        throw new ReplicationAssertionError(`${record.tag} not supported with skipExistingRows: true`);
      }
    }

    if (record.tag == SaveOperationTag.UPDATE) {
      const result = current_data;
      if (result == null) {
        // Not an error if we re-apply a transaction
        existing_buckets = [];
        existing_lookups = [];
        // Log to help with debugging if there was a consistency issue
        if (this.storeCurrentData) {
          logger.warn(
            `Cannot find previous record for update on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
          );
        }
      } else {
        existing_buckets = result.buckets;
        existing_lookups = result.lookups;
        if (this.storeCurrentData) {
          const data = bson.deserialize(
            (result.data as mongo.Binary).buffer,
            storage.BSON_DESERIALIZE_OPTIONS
          ) as SqliteRow;
          after = storage.mergeToast(after!, data);
        }
      }
    } else if (record.tag == SaveOperationTag.DELETE) {
      const result = current_data;
      if (result == null) {
        // Not an error if we re-apply a transaction
        existing_buckets = [];
        existing_lookups = [];
        // Log to help with debugging if there was a consistency issue
        if (this.storeCurrentData) {
          logger.warn(
            `Cannot find previous record for delete on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
          );
        }
      } else {
        existing_buckets = result.buckets;
        existing_lookups = result.lookups;
      }
    }

    let afterData: bson.Binary | undefined;
    if (afterId != null && !this.storeCurrentData) {
      afterData = new bson.Binary(bson.serialize({}));
    } else if (afterId != null) {
      try {
        // This will fail immediately if the record is > 16MB.
        afterData = new bson.Binary(bson.serialize(after!));
        // We additionally make sure it's <= 15MB - we need some margin for metadata.
        if (afterData.length() > MAX_ROW_SIZE) {
          throw new ServiceError(ErrorCode.PSYNC_S1002, `Row too large: ${afterData.length()}`);
        }
      } catch (e) {
        // Replace with empty values, equivalent to TOAST values
        after = Object.fromEntries(
          Object.entries(after!).map(([key, value]) => {
            return [key, undefined];
          })
        );
        afterData = new bson.Binary(bson.serialize(after!));

        container.reporter.captureMessage(
          `Data too big on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${e.message}`,
          {
            level: errors.ErrorSeverity.WARNING,
            metadata: {
              replication_slot: this.slot_name,
              table: record.sourceTable.qualifiedName
            }
          }
        );
      }
    }

    // 2. Save bucket data
    if (beforeId != null && (afterId == null || !storage.replicaIdEquals(beforeId, afterId))) {
      // Source ID updated
      if (sourceTable.syncData) {
        // Delete old record
        batch.saveBucketData({
          op_seq: opSeq,
          sourceKey: beforeId,
          table: sourceTable,
          before_buckets: existing_buckets,
          evaluated: []
        });
        // Clear this, so we don't also try to REMOVE for the new id
        existing_buckets = [];
      }

      if (sourceTable.syncParameters) {
        // Delete old parameters
        batch.saveParameterData({
          op_seq: opSeq,
          sourceKey: beforeId,
          sourceTable,
          evaluated: [],
          existing_lookups
        });
        existing_lookups = [];
      }
    }

    // If we re-apply a transaction, we can end up with a partial row.
    //
    // We may end up with toasted values, which means the record is not quite valid.
    // However, it will be valid by the end of the transaction.
    //
    // In this case, we don't save the op, but we do save the current data.
    if (afterId && after && utils.isCompleteRow(this.storeCurrentData, after)) {
      // Insert or update
      if (sourceTable.syncData) {
        const { results: evaluated, errors: syncErrors } = this.sync_rules.evaluateRowWithErrors({
          record: after,
          sourceTable
        });

        for (let error of syncErrors) {
          container.reporter.captureMessage(
            `Failed to evaluate data query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`,
            {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                replication_slot: this.slot_name,
                table: record.sourceTable.qualifiedName
              }
            }
          );
          logger.error(
            `Failed to evaluate data query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`
          );
        }

        // Save new one
        batch.saveBucketData({
          op_seq: opSeq,
          sourceKey: afterId,
          evaluated,
          table: sourceTable,
          before_buckets: existing_buckets
        });
        new_buckets = evaluated.map((e) => {
          return {
            bucket: e.bucket,
            table: e.table,
            id: e.id
          };
        });
      }

      if (sourceTable.syncParameters) {
        // Parameters
        const { results: paramEvaluated, errors: paramErrors } = this.sync_rules.evaluateParameterRowWithErrors(
          sourceTable,
          after
        );

        for (let error of paramErrors) {
          container.reporter.captureMessage(
            `Failed to evaluate parameter query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`,
            {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                replication_slot: this.slot_name,
                table: record.sourceTable.qualifiedName
              }
            }
          );
          logger.error(
            `Failed to evaluate parameter query on ${record.sourceTable.qualifiedName}.${after.id}: ${error.error}`
          );
        }

        batch.saveParameterData({
          op_seq: opSeq,
          sourceKey: afterId,
          sourceTable,
          evaluated: paramEvaluated,
          existing_lookups
        });
        new_lookups = paramEvaluated.map((p) => {
          return storage.serializeLookup(p.lookup);
        });
      }
    }

    let result: CurrentDataDocument | null = null;

    // 5. TOAST: Update current data and bucket list.
    if (afterId) {
      // Insert or update
      const after_key: SourceKey = { g: this.group_id, t: sourceTable.id, k: afterId };
      batch.upsertCurrentData(after_key, {
        data: afterData,
        buckets: new_buckets,
        lookups: new_lookups
      });
      result = {
        _id: after_key,
        data: afterData!,
        buckets: new_buckets,
        lookups: new_lookups
      };
    }

    if (afterId == null || !storage.replicaIdEquals(beforeId, afterId)) {
      // Either a delete (afterId == null), or replaced the old replication id
      batch.deleteCurrentData(before_key);
    }
    return result;
  }

  private async withTransaction(cb: () => Promise<void>) {
    await replicationMutex.exclusiveLock(async () => {
      await this.session.withTransaction(
        async () => {
          try {
            await cb();
          } catch (e: unknown) {
            if (e instanceof mongo.MongoError && e.hasErrorLabel('TransientTransactionError')) {
              // Likely write conflict caused by concurrent write stream replicating
            } else {
              logger.warn('Transaction error', e as Error);
            }
            await timers.setTimeout(Math.random() * 50);
            throw e;
          }
        },
        { maxCommitTimeMS: 10000 }
      );
    });
  }

  private async withReplicationTransaction(
    description: string,
    callback: (session: mongo.ClientSession, opSeq: MongoIdSequence) => Promise<void>
  ): Promise<void> {
    let flushTry = 0;

    const start = Date.now();
    const lastTry = start + 90000;

    const session = this.session;

    await this.withTransaction(async () => {
      flushTry += 1;
      if (flushTry % 10 == 0) {
        logger.info(`${this.slot_name} ${description} - try ${flushTry}`);
      }
      if (flushTry > 20 && Date.now() > lastTry) {
        throw new ServiceError(ErrorCode.PSYNC_S1402, 'Max transaction tries exceeded');
      }

      const next_op_id_doc = await this.db.op_id_sequence.findOneAndUpdate(
        {
          _id: 'main'
        },
        {
          $setOnInsert: { op_id: 0n },
          $set: {
            // Force update to ensure we get a mongo lock
            ts: Date.now()
          }
        },
        {
          upsert: true,
          returnDocument: 'after',
          session
        }
      );
      const opSeq = new MongoIdSequence(next_op_id_doc?.op_id ?? 0n);

      await callback(session, opSeq);

      await this.db.op_id_sequence.updateOne(
        {
          _id: 'main'
        },
        {
          $set: {
            op_id: opSeq.last()
          }
        },
        {
          session
        }
      );

      await this.db.sync_rules.updateOne(
        {
          _id: this.group_id
        },
        {
          $set: {
            last_keepalive_ts: new Date()
          }
        },
        { session }
      );
    });
  }

  async [Symbol.asyncDispose]() {
    await this.session.endSession();
    super[Symbol.dispose]();
  }

  private lastWaitingLogThottled = 0;

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<boolean> {
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    await this.flush();

    if (this.last_checkpoint_lsn != null && lsn < this.last_checkpoint_lsn) {
      // When re-applying transactions, don't create a new checkpoint until
      // we are past the last transaction.
      logger.info(`Re-applied transaction ${lsn} - skipping checkpoint`);
      return false;
    }
    if (lsn < this.no_checkpoint_before_lsn) {
      if (Date.now() - this.lastWaitingLogThottled > 5_000) {
        logger.info(
          `Waiting until ${this.no_checkpoint_before_lsn} before creating checkpoint, currently at ${lsn}. Persisted op: ${this.persisted_op}`
        );
        this.lastWaitingLogThottled = Date.now();
      }

      // Edge case: During initial replication, we have a no_checkpoint_before_lsn set,
      // and don't actually commit the snapshot.
      // The first commit can happen from an implicit keepalive message.
      // That needs the persisted_op to get an accurate checkpoint, so
      // we persist that in keepalive_op.

      await this.db.sync_rules.updateOne(
        {
          _id: this.group_id
        },
        {
          $set: {
            keepalive_op: this.persisted_op == null ? null : String(this.persisted_op)
          }
        },
        { session: this.session }
      );

      return false;
    }

    if (!createEmptyCheckpoints && this.persisted_op == null) {
      return false;
    }

    const now = new Date();
    const update: Partial<SyncRuleDocument> = {
      last_checkpoint_lsn: lsn,
      last_checkpoint_ts: now,
      last_keepalive_ts: now,
      snapshot_done: true,
      last_fatal_error: null,
      keepalive_op: null
    };

    if (this.persisted_op != null) {
      update.last_checkpoint = this.persisted_op;
    }

    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: update
      },
      { session: this.session }
    );
    this.persisted_op = null;
    this.last_checkpoint_lsn = lsn;
    return true;
  }

  async keepalive(lsn: string): Promise<boolean> {
    if (this.last_checkpoint_lsn != null && lsn <= this.last_checkpoint_lsn) {
      // No-op
      return false;
    }

    if (lsn < this.no_checkpoint_before_lsn) {
      return false;
    }

    if (this.persisted_op != null) {
      // The commit may have been skipped due to "no_checkpoint_before_lsn".
      // Apply it now if relevant
      logger.info(`Commit due to keepalive at ${lsn} / ${this.persisted_op}`);
      return await this.commit(lsn);
    }

    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          last_checkpoint_lsn: lsn,
          snapshot_done: true,
          last_fatal_error: null,
          last_keepalive_ts: new Date()
        }
      },
      { session: this.session }
    );
    this.last_checkpoint_lsn = lsn;

    return true;
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    const { after, before, sourceTable, tag } = record;
    for (const event of this.getTableEvents(sourceTable)) {
      this.iterateListeners((cb) =>
        cb.replicationEvent?.({
          batch: this,
          table: sourceTable,
          data: {
            op: tag,
            after: after && utils.isCompleteRow(this.storeCurrentData, after) ? after : undefined,
            before: before && utils.isCompleteRow(this.storeCurrentData, before) ? before : undefined
          },
          event
        })
      );
    }

    /**
     * Return if the table is just an event table
     */
    if (!sourceTable.syncData && !sourceTable.syncParameters) {
      return null;
    }

    logger.debug(`Saving ${record.tag}:${record.before?.id}/${record.after?.id}`);

    this.batch ??= new OperationBatch();
    this.batch.push(new RecordOperation(record));

    if (this.batch.shouldFlush()) {
      const r = await this.flush();
      // HACK: Give other streams a  chance to also flush
      await timers.setTimeout(5);
      return r;
    }
    return null;
  }

  /**
   * Drop is equivalent to TRUNCATE, plus removing our record of the table.
   */
  async drop(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.truncate(sourceTables);
    const result = await this.flush();

    await this.withTransaction(async () => {
      for (let table of sourceTables) {
        await this.db.source_tables.deleteOne({ _id: table.id });
      }
    });
    return result;
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.flush();

    let last_op: bigint | null = null;
    for (let table of sourceTables) {
      last_op = await this.truncateSingle(table);
    }

    if (last_op) {
      this.persisted_op = last_op;
    }

    return {
      flushed_op: String(last_op!)
    };
  }

  async truncateSingle(sourceTable: storage.SourceTable): Promise<bigint> {
    let last_op: bigint | null = null;

    // To avoid too large transactions, we limit the amount of data we delete per transaction.
    // Since we don't use the record data here, we don't have explicit size limits per batch.
    const BATCH_LIMIT = 2000;

    let lastBatchCount = BATCH_LIMIT;
    while (lastBatchCount == BATCH_LIMIT) {
      await this.withReplicationTransaction(`Truncate ${sourceTable.qualifiedName}`, async (session, opSeq) => {
        const current_data_filter: mongo.Filter<CurrentDataDocument> = {
          _id: idPrefixFilter<SourceKey>({ g: this.group_id, t: sourceTable.id }, ['k'])
        };

        const cursor = this.db.current_data.find(current_data_filter, {
          projection: {
            _id: 1,
            buckets: 1,
            lookups: 1
          },
          limit: BATCH_LIMIT,
          session: session
        });
        const batch = await cursor.toArray();
        const persistedBatch = new PersistedBatch(this.group_id, 0);

        for (let value of batch) {
          persistedBatch.saveBucketData({
            op_seq: opSeq,
            before_buckets: value.buckets,
            evaluated: [],
            table: sourceTable,
            sourceKey: value._id.k
          });
          persistedBatch.saveParameterData({
            op_seq: opSeq,
            existing_lookups: value.lookups,
            evaluated: [],
            sourceTable: sourceTable,
            sourceKey: value._id.k
          });

          persistedBatch.deleteCurrentData(value._id);
        }
        await persistedBatch.flush(this.db, session);
        lastBatchCount = batch.length;

        last_op = opSeq.last();
      });
    }

    return last_op!;
  }

  async markSnapshotDone(tables: storage.SourceTable[], no_checkpoint_before_lsn: string) {
    const session = this.session;
    const ids = tables.map((table) => table.id);

    await this.withTransaction(async () => {
      await this.db.source_tables.updateMany(
        { _id: { $in: ids } },
        {
          $set: {
            snapshot_done: true
          }
        },
        { session }
      );

      if (no_checkpoint_before_lsn > this.no_checkpoint_before_lsn) {
        this.no_checkpoint_before_lsn = no_checkpoint_before_lsn;

        await this.db.sync_rules.updateOne(
          {
            _id: this.group_id
          },
          {
            $set: {
              no_checkpoint_before: no_checkpoint_before_lsn,
              last_keepalive_ts: new Date()
            }
          },
          { session: this.session }
        );
      }
    });
    return tables.map((table) => {
      const copy = new storage.SourceTable(
        table.id,
        table.connectionTag,
        table.objectId,
        table.schema,
        table.table,
        table.replicaIdColumns,
        table.snapshotComplete
      );
      copy.syncData = table.syncData;
      copy.syncParameters = table.syncParameters;
      return copy;
    });
  }

  /**
   * Gets relevant {@link SqlEventDescriptor}s for the given {@link SourceTable}
   */
  protected getTableEvents(table: storage.SourceTable): SqlEventDescriptor[] {
    return this.sync_rules.event_descriptors.filter((evt) =>
      [...evt.getSourceTables()].some((sourceTable) => sourceTable.matches(table))
    );
  }
}

export function currentBucketKey(b: CurrentBucket) {
  return `${b.bucket}/${b.table}/${b.id}`;
}
