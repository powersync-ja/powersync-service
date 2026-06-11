import { mongo } from '@powersync/lib-service-mongodb';
import { HydratedSyncConfig, SqlEventDescriptor, SqliteRow, SqliteValue } from '@powersync/service-sync-rules';
import * as bson from 'bson';

import {
  BaseObserver,
  container,
  ErrorCode,
  errors,
  Logger,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  BucketStorageMarkRecordUnavailable,
  deserializeBson,
  InternalOpId,
  isCompleteRow,
  PerformanceTracer,
  SaveOperationTag,
  storage,
  utils
} from '@powersync/service-core';
import * as timers from 'node:timers/promises';
import { mongoTableId } from '../../utils/util.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { PersistedBatch } from './common/PersistedBatch.js';
import { LoadedSourceRecord, SourceRecordStore } from './common/SourceRecordStore.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { MAX_ROW_SIZE } from './MongoBucketBatchShared.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { batchCreateCustomWriteCheckpoints } from './MongoWriteCheckpointAPI.js';
import { OperationBatch, RecordOperation } from './OperationBatch.js';

// Currently, we can only have a single flush() at a time, since it locks the op_id sequence.
// While the MongoDB transaction retry mechanism handles this okay, using an in-process Mutex
// makes it more fair and has less overhead.
//
// In the future, we can investigate allowing multiple replication streams operating independently.
const replicationMutex = new utils.Mutex();

export interface MongoBucketBatchOptions {
  db: VersionedPowerSyncMongo;
  syncRules: HydratedSyncConfig;
  replicationStreamId: number;
  replicationStreamName: string;
  syncConfigIds?: bson.ObjectId[];
  lastCheckpointLsn: string | null;
  keepaliveOp: InternalOpId | null;
  resumeFromLsn: string | null;
  storeCurrentData: boolean;
  mapping: BucketDefinitionMapping;
  /**
   * Set to true for initial replication.
   */
  skipExistingRows: boolean;

  markRecordUnavailable: BucketStorageMarkRecordUnavailable | undefined;
  hooks: storage.StorageHooks | undefined;

  logger: Logger;
  tracer?: PerformanceTracer<'storage' | 'evaluate'>;
}

export abstract class MongoBucketBatch
  extends BaseObserver<storage.BucketBatchStorageListener>
  implements storage.BucketStorageBatch
{
  protected readonly options: MongoBucketBatchOptions;
  protected logger: Logger;

  private readonly client: mongo.MongoClient;
  readonly db: VersionedPowerSyncMongo;
  public readonly session: mongo.ClientSession;
  protected readonly sync_rules: HydratedSyncConfig;

  protected readonly replicationStreamId: number;

  private readonly replicationStreamName: string;
  /**
   * Source-level setting for whether raw row data should be stored in current_data.
   *
   * Some sources always send complete rows (MongoDB, MySQL with binlog_row_image=full),
   * in which case this is false for the whole batch. For sources where it depends on the
   * table (Postgres REPLICA IDENTITY), this is true and the decision is refined per-table
   * via SourceTable.storeCurrentData. The effective per-record value is the conjunction of
   * the two.
   */
  private readonly storeCurrentData: boolean;
  public readonly skipExistingRows: boolean;
  protected readonly mapping: BucketDefinitionMapping;

  private batch: OperationBatch | null = null;
  private write_checkpoint_batch: storage.CustomWriteCheckpointOptions[] = [];
  private markRecordUnavailable: BucketStorageMarkRecordUnavailable | undefined;
  private hooks: storage.StorageHooks | undefined;
  private clearedError = false;

  private tracer: PerformanceTracer<'storage' | 'evaluate'>;

  /**
   * Last LSN received associated with a checkpoint.
   *
   * This could be either:
   * 1. A commit LSN.
   * 2. A keepalive message LSN.
   */
  protected last_checkpoint_lsn: string | null = null;

  protected persisted_op: InternalOpId | null = null;

  /**
   * Last written op, if any. This may not reflect a consistent checkpoint.
   */
  public last_flushed_op: InternalOpId | null = null;

  /**
   * lastCheckpointLsn is the last consistent commit.
   *
   * While that is generally a "safe" point to resume from, there are cases where we may want to resume from a different point:
   * 1. After an initial snapshot, we don't have a consistent commit yet, but need to resume from the snapshot LSN.
   * 2. If "no_checkpoint_before_lsn" is set far in advance, it may take a while to reach that point. We
   *    may want to resume at incremental points before that.
   *
   * This is set when creating the batch, but may not be updated afterwards.
   */
  public resumeFromLsn: string | null = null;

  constructor(options: MongoBucketBatchOptions) {
    super();
    this.logger = options.logger;
    this.options = options;
    this.client = options.db.client;
    this.db = options.db;
    this.replicationStreamId = options.replicationStreamId;
    this.last_checkpoint_lsn = options.lastCheckpointLsn;
    this.resumeFromLsn = options.resumeFromLsn;
    this.session = this.client.startSession();
    this.replicationStreamName = options.replicationStreamName;
    this.sync_rules = options.syncRules;
    this.storeCurrentData = options.storeCurrentData;
    this.mapping = options.mapping;
    this.skipExistingRows = options.skipExistingRows;
    this.markRecordUnavailable = options.markRecordUnavailable;
    this.hooks = options.hooks;
    this.batch = new OperationBatch();

    this.persisted_op = options.keepaliveOp ?? null;
    this.tracer = options.tracer ?? new PerformanceTracer('MongoDB storage');
  }

  addCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): void {
    this.write_checkpoint_batch.push({
      ...checkpoint,
      sync_rules_id: this.replicationStreamId
    });
  }

  get lastCheckpointLsn() {
    return this.last_checkpoint_lsn;
  }

  abstract resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult>;

  protected abstract createPersistedBatch(writtenSize: number): PersistedBatch;

  protected abstract get sourceRecordStore(): SourceRecordStore;

  protected abstract cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]): Promise<void>;

  abstract commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult>;

  abstract keepalive(lsn: string): Promise<storage.CheckpointResult>;

  abstract setResumeLsn(lsn: string): Promise<void>;

  abstract getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null>;

  abstract markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void>;

  abstract markSnapshotDone(no_checkpoint_before_lsn: string, options?: { throwOnConflict?: boolean }): Promise<void>;

  abstract markTableSnapshotRequired(table: storage.SourceTable): Promise<void>;

  abstract markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]>;

  async flush(options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    let result: storage.FlushedResult | null = null;
    // One flush may be split over multiple transactions.
    // Each flushInner() is one transaction.
    while (this.batch != null || this.write_checkpoint_batch.length > 0) {
      let r = await this.flushInner(options);
      if (r) {
        result = r;
      }
    }
    return result;
  }

  private async flushInner(options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    const batch = this.batch;
    let last_op: InternalOpId | null = null;
    let resumeBatch: OperationBatch | null = null;

    using _ = this.tracer.span('storage', 'flush');

    await this.hooks?.beforeBatchFlush?.(this);

    await this.withReplicationTransaction(`Flushing ${batch?.length ?? 0} ops`, async (session, opSeq) => {
      if (batch != null) {
        resumeBatch = await this.replicateBatch(session, batch, opSeq, options);
      }

      if (this.write_checkpoint_batch.length > 0) {
        this.logger.info(`Writing ${this.write_checkpoint_batch.length} custom write checkpoints`);
        await batchCreateCustomWriteCheckpoints(this.db, session, this.write_checkpoint_batch, opSeq.next());
        this.write_checkpoint_batch = [];
      }

      last_op = opSeq.last();
    });

    // null if done, set if we need another flush
    this.batch = resumeBatch;

    if (last_op == null) {
      throw new ReplicationAssertionError('Unexpected last_op == null');
    }

    this.persisted_op = last_op;
    this.last_flushed_op = last_op;
    await this.hooks?.afterBatchFlush?.(this);
    return { flushed_op: last_op };
  }

  private async replicateBatch(
    session: mongo.ClientSession,
    batch: OperationBatch,
    op_seq: MongoIdSequence,
    options?: storage.BucketBatchCommitOptions
  ): Promise<OperationBatch | null> {
    let sizes: Map<string, number> | undefined = undefined;
    using _ = this.tracer.span('storage', 'replicate_batch');
    // Only look up current_data sizes if the batch stores current_data and at least one
    // table in it does too (per-table can disable it, e.g. Postgres REPLICA IDENTITY FULL).
    const anyTableStoresCurrentData =
      this.storeCurrentData && batch.batch.some((r) => r.record.sourceTable.storeCurrentData);
    if (anyTableStoresCurrentData && !this.skipExistingRows) {
      // We skip this step if no tables store current_data, since the sizes will
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
      // Within this branch this.storeCurrentData is true, so the per-table flag is the
      // effective value - only look up sizes for tables that actually store current_data.
      const sizeLookups = batch.batch
        .filter((r) => r.record.sourceTable.storeCurrentData)
        .map((r) => ({
          sourceTableId: mongoTableId(r.record.sourceTable.id),
          replicaId: r.beforeId
        }));

      sizes = await this.sourceRecordStore.loadSizes(session, sizeLookups);
    }

    // If set, we need to start a new transaction with this batch.
    let resumeBatch: OperationBatch | null = null;
    let transactionSize = 0;

    let didFlush = false;

    // Now batch according to the sizes
    // This is a single batch if storeCurrentData == false
    for await (let b of batch.batched(sizes)) {
      if (resumeBatch) {
        for (let op of b) {
          resumeBatch.push(op);
        }
        continue;
      }
      using lookupSpan = this.tracer.span('storage', 'lookup');
      const lookups = b.map((r) => ({
        sourceTableId: mongoTableId(r.record.sourceTable.id),
        replicaId: r.beforeId
      }));
      let sourceRecordLookup = await this.sourceRecordStore.loadDocuments(session, lookups, this.skipExistingRows);
      lookupSpan.end();

      let persistedBatch: PersistedBatch | null = this.createPersistedBatch(transactionSize);

      // The current code structure makes it tricky to cleanly split this span from the one
      // where fluhsing. So we manually end and re-create this span whenever we flush.
      let evalSpan = this.tracer.span('evaluate');
      for (let op of b) {
        if (resumeBatch) {
          resumeBatch.push(op);
          continue;
        }
        const sourceRecord = sourceRecordLookup.get(op.internalBeforeKey) ?? null;
        if (sourceRecord != null) {
          // If it will be used again later, it will be set again using nextData below
          sourceRecordLookup.delete(op.internalBeforeKey);
        }
        const nextData = this.saveOperation(persistedBatch!, op, sourceRecord, op_seq);
        if (nextData != null) {
          // Update our current_data and size cache
          sourceRecordLookup.set(op.internalAfterKey!, nextData);
          sizes?.set(op.internalAfterKey!, nextData.data?.length() ?? 0);
        }

        if (persistedBatch!.shouldFlushTransaction()) {
          evalSpan.end();
          // Transaction is getting big.
          // Flush, and resume in a new transaction.
          using persistSpan = this.tracer.span('storage', 'persist_flush');
          const { flushedAny } = await persistedBatch!.flush(this.session, options);

          didFlush ||= flushedAny;
          persistedBatch = null;
          // Computing our current progress is a little tricky here, since
          // we're stopping in the middle of a batch.
          // We create a new batch, and push any remaining operations to it.
          resumeBatch = new OperationBatch();
          persistSpan.end();
          evalSpan = this.tracer.span('evaluate');
        }
      }
      evalSpan.end();

      if (persistedBatch) {
        transactionSize = persistedBatch.currentSize;
        using _ = this.tracer.span('storage', 'persist_flush');
        const { flushedAny } = await persistedBatch.flush(this.session, options);
        didFlush ||= flushedAny;
      }
    }

    if (didFlush) {
      using _ = this.tracer.span('storage', 'clear_error');
      await this.clearError();
    }

    return resumeBatch?.hasData() ? resumeBatch : null;
  }

  private saveOperation(
    batch: PersistedBatch,
    operation: RecordOperation,
    sourceRecord: LoadedSourceRecord | null,
    opSeq: MongoIdSequence
  ) {
    const record = operation.record;
    const beforeId = operation.beforeId;
    const afterId = operation.afterId;
    let after = record.after;
    const sourceTable = record.sourceTable;
    // Effective per-record flag: store current_data only if both the batch (source-level,
    // e.g. Postgres) and the table (e.g. non-FULL replica identity) require it.
    const storeCurrentData = this.storeCurrentData && sourceTable.storeCurrentData;

    let existing_buckets: LoadedSourceRecord['buckets'] = [];
    let new_buckets: LoadedSourceRecord['buckets'] = [];
    let existing_lookups: LoadedSourceRecord['lookups'] = [];
    let new_lookups: LoadedSourceRecord['lookups'] = [];

    const sourceTableId = mongoTableId(record.sourceTable.id);

    if (this.skipExistingRows) {
      if (record.tag == SaveOperationTag.INSERT) {
        if (sourceRecord != null) {
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
      const result = sourceRecord;
      if (result == null) {
        // Not an error if we re-apply a transaction
        existing_buckets = [];
        existing_lookups = [];
        if (!isCompleteRow(storeCurrentData, after!)) {
          if (this.markRecordUnavailable != null) {
            // This will trigger a "resnapshot" of the record.
            // This is not relevant if storeCurrentData is false, since we'll get the full row
            // directly in the replication stream.
            this.markRecordUnavailable(record);
          } else {
            // Log to help with debugging if there was a consistency issue
            this.logger.warn(
              `Cannot find previous record for update on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
            );
          }
        }
      } else {
        existing_buckets = result.buckets;
        existing_lookups = result.lookups;
        if (storeCurrentData && result.data != null) {
          const data = deserializeBson(result.data.buffer) as SqliteRow;
          after = storage.mergeToast<SqliteValue>(after!, data);
        }
      }
    } else if (record.tag == SaveOperationTag.DELETE) {
      const result = sourceRecord;
      if (result == null) {
        // Not an error if we re-apply a transaction
        existing_buckets = [];
        existing_lookups = [];
        // Log to help with debugging if there was a consistency issue.
        // Gate on the batch-level flag: FULL tables (per-record flag false) still get a
        // current_data entry, so a missing record on DELETE is meaningful for them too.
        if (this.storeCurrentData && this.markRecordUnavailable == null) {
          this.logger.warn(
            `Cannot find previous record for delete on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
          );
        }
      } else {
        existing_buckets = result.buckets;
        existing_lookups = result.lookups;
      }
    }

    let afterData: bson.Binary | null = null;
    if (afterId != null && !storeCurrentData) {
      afterData = null;
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
              replication_slot: this.replicationStreamName,
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
    if (afterId && after && utils.isCompleteRow(storeCurrentData, after)) {
      // Insert or update
      if (sourceTable.syncData) {
        const { results, errors: syncErrors } = this.sync_rules.evaluateRowWithErrors({
          record: after,
          sourceTable: sourceTable.ref,
          bucketDataSources: sourceTable.bucketDataSources
        });
        const evaluated = results;

        for (let error of syncErrors) {
          container.reporter.captureMessage(
            `Failed to evaluate data query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`,
            {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                replication_slot: this.replicationStreamName,
                table: record.sourceTable.qualifiedName
              }
            }
          );
          this.logger.error(
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
        new_buckets = this.sourceRecordStore.mapEvaluatedBuckets(evaluated);
      }

      if (sourceTable.syncParameters) {
        // Parameters
        const { results: paramEvaluated, errors: paramErrors } = this.sync_rules.evaluateParameterRowWithErrors(
          sourceTable.ref,
          after,
          { parameterLookupSources: sourceTable.parameterLookupSources }
        );

        for (let error of paramErrors) {
          container.reporter.captureMessage(
            `Failed to evaluate parameter query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`,
            {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                replication_slot: this.replicationStreamName,
                table: record.sourceTable.qualifiedName
              }
            }
          );
          this.logger.error(
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
        new_lookups = this.sourceRecordStore.mapParameterLookups(paramEvaluated);
      }
    }

    let result: LoadedSourceRecord | null = null;

    // 5. TOAST: Update current data and bucket list.
    if (afterId) {
      // Insert or update
      batch.upsertCurrentData({
        sourceTableId,
        replicaId: afterId,
        data: afterData,
        buckets: new_buckets,
        lookups: new_lookups
      });
      result = {
        sourceTableId,
        replicaId: afterId,
        data: afterData,
        buckets: new_buckets,
        lookups: new_lookups,
        cacheKey: operation.internalAfterKey!
      };
    }

    if (afterId == null || !storage.replicaIdEquals(beforeId, afterId)) {
      // Either a delete (afterId == null), or replaced the old replication id
      // Note that this is a soft delete.
      // We don't specifically need a new or unique op_id here, but it must be greater than the
      // last checkpoint, so we use next().
      batch.softDeleteCurrentData(sourceTableId, beforeId, opSeq.next());
    }
    return result;
  }

  protected async withTransaction(cb: () => Promise<void>) {
    using lockSpan = this.tracer.span('storage', 'internal_lock');
    await replicationMutex.exclusiveLock(async () => {
      lockSpan.end();
      await this.session.withTransaction(
        async () => {
          try {
            await cb();
          } catch (e: unknown) {
            if (e instanceof mongo.MongoError && e.hasErrorLabel('TransientTransactionError')) {
              // Likely write conflict caused by concurrent write stream replicating
            } else {
              this.logger.warn('Transaction error', e as Error);
            }
            const delay = Math.random() * 50;
            using _ = this.tracer.span('storage', 'retry_delay');
            await timers.setTimeout(delay);
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
        this.logger.info(`${description} - try ${flushTry}`);
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
          _id: this.replicationStreamId
        },
        {
          $set: {
            last_keepalive_ts: new Date()
          }
        },
        { session }
      );
      // We don't notify checkpoint here - we don't make any checkpoint updates directly
    });
  }

  async [Symbol.asyncDispose]() {
    if (this.batch != null || this.write_checkpoint_batch.length > 0) {
      // We don't error here, since:
      // 1. In error states, this is expected (we can't distinguish between disposing after success or error).
      // 2. SuppressedError is messy to deal with.
      this.logger.warn('Disposing writer with unflushed changes');
    }
    await this.session.endSession();
    super.clearListeners();
  }

  async dispose() {
    await this[Symbol.asyncDispose]();
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    const { after, before, sourceTable, tag } = record;
    const storeCurrentData = this.storeCurrentData && sourceTable.storeCurrentData;
    for (const event of this.getTableEvents(sourceTable)) {
      this.iterateListeners((cb) =>
        cb.replicationEvent?.({
          batch: this,
          table: sourceTable,
          data: {
            op: tag,
            after: after && utils.isCompleteRow(storeCurrentData, after) ? after : undefined,
            before: before && utils.isCompleteRow(storeCurrentData, before) ? before : undefined
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

    this.logger.debug(`Saving ${record.tag}:${record.before?.id}/${record.after?.id}`);

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
        await this.db.commonSourceTables(this.replicationStreamId).deleteOne({ _id: mongoTableId(table.id) });
      }
    });

    await this.cleanupDroppedSourceTables(sourceTables);
    return result;
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.flush();

    let last_op: InternalOpId | null = null;
    for (let table of sourceTables) {
      last_op = await this.truncateSingle(table);
    }

    if (last_op) {
      this.persisted_op = last_op;
      return {
        flushed_op: last_op
      };
    } else {
      return null;
    }
  }

  async truncateSingle(sourceTable: storage.SourceTable): Promise<InternalOpId> {
    let last_op: InternalOpId | null = null;

    // To avoid too large transactions, we limit the amount of data we delete per transaction.
    // Since we don't use the record data here, we don't have explicit size limits per batch.
    const BATCH_LIMIT = 2000;

    let lastBatchCount = BATCH_LIMIT;
    while (lastBatchCount == BATCH_LIMIT) {
      await this.withReplicationTransaction(`Truncate ${sourceTable.qualifiedName}`, async (session, opSeq) => {
        using evalSpan = this.tracer.span('evaluate');
        const sourceTableId = mongoTableId(sourceTable.id);
        const batch = await this.sourceRecordStore.loadTruncateBatch(session, sourceTableId, BATCH_LIMIT);
        const persistedBatch = this.createPersistedBatch(0);

        for (let value of batch) {
          persistedBatch.saveBucketData({
            op_seq: opSeq,
            before_buckets: value.buckets,
            evaluated: [],
            table: sourceTable,
            sourceKey: value.replicaId
          });
          persistedBatch.saveParameterData({
            op_seq: opSeq,
            existing_lookups: value.lookups,
            evaluated: [],
            sourceTable: sourceTable,
            sourceKey: value.replicaId
          });

          // Since this is not from streaming replication, we can do a hard delete
          persistedBatch.hardDeleteCurrentData(sourceTableId, value.replicaId);
        }
        evalSpan.end();

        using _ = this.tracer.span('storage', 'persist_flush');
        await persistedBatch.flush(session);
        lastBatchCount = batch.length;

        last_op = opSeq.last();
      });
    }

    return last_op!;
  }

  async updateTableProgress(
    table: storage.SourceTable,
    progress: Partial<storage.TableSnapshotStatus>
  ): Promise<storage.SourceTable> {
    const copy = table.clone();
    const snapshotStatus = {
      totalEstimatedCount: progress.totalEstimatedCount ?? copy.snapshotStatus?.totalEstimatedCount ?? 0,
      replicatedCount: progress.replicatedCount ?? copy.snapshotStatus?.replicatedCount ?? 0,
      lastKey: progress.lastKey ?? copy.snapshotStatus?.lastKey ?? null
    };
    copy.snapshotStatus = snapshotStatus;

    await this.withTransaction(async () => {
      await this.db.commonSourceTables(this.replicationStreamId).updateOne(
        { _id: mongoTableId(table.id) },
        {
          $set: {
            snapshot_status: {
              last_key: snapshotStatus.lastKey == null ? null : new bson.Binary(snapshotStatus.lastKey),
              total_estimated_count: snapshotStatus.totalEstimatedCount,
              replicated_count: snapshotStatus.replicatedCount
            }
          }
        },
        { session: this.session }
      );
    });

    return copy;
  }

  protected async clearError(): Promise<void> {
    // No need to clear an error more than once per batch, since an error would always result in restarting the batch.
    if (this.clearedError) {
      return;
    }

    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          last_fatal_error: null,
          last_fatal_error_ts: null
        }
      }
    );
    this.clearedError = true;
  }

  /**
   * Gets relevant {@link SqlEventDescriptor}s for the given {@link SourceTable}
   */
  protected getTableEvents(table: storage.SourceTable): SqlEventDescriptor[] {
    return this.sync_rules.eventDescriptors.filter((evt) =>
      [...evt.getSourceTables()].some((sourceTable) => sourceTable.matches(table.ref))
    );
  }
}
