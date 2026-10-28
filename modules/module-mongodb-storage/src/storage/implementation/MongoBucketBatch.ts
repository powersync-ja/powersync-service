import { mongo } from '@powersync/lib-service-mongodb';
import {
  EventDefinitionId,
  HydratedEventDescriptor,
  HydratedSyncConfig,
  SqliteRow,
  SqliteValue
} from '@powersync/service-sync-rules';
import * as bson from 'bson';

import {
  BaseObserver,
  container,
  ErrorCode,
  errors,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  BucketDefinitionMapping,
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
import { PersistedBatch } from './common/PersistedBatch.js';
import { LoadedSourceRecord, SourceRecordStore } from './common/SourceRecordStore.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { SyncRuleDocumentBase } from './models.js';
import { MAX_ROW_SIZE } from './MongoBucketBatchShared.js';
import { MongoIdSequence, OpIdRangeExhausted } from './MongoIdSequence.js';
import { MongoOpIdAllocator } from './MongoOpIdAllocator.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';
import { REPLICATION_WRITE_OPTIONS } from './MongoReplicationOptions.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { MongoWriteBatch } from './MongoWriteBatch.js';
import { OperationBatch, RecordOperation } from './OperationBatch.js';
import { ObjectStorage } from './v3/object-storage/ObjectStorage.js';
import { createObjectStorageUsageWriterId } from './v3/object-storage/ObjectStorageUsage.js';

export interface MongoBucketBatchOptions {
  replicationLock: MongoSyncRulesLock;
  opIdAllocator: MongoOpIdAllocator;
  /**
   * The parsed sync config set for this batch.
   *
   * The batch derives both the hydrated sync rules and the bucket definition mapping from
   * this single set, so they always come from the same parse. Do not add separate
   * syncRules/mapping options - pairing values from different parses is exactly the bug
   * this shape prevents.
   */
  parsedSyncConfig: MongoParsedSyncConfigSet;
  replicationStreamId: number;
  replicationStreamName: string;
  syncConfigIds?: bson.ObjectId[];
  /**
   * Seeds the in-memory persisted-op tracking for v1 storage. Not used by v3 storage, which
   * tracks the persisted-op head durably on the replication stream document instead.
   */
  keepaliveOp?: InternalOpId | null;
  resumeFromLsn: string | null;
  storeCurrentData: boolean;
  /**
   * Set to true for initial replication.
   */
  skipExistingRows: boolean;

  markRecordUnavailable: BucketStorageMarkRecordUnavailable | undefined;
  hooks: storage.StorageHooks | undefined;

  logger: Logger;
  tracer?: PerformanceTracer<'storage' | 'evaluate'>;
  /**
   * Aborts in-flight object storage uploads when replication stops.
   */
  signal?: AbortSignal;

  objectStorage?: ObjectStorage;
  inlineThresholdBytes?: number;
}

export abstract class MongoBucketBatch
  extends BaseObserver<storage.BucketBatchStorageListener>
  implements storage.BucketStorageBatch
{
  protected readonly options: MongoBucketBatchOptions;
  protected logger: Logger;

  readonly db: VersionedPowerSyncMongo;
  /** Shared by this lease's writers; only use inside a serialized public operation. */
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
  protected readonly storeCurrentData: boolean;
  public readonly skipExistingRows: boolean;
  protected readonly mapping: BucketDefinitionMapping;
  protected readonly objectStorageUsageWriterId = createObjectStorageUsageWriterId();

  protected batch: OperationBatch | null = null;
  protected write_checkpoint_batch: storage.CustomWriteCheckpointOptions[] = [];
  protected readonly markRecordUnavailable: BucketStorageMarkRecordUnavailable | undefined;
  protected hooks: storage.StorageHooks | undefined;
  protected clearedError = false;

  protected tracer: PerformanceTracer<'storage' | 'evaluate'>;

  /**
   * Last written op, if any. This may not reflect a consistent checkpoint.
   */
  public last_flushed_op: InternalOpId | null = null;

  /**
   * LSN to resume replication from.
   *
   * This is typically the last commit LSN, but there are cases where it differs:
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
    this.options = {
      ...options,
      signal:
        options.signal == null
          ? options.replicationLock.signal
          : AbortSignal.any([options.signal, options.replicationLock.signal])
    };
    // A MongoDB session can only be used with its originating client. Writers
    // created through another factory must still use this lease's database.
    this.db = options.replicationLock.db;
    this.replicationStreamId = options.replicationStreamId;
    this.resumeFromLsn = options.resumeFromLsn;
    this.session = options.replicationLock.session;
    this.replicationStreamName = options.replicationStreamName;
    this.sync_rules = options.parsedSyncConfig.hydratedSyncConfig;
    this.storeCurrentData = options.storeCurrentData;
    this.mapping = options.parsedSyncConfig.mapping;
    this.skipExistingRows = options.skipExistingRows;
    this.markRecordUnavailable = options.markRecordUnavailable;
    this.hooks = options.hooks;
    this.batch = new OperationBatch();

    this.tracer = options.tracer ?? new PerformanceTracer('MongoDB storage');
  }

  addCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): void {
    this.write_checkpoint_batch.push({
      ...checkpoint,
      sync_rules_id: this.replicationStreamId
    });
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult> {
    return this.withWriterLock(() => this.resolveTablesInternal(options));
  }

  protected abstract resolveTablesInternal(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult>;

  protected abstract createPersistedBatch(writtenSize: number): PersistedBatch;

  protected abstract get sourceRecordStore(): SourceRecordStore;

  protected abstract cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]): Promise<void>;

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    return this.withWriterLock(() => this.commitInternal(lsn, options));
  }

  protected abstract commitInternal(
    lsn: string,
    options?: storage.BucketBatchCommitOptions
  ): Promise<storage.CheckpointResult>;

  async keepalive(lsn: string): Promise<storage.CheckpointResult> {
    return this.commit(lsn, { createEmptyCheckpoints: true });
  }

  async setResumeLsn(lsn: string): Promise<void> {
    return this.withWriterLock(() => this.setResumeLsnInternal(lsn));
  }

  protected abstract setResumeLsnInternal(lsn: string): Promise<void>;

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    return this.withWriterLock(() => this.getSourceTableStatusInternal(table));
  }

  protected abstract getSourceTableStatusInternal(table: storage.SourceTable): Promise<storage.SourceTable | null>;

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    return this.withWriterLock(() => this.markAllSnapshotDoneInternal(no_checkpoint_before_lsn));
  }

  protected abstract markAllSnapshotDoneInternal(no_checkpoint_before_lsn: string): Promise<void>;

  async markSnapshotDone(no_checkpoint_before_lsn: string, options?: { throwOnConflict?: boolean }): Promise<void> {
    return this.withWriterLock(() => this.markSnapshotDoneInternal(no_checkpoint_before_lsn, options));
  }

  protected abstract markSnapshotDoneInternal(
    no_checkpoint_before_lsn: string,
    options?: { throwOnConflict?: boolean }
  ): Promise<void>;

  async markTableSnapshotRequired(table: storage.SourceTable): Promise<void> {
    return this.withWriterLock(() => this.markTableSnapshotRequiredInternal(table));
  }

  protected abstract markTableSnapshotRequiredInternal(table: storage.SourceTable): Promise<void>;

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    return this.withWriterLock(() => this.markTableSnapshotDoneInternal(tables, no_checkpoint_before_lsn));
  }

  protected abstract markTableSnapshotDoneInternal(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]>;

  /** Perform any version-specific setup required before writing custom checkpoints. */
  protected async prepareCustomWriteCheckpoints(): Promise<void> {}

  async flush(options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    return this.withWriterLock(() => this.flushInternal(options));
  }

  /** Internal helpers require the public operation to hold the writer mutex. */
  protected async flushInternal(options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
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

  /** Publish the checkpoint in the final flush transaction, or use the empty-commit path. */
  protected async flushAndCommit<T>(
    checkpoint: (stream: SyncRuleDocumentBase, lastOp: InternalOpId, writes?: MongoWriteBatch) => Promise<T>,
    commitWithoutFlush: () => Promise<T>,
    options?: storage.BatchBucketFlushOptions,
    projection?: mongo.Document
  ): Promise<T> {
    if (this.batch == null && this.write_checkpoint_batch.length === 0) {
      return commitWithoutFlush();
    }
    let result!: T;
    while (this.batch != null || this.write_checkpoint_batch.length > 0) {
      await this.flushInner(
        options,
        async (stream, lastOp, writes) => {
          result = await checkpoint(stream, lastOp, writes);
        },
        projection
      );
    }
    return result;
  }

  protected abstract flushInner(
    options?: storage.BatchBucketFlushOptions,
    checkpoint?: (stream: SyncRuleDocumentBase, lastOp: InternalOpId, writes?: MongoWriteBatch) => Promise<void>,
    projection?: mongo.Document
  ): Promise<storage.FlushedResult | null>;

  /**
   * Evaluate one source operation and append its storage writes to batch.
   *
   * Diagnostics run during evaluation. V1 calls markRecordUnavailable immediately;
   * v3/v4 supply a collector and deliver repair notifications only after a
   * confirmed publication. A failed row attempt may have registered a repair,
   * so its caller must discard that row's collected notification on failure.
   */
  protected saveOperation(
    batch: PersistedBatch,
    operation: RecordOperation,
    sourceRecord: LoadedSourceRecord | null,
    opSeq: MongoIdSequence,
    onRecordUnavailable: BucketStorageMarkRecordUnavailable | undefined
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
      } else {
        existing_buckets = result.buckets;
        existing_lookups = result.lookups;
        if (storeCurrentData && result.data != null) {
          const data = deserializeBson(result.data.buffer) as SqliteRow;
          after = storage.mergeToast<SqliteValue>(after!, data);
        }
      }
      if (!isCompleteRow(storeCurrentData, after!)) {
        if (onRecordUnavailable != null) {
          // This will trigger a "resnapshot" of the record.
          // This is not relevant if storeCurrentData is false, since we'll get the full row
          // directly in the replication stream.
          // Check after merging, including existing incomplete rows/tombstones.
          // A flush can commit before a process dies or notification delivery
          // fails. Its source position has not advanced, so WAL replay must
          // request repair again rather than treating mere record existence as
          // proof that the missing TOAST values have been recovered.
          onRecordUnavailable(record);
        } else if (result == null) {
          // Log to help with debugging if there was a consistency issue
          this.logger.warn(
            `Cannot find previous record for update on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
          );
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
        if (this.storeCurrentData && onRecordUnavailable == null) {
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

  protected async fence(session = this.session, projection?: mongo.Document) {
    // Graceful cancellation belongs to the source connector's page/batch boundary.
    // It must still be able to persist progress for rows already flushed. Lease
    // loss is different: the fence rejects every subsequent writer transaction.
    return MongoSyncRulesLock.fence(
      this.db,
      this.replicationStreamId,
      this.options.replicationLock,
      session,
      projection
    );
  }

  protected async withTransaction<T>(
    cb: (stream: SyncRuleDocumentBase) => Promise<T>,
    session = this.session
  ): Promise<T> {
    return this.withFencedTransaction(() => this.fence(session), cb, session);
  }

  /**
   * Internal transaction helper: the public writer operation already holds the
   * mutex. The first operation must modify the stream document conditional on
   * lease ownership. This database fence is required on every retry even though
   * local writers are serialized; another process may have taken over the lease.
   */
  protected async withFencedTransaction<S, T>(
    acquireFence: () => Promise<S>,
    cb: (stream: S) => Promise<T>,
    session = this.session,
    description = 'Replication transaction'
  ): Promise<T> {
    this.options.replicationLock.throwIfAborted();
    let attempts = 0;
    // Includes driver commit retries and the delays between transaction attempts.
    using _transactionSpan = this.tracer.span('storage', 'transaction');
    const result = await session.withTransaction(
      async () => {
        attempts++;
        // One callback invocation per transaction attempt. Commit happens after
        // this span; command monitoring is needed to measure the full wire lifetime.
        using attemptSpan = this.tracer.span('storage', 'transaction_body');
        try {
          const stream = await acquireFence();
          const result = await cb(stream);
          this.options.replicationLock.throwIfAborted();
          return result;
        } catch (e: unknown) {
          attemptSpan.end();
          if (e instanceof OpIdRangeExhausted) {
            throw e;
          }
          if (e instanceof mongo.MongoError && e.hasErrorLabel('TransientTransactionError')) {
            // Keep the existing every-tenth-attempt cadence, including failures
            // of the fence. Log the actual error rather than assuming every
            // transient error is a write conflict. Omit stacks and bulk results.
            if ((attempts + 1) % 10 === 0) {
              this.logger.info(`${description} - try ${attempts + 1}`, {
                error: e.message,
                code: e.code,
                codeName: e instanceof mongo.MongoServerError ? e.codeName : undefined,
                errorLabels: e.errorLabels
              });
            }
          } else {
            this.logger.warn('Transaction error', e as Error);
          }
          const delay = Math.random() * 50;
          using _ = this.tracer.span('storage', 'retry_delay');
          await timers.setTimeout(delay);
          throw e;
        }
      },
      { maxCommitTimeMS: 10000, ...REPLICATION_WRITE_OPTIONS }
    );
    return result;
  }

  /**
   * Public database operations acquire the lease's mutex once, then call only
   * internal helpers until they finish. Those helpers must never call a public
   * database operation or reacquire this mutex. All session use, including reads,
   * belongs inside this boundary so it cannot join another writer's transaction.
   * Transaction helpers still fence database ownership on every attempt.
   */
  private async withWriterLock<T>(callback: () => Promise<T>): Promise<T> {
    using lockSpan = this.tracer.span('storage', 'internal_lock');
    this.options.replicationLock.throwIfAborted();
    return this.options.replicationLock.writerMutex.exclusiveLock(async () => {
      lockSpan.end();
      this.options.replicationLock.throwIfAborted();
      return callback();
    });
  }

  /** Internal single-document update; the public operation already holds the mutex. */
  protected async updateStreamMetadata(
    set: mongo.Document,
    filter: mongo.Document = {},
    writeConcern: mongo.WriteConcernSettings = REPLICATION_WRITE_OPTIONS.writeConcern
  ): Promise<void> {
    await this.writeStreamMetadata(set, filter, writeConcern);
  }

  /** Caller must already be inside a fenced transaction holding the writer mutex. */
  protected async updateStreamMetadataInTransaction(set: mongo.Document, filter: mongo.Document = {}): Promise<void> {
    await this.writeStreamMetadata(set, filter);
  }

  private async writeStreamMetadata(
    set: mongo.Document,
    filter: mongo.Document,
    writeConcern?: mongo.WriteConcernSettings
  ): Promise<void> {
    const result = await this.db.sync_rules.updateOne(
      { ...filter, ...MongoSyncRulesLock.ownerFilter(this.replicationStreamId, this.options.replicationLock) },
      [{ $set: { ...set, ...MongoSyncRulesLock.heartbeatUpdate() } }],
      { session: this.session, ...(writeConcern == null ? {} : { writeConcern }) }
    );
    if (result.matchedCount === 0) {
      throw new ReplicationAbortedError('Replication writer no longer owns the stream');
    }
  }

  /** Highest durably persisted operation, including operations not yet checkpointed. */
  protected abstract persistedOpHead(stream: SyncRuleDocumentBase): InternalOpId;

  /** Advance the persisted head in the same transaction as the operations. */
  protected abstract onReplicationTransactionFlush(writes: MongoWriteBatch, lastOp: InternalOpId): void;

  async [Symbol.asyncDispose]() {
    if (this.batch?.hasData() || this.write_checkpoint_batch.length > 0) {
      // We don't error here, since:
      // 1. In error states, this is expected (we can't distinguish between disposing after success or error).
      // 2. SuppressedError is messy to deal with.
      this.logger.warn('Disposing writer with unflushed changes');
    }
    // The lease owns the shared session and ends it after all writers stop.
    super.clearListeners();
  }

  async dispose() {
    await this[Symbol.asyncDispose]();
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    const { after, before, sourceTable, tag } = record;
    const storeCurrentData = this.storeCurrentData && sourceTable.storeCurrentData;
    // V3 source tables own disjoint event-definition ids for each physical table. Multiple
    // SourceTables may evaluate different events, but each definition is evaluated through
    // at most one record for this row change.
    // Legacy storage leaves eventDefinitionIds undefined and selects by table ref.
    if (sourceTable.syncEvent) {
      for (const { event, eventId } of this.getTableEvents(sourceTable)) {
        this.iterateListeners((cb) =>
          cb.replicationEvent?.({
            batch: this,
            table: sourceTable,
            data: {
              op: tag,
              after: after && utils.isCompleteRow(storeCurrentData, after) ? after : undefined,
              before: before && utils.isCompleteRow(storeCurrentData, before) ? before : undefined
            },
            event,
            event_id: eventId
          })
        );
      }
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
    return this.withWriterLock(() => this.dropInternal(sourceTables));
  }

  protected async dropInternal(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.truncateInternal(sourceTables);

    await this.withTransaction(async () => {
      for (let table of sourceTables) {
        await this.db
          .commonSourceTables(this.replicationStreamId)
          .deleteOne({ _id: mongoTableId(table.id) }, { session: this.session });
      }
    });

    await this.cleanupDroppedSourceTables(sourceTables);
    return null;
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    return this.withWriterLock(() => this.truncateInternal(sourceTables));
  }

  protected async truncateInternal(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.flushInternal();

    let last_op: InternalOpId | null = null;
    for (let table of sourceTables) {
      last_op = await this.truncateSingleInternal(table);
    }

    if (last_op) {
      return {
        flushed_op: last_op
      };
    } else {
      return null;
    }
  }

  async truncateSingle(sourceTable: storage.SourceTable): Promise<InternalOpId> {
    return this.withWriterLock(() => this.truncateSingleInternal(sourceTable));
  }

  protected abstract truncateSingleInternal(sourceTable: storage.SourceTable): Promise<InternalOpId>;

  async updateTableProgress(
    table: storage.SourceTable,
    progress: Partial<storage.TableSnapshotStatus>
  ): Promise<storage.SourceTable> {
    return this.withWriterLock(() => this.updateTableProgressInternal(table, progress));
  }

  private async updateTableProgressInternal(
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

  /**
   * Gets relevant {@link HydratedEventDescriptor}s for the given {@link SourceTable}
   */
  protected getTableEvents(
    table: storage.SourceTable
  ): { event: HydratedEventDescriptor; eventId?: EventDefinitionId }[] {
    // V3 storage assigns event-definition ids to each source table, so membership is authoritative.
    // Iterate the table's distinct ids and resolve each through the stream's deduped event map, so a
    // definition reused across configs has one evaluator for that id. The evaluator may still return
    // multiple payloads from matching queries. Legacy storage leaves this undefined and selects by table ref.
    if (table.eventDefinitionIds != null) {
      const eventById = this.options.parsedSyncConfig.eventById;
      const events: { event: HydratedEventDescriptor; eventId: EventDefinitionId }[] = [];
      for (const id of table.eventDefinitionIds) {
        const event = eventById.get(id);
        if (event != null && event.tableTriggersEvent(table.ref)) {
          events.push({ event, eventId: id });
        }
      }
      return events;
    }

    return this.sync_rules.eventDescriptors
      .filter((event) => event.tableTriggersEvent(table.ref))
      .map((event) => ({ event }));
  }
}
