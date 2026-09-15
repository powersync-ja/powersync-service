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
import { PreparedPublication } from './common/PreparedPublication.js';
import { LoadedSourceRecord, SourceRecordStore } from './common/SourceRecordStore.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { SyncRuleDocumentBase } from './models.js';
import { MAX_ROW_SIZE } from './MongoBucketBatchShared.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { MongoOpIdAllocator } from './MongoOpIdAllocator.js';
import { MongoParsedSyncConfigSet } from './MongoParsedSyncConfigSet.js';
import { MongoReplicationApplication } from './MongoReplicationApplication.js';
import { MongoPublicationWriter, PublicationOptions } from './MongoReplicationPipeline.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { MongoWriteBatch } from './MongoWriteBatch.js';
import { OperationBatch, RecordOperation } from './OperationBatch.js';
import { ObjectStorage } from './v3/object-storage/ObjectStorage.js';
import { createObjectStorageUsageWriterId } from './v3/object-storage/ObjectStorageUsage.js';

export interface MongoBucketBatchOptions {
  replicationLock: MongoSyncRulesLock;
  opIdAllocator: MongoOpIdAllocator;
  db: VersionedPowerSyncMongo;
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
  protected readonly objectStorageUsageWriterId = createObjectStorageUsageWriterId();

  private batch: OperationBatch | null = null;
  private pipeline?: MongoPublicationWriter;
  private application?: MongoReplicationApplication;

  protected get eagerPublication(): boolean {
    return true;
  }

  protected get uploadSignal(): AbortSignal | undefined {
    return this.pipeline?.uploadSignal ?? this.options.signal;
  }

  protected write_checkpoint_batch: storage.CustomWriteCheckpointOptions[] = [];
  private markRecordUnavailable: BucketStorageMarkRecordUnavailable | undefined;
  private hooks: storage.StorageHooks | undefined;
  private clearedError = false;

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
    this.client = options.db.client;
    this.db = options.db;
    this.replicationStreamId = options.replicationStreamId;
    this.resumeFromLsn = options.resumeFromLsn;
    this.session = this.client.startSession();
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

  protected abstract batchCreateCustomWriteCheckpoints(session: mongo.ClientSession, opId: InternalOpId): Promise<void>;

  /** Perform any version-specific setup required before writing custom checkpoints. */
  protected async prepareCustomWriteCheckpoints(): Promise<void> {}

  async flush(options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    await this.publishBoundary({ flushOptions: options });
    return this.last_flushed_op == null ? null : { flushed_op: this.last_flushed_op };
  }

  /** Publish the checkpoint in the final group, or use the version-specific empty commit. */
  protected async flushAndCommit<T>(
    checkpoint: (stream: SyncRuleDocumentBase, lastOp: InternalOpId) => Promise<T>,
    commitWithoutFlush: () => Promise<T>,
    options?: storage.BatchBucketFlushOptions
  ): Promise<T> {
    const result = await this.publishBoundary({ checkpoint, flushOptions: options });
    return result.published ? result.value : commitWithoutFlush();
  }

  /** Shared durable boundary for rows, custom checkpoints and a final client checkpoint. */
  private async publishBoundary<T = void>(
    options: PublicationOptions<T>
  ): Promise<{ published: false } | { published: true; value: T }> {
    await this.enqueuePipelineBatch();
    const beforePublish = await this.prepareCheckpointPublication();
    const receipt = await this.publicationWriter().seal({ ...options, beforePublish }, beforePublish != null);
    const result = receipt.published
      ? { published: true as const, value: await receipt.persisted }
      : await receipt.persisted.then(() => ({ published: false as const }));
    this.write_checkpoint_batch = [];
    return result;
  }

  async queueResumeLsn(lsn: string, options?: storage.BatchBucketFlushOptions): Promise<storage.BatchProgressReceipt> {
    if (this.eagerPublication || this.write_checkpoint_batch.length > 0) {
      await this.flush(options);
      await this.setResumeLsn(lsn);
      return { persisted: Promise.resolve() };
    }
    await this.enqueuePipelineBatch();
    return this.publicationWriter().seal({ resumeLsn: lsn, flushOptions: options }, true);
  }

  /** Custom checkpoint writes use the same immutable publication boundary as rows. */
  private async prepareCheckpointPublication(): Promise<PublicationOptions['beforePublish']> {
    if (this.write_checkpoint_batch.length === 0) {
      return undefined;
    }
    // Collection/index creation must happen outside publication transactions.
    await this.prepareCustomWriteCheckpoints();
    let opId = 0n;
    await this.publicationWriter().prepare(async (application) => {
      opId = await application.applyRow((_row, sequence) => sequence.next());
    });
    return (session) => this.batchCreateCustomWriteCheckpoints(session, opId);
  }

  private async publish<T>(
    publication: PreparedPublication,
    expectedHead: bigint,
    lastOp: bigint,
    options?: PublicationOptions<T>
  ): Promise<T | undefined> {
    let flushedAny = false;
    const result = await this.runFencedTransaction(
      () => this.fence(this.session, { sync_configs: 1, last_persisted_op: 1, last_checkpoint: 1, keepalive_op: 1 }),
      async (stream) => {
        this.pipeline!.check();
        if (this.persistedOpHead(stream) !== expectedHead) {
          // Another process using this lease must not invalidate prepared membership.
          throw new ReplicationAbortedError('Replication stream advanced during publication preparation');
        }
        const stats = await publication.publish(this.session, options?.flushOptions);
        flushedAny = stats.flushedAny;
        if (flushedAny && !this.clearedError) {
          await this.clearError(this.session);
        }
        await options?.beforePublish?.(this.session);
        let result: T | undefined;
        if (options?.checkpoint != null) {
          result = await options.checkpoint(stream, lastOp);
        } else {
          const writes = this.db.createWriteBatch(this.session, { ordered: false });
          this.onReplicationTransactionFlush(writes, lastOp);
          if (options?.resumeLsn != null) {
            writes.updateOne(
              this.db.sync_rules,
              { _id: this.replicationStreamId },
              { $max: { resume_lsn: options.resumeLsn } }
            );
          }
          await writes.execute();
        }
        this.pipeline!.check();
        return result;
      }
    );
    this.clearedError ||= flushedAny;
    this.recordPersistedOp(lastOp);
    this.last_flushed_op = lastOp;
    await this.hooks?.afterBatchFlush?.(this);
    return result;
  }

  /** Registration does not read or apply source input. */
  private publicationWriter(): MongoPublicationWriter {
    if (this.pipeline == null) {
      this.pipeline = this.options.opIdAllocator.publicationPipeline(this.options.replicationLock.signal).register({
        sourceSignal: this.options.signal,
        allocator: this.options.opIdAllocator,
        session: this.session,
        readHead: async (session) => {
          const stream = await this.db.sync_rules.findOne(
            MongoSyncRulesLock.ownerFilter(this.replicationStreamId, this.options.replicationLock),
            { session, readConcern: { level: 'majority' } }
          );
          return this.persistedOpHead(MongoSyncRulesLock.assertOwned(stream));
        },
        createBatch: () => this.createPersistedBatch(0),
        publish: (publication, expectedHead, lastOp, options) =>
          this.publish(publication, expectedHead, lastOp, options)
      });
      this.application = new MongoReplicationApplication(
        this.pipeline,
        this.sourceRecordStore,
        (batch, operation, before, sequence) => this.saveOperation(batch, operation, before, sequence),
        this.storeCurrentData,
        this.skipExistingRows,
        this.eagerPublication
      );
    }
    return this.pipeline;
  }

  private async enqueuePipelineBatch(): Promise<void> {
    this.publicationWriter().check();
    const input = this.batch;
    this.batch = null;
    if (input == null || !input.hasData()) {
      return;
    }
    await this.application!.apply(input, async () => {
      await this.hooks?.beforeBatchFlush?.(this);
    });
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

  protected async withTransaction<T>(cb: (stream: SyncRuleDocumentBase) => Promise<T>): Promise<T> {
    await this.flush();
    return this.withFencedTransaction(() => this.fence(), cb);
  }

  /** The first operation must modify the stream document conditional on lease ownership. */
  protected async withFencedTransaction<S, T>(
    acquireFence: () => Promise<S>,
    cb: (stream: S) => Promise<T>
  ): Promise<T> {
    return this.withWriterAccess(() => this.runFencedTransaction(acquireFence, cb));
  }

  /** Publications already occupy their place in the FIFO and need no metadata barrier. */
  private async runFencedTransaction<S, T>(acquireFence: () => Promise<S>, cb: (stream: S) => Promise<T>): Promise<T> {
    return this.session.withTransaction(
      async () => {
        const stream = await acquireFence();
        try {
          const result = await cb(stream);
          this.options.replicationLock.throwIfAborted();
          return result;
        } catch (e: unknown) {
          if (e instanceof mongo.MongoError && e.hasErrorLabel('TransientTransactionError')) {
            // Likely write conflict caused by concurrent writes to this replication stream.
          } else {
            this.logger.warn('Transaction error', e as Error);
          }
          const delay = Math.random() * 50;
          using _ = this.tracer.span('storage', 'retry_delay');
          await timers.setTimeout(delay);
          throw e;
        }
      },
      { maxCommitTimeMS: 10000, writeConcern: { w: 'majority' } }
    );
  }

  /** Seal and publish the shared prefix before changing stream metadata. */
  protected async withWriterAccess<T>(callback: () => Promise<T>): Promise<T> {
    // Register lazily, including metadata-only writers. Their barriers share the
    // same ordered prefix as snapshot and streaming publications.
    await this.enqueuePipelineBatch();
    return this.pipeline!.exclusive(callback);
  }

  /** Single-document updates enforce ownership atomically, without a separate transaction. */
  protected async updateStreamMetadata(
    set: mongo.Document,
    filter: mongo.Document = {},
    writeConcern: mongo.WriteConcernSettings = { w: 'majority' }
  ): Promise<void> {
    if (this.session.inTransaction()) {
      return this.writeStreamMetadata(set, filter, writeConcern);
    }
    await this.flush();
    return this.withWriterAccess(() => this.writeStreamMetadata(set, filter, writeConcern));
  }

  private async writeStreamMetadata(
    set: mongo.Document,
    filter: mongo.Document,
    writeConcern: mongo.WriteConcernSettings
  ): Promise<void> {
    const result = await this.db.sync_rules.updateOne(
      { ...filter, ...MongoSyncRulesLock.ownerFilter(this.replicationStreamId, this.options.replicationLock) },
      [{ $set: { ...set, ...MongoSyncRulesLock.heartbeatUpdate() } }],
      {
        session: this.session,
        ...(this.session.inTransaction() ? {} : { writeConcern })
      }
    );
    if (result.matchedCount === 0) {
      throw new ReplicationAbortedError('Replication writer no longer owns the stream');
    }
  }

  /** Highest durably persisted operation, including operations not yet checkpointed. */
  protected abstract persistedOpHead(stream: SyncRuleDocumentBase): InternalOpId;

  /** Advance the persisted head in the same transaction as the operations. */
  protected abstract onReplicationTransactionFlush(writes: MongoWriteBatch, lastOp: InternalOpId): void;

  /**
   * Called after a replication transaction has successfully committed, with the last persisted op id.
   *
   * v1 storage tracks this in memory to fold into the next checkpoint. v3 storage does not need it:
   * the stream-level `last_persisted_op` is already advanced durably by
   * {@link onReplicationTransactionFlush} or the combined checkpoint update within the same
   * transaction, and empty commits read it from the document.
   *
   * Called by the shared publisher after commit, including truncation and custom checkpoints.
   */
  protected recordPersistedOp(_lastOp: InternalOpId): void {
    // No-op by default.
  }

  async [Symbol.asyncDispose]() {
    if (this.batch?.hasData() || this.write_checkpoint_batch.length > 0) {
      // We don't error here, since:
      // 1. In error states, this is expected (we can't distinguish between disposing after success or error).
      // 2. SuppressedError is messy to deal with.
      this.logger.warn('Disposing writer with unflushed changes');
    }
    await this.pipeline?.[Symbol.asyncDispose]();
    await this.session.endSession();
    super.clearListeners();
  }

  async dispose() {
    await this[Symbol.asyncDispose]();
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    this.pipeline?.check();
    this.options.replicationLock.throwIfAborted();
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
      if (!this.eagerPublication && this.write_checkpoint_batch.length === 0) {
        await this.enqueuePipelineBatch();
        return null;
      }
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
        await this.db
          .commonSourceTables(this.replicationStreamId)
          .deleteOne({ _id: mongoTableId(table.id) }, { session: this.session });
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
      this.recordPersistedOp(last_op);
      return {
        flushed_op: last_op
      };
    } else {
      return null;
    }
  }

  async truncateSingle(sourceTable: storage.SourceTable): Promise<InternalOpId> {
    await this.flush();
    const sourceTableId = mongoTableId(sourceTable.id);
    const limit = 2000;
    let lastOp = this.last_flushed_op ?? 0n;
    for (;;) {
      let count = 0;
      await this.publicationWriter().scan(async (application) => {
        // A table scan cannot discover rows that exist only in another writer's
        // pending group. The scan scope publishes that prefix before reading.
        const records = await this.sourceRecordStore.loadTruncateBatch(application.session, sourceTableId, limit);
        count = records.length;
        lastOp = application.lastOp;
        for (const record of records) {
          await application.applyRow((row, sequence) => {
            row.saveBucketData({
              op_seq: sequence,
              before_buckets: record.buckets,
              evaluated: [],
              table: sourceTable,
              sourceKey: record.replicaId
            });
            row.saveParameterData({
              op_seq: sequence,
              existing_lookups: record.lookups,
              evaluated: [],
              sourceTable,
              sourceKey: record.replicaId
            });
            // Truncation is outside streaming replication, so hard deletes are safe.
            row.hardDeleteCurrentData(sourceTableId, record.replicaId);
          });
          lastOp = application.lastOp;
          application.recordMembership(record.cacheKey, null);
          await application.publishIfFull();
        }
      });
      const receipt = await this.publicationWriter().seal();
      // The next page must observe the hard deletes from this page.
      await receipt.persisted;
      if (count < limit) {
        return lastOp;
      }
    }
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

  protected async clearError(session: mongo.ClientSession): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          last_fatal_error: null,
          last_fatal_error_ts: null
        }
      },
      {
        session
      }
    );
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
