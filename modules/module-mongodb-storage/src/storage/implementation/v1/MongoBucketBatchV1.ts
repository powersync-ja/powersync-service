import { mongo } from '@powersync/lib-service-mongodb';
import { ErrorCode, ReplicationAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, InternalOpId, SourceTable, storage } from '@powersync/service-core';
import { HydratedSyncConfig } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { calculateCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoIdSequence, OpIdRangeExhausted } from '../MongoIdSequence.js';
import { MongoSyncRulesLock } from '../MongoSyncRulesLock.js';
import { MongoWriteBatch } from '../MongoWriteBatch.js';
import { OperationBatch } from '../OperationBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { SyncRuleDocumentBase } from '../models.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import { SourceRecordStoreV1 } from './SourceRecordStoreV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';
import { SourceTableDocumentV1, SyncRuleDocumentV1 } from './models.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  declare readonly db: VersionedPowerSyncMongoV1;

  private readonly store: SourceRecordStore;
  private needsActivation = true;
  private lastWaitingLogThrottled = 0;

  /**
   * The last persisted op that is not yet covered by a checkpoint, if any.
   *
   * Seeded from the persisted keepalive_op, so that ops persisted before a restart can still be
   * included in the next checkpoint.
   */
  private persisted_op: InternalOpId | null = null;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.persisted_op = options.keepaliveOp ?? null;
    this.store = new SourceRecordStoreV1(this.db, this.replicationStreamId);
  }

  private async withReplicationTransaction(
    description: string,
    callback: (session: mongo.ClientSession, opSeq: MongoIdSequence) => Promise<void>,
    finish?: (stream: SyncRuleDocumentBase, lastOp: InternalOpId) => Promise<boolean>,
    projection?: mongo.Document
  ): Promise<void> {
    let flushTry = 0;
    const lastTry = Date.now() + 90000;
    const allocator = this.options.opIdAllocator;
    let lastOp = 0n;
    // Refill outside the publication transaction, before evaluating any rows.
    // Exhaustion remains a fallback for large batches or a newer stream head.
    this.options.replicationLock.throwIfAborted();
    await allocator.ensureCapacity();
    while (true) {
      try {
        await this.withFencedTransaction(
          () => this.fence(this.session, projection),
          async (stream) => {
            flushTry += 1;
            if (flushTry > 20 && Date.now() > lastTry) {
              throw new ServiceError(ErrorCode.PSYNC_S1402, 'Max transaction tries exceeded');
            }
            // The fence already holds this stream document for the transaction.
            // A different writer may have used higher IDs since our last flush.
            const opSeq = allocator.sequence(this.persistedOpHead(stream));
            await callback(this.session, opSeq);
            lastOp = opSeq.last();

            if (!(await finish?.(stream, lastOp))) {
              const writes = this.db.createWriteBatch(this.session, { ordered: false });
              this.onReplicationTransactionFlush(writes, lastOp);
              await writes.execute();
            }
          },
          this.session,
          description
        );
        allocator.consume(lastOp);
        return;
      } catch (error) {
        if (!(error instanceof OpIdRangeExhausted)) {
          throw error;
        }
        // The transaction aborted every write. Reserve outside it and replay
        // evaluation from the same committed stream head.
        this.options.replicationLock.throwIfAborted();
        await allocator.reserve();
      }
    }
  }

  private async clearError(session: mongo.ClientSession): Promise<void> {
    await this.db.sync_rules.updateOne(
      { _id: this.replicationStreamId },
      { $set: { last_fatal_error: null, last_fatal_error_ts: null } },
      { session }
    );
  }

  protected override async flushInner(
    options?: storage.BatchBucketFlushOptions,
    checkpoint?: (stream: SyncRuleDocumentBase, lastOp: InternalOpId, writes?: MongoWriteBatch) => Promise<void>,
    projection?: mongo.Document
  ): Promise<storage.FlushedResult | null> {
    const batch = this.batch;
    let last_op: InternalOpId | null = null;
    let resumeBatch: OperationBatch | null = null;
    let clearedError = false;

    using _ = this.tracer.span('storage', 'flush');

    await this.hooks?.beforeBatchFlush?.(this);
    if (this.write_checkpoint_batch.length > 0) {
      // Collection/index creation cannot run inside the replication transaction.
      await this.prepareCustomWriteCheckpoints();
    }

    await this.withReplicationTransaction(
      `Flushing ${batch?.length ?? 0} ops`,
      async (session, opSeq) => {
        clearedError = false;
        if (batch != null) {
          const result = await this.replicateBatch(session, batch, opSeq, options);
          resumeBatch = result.resumeBatch;
          clearedError ||= result.clearedError;
        }

        if (this.write_checkpoint_batch.length > 0) {
          this.logger.info(`Writing ${this.write_checkpoint_batch.length} custom write checkpoints`);
          await this.batchCreateCustomWriteCheckpoints(session, opSeq.next());
        }

        last_op = opSeq.last();
      },
      async (stream, lastOp) => {
        if (checkpoint != null && resumeBatch == null) {
          // The checkpoint must also persist the head, including when visibility is blocked.
          await checkpoint(stream, lastOp);
          return true;
        }
        return false;
      },
      projection
    );

    // Keep checkpoints available if the transaction retries after writing them.
    this.write_checkpoint_batch = [];

    if (clearedError) {
      this.clearedError = true;
    }

    // null if done, set if we need another flush
    this.batch = resumeBatch;

    if (last_op == null) {
      throw new ReplicationAssertionError('Unexpected last_op == null');
    }

    this.recordPersistedOp(last_op);
    this.last_flushed_op = last_op;
    await this.hooks?.afterBatchFlush?.(this);
    return { flushed_op: last_op };
  }

  private async replicateBatch(
    session: mongo.ClientSession,
    batch: OperationBatch,
    op_seq: MongoIdSequence,
    options?: storage.BucketBatchCommitOptions
  ): Promise<{ resumeBatch: OperationBatch | null; clearedError: boolean }> {
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

      let persistedBatch: PersistedBatchV1 | null = this.createPersistedBatch(transactionSize);

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
        const nextData = this.saveOperation(persistedBatch!, op, sourceRecord, op_seq, this.markRecordUnavailable);
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

    const clearedError = didFlush && !this.clearedError;
    if (clearedError) {
      // No need to clear an error more than once per batch, since an error would always result in restarting the batch.
      using _ = this.tracer.span('storage', 'clear_error');
      await this.clearError(this.session);
    }

    return {
      resumeBatch: resumeBatch?.hasData() ? resumeBatch : null,
      clearedError
    };
  }

  protected override async truncateSingleInternal(sourceTable: storage.SourceTable): Promise<InternalOpId> {
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

    if (last_op) {
      this.recordPersistedOp(last_op);
    }

    return last_op!;
  }

  protected override persistedOpHead(stream: SyncRuleDocumentBase): InternalOpId {
    const legacy = stream as SyncRuleDocumentV1;
    const checkpoint = legacy.last_checkpoint ?? 0n;
    const keepalive = legacy.keepalive_op == null ? 0n : BigInt(legacy.keepalive_op);
    return checkpoint > keepalive ? checkpoint : keepalive;
  }

  protected override onReplicationTransactionFlush(writes: MongoWriteBatch, lastOp: InternalOpId): void {
    // Keep flushed operations recoverable before a checkpoint can be published.
    // keepalive_op remains a decimal string for compatibility with legacy readers.
    writes.updateOne(this.db.sync_rules, { _id: this.replicationStreamId }, [
      {
        $set: {
          keepalive_op: {
            $toString: { $max: [{ $toLong: '$keepalive_op' }, '$last_checkpoint', lastOp] }
          }
        }
      }
    ]);
  }

  private recordPersistedOp(lastOp: InternalOpId): void {
    this.persisted_op = lastOp;
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatchV1 {
    return new PersistedBatchV1(this.db, this.replicationStreamId, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }
  protected async cleanupDroppedSourceTables(_tables: SourceTable[]) {
    // No-op for V1: source records live in a shared collection.
  }

  private async batchCreateCustomWriteCheckpoints(session: mongo.ClientSession, opId: InternalOpId): Promise<void> {
    // Only the final checkpoint per user/stream is visible after this transaction.
    const checkpoints = new Map(
      this.write_checkpoint_batch.map((checkpoint) => [
        JSON.stringify([checkpoint.sync_rules_id, checkpoint.user_id]),
        checkpoint
      ])
    );
    await this.db.custom_write_checkpoints.bulkWrite(
      [...checkpoints.values()].map((checkpointOptions) => {
        const set: Record<string, unknown> = {
          checkpoint: checkpointOptions.checkpoint,
          sync_rules_id: checkpointOptions.sync_rules_id,
          op_id: opId
        };
        if (checkpointOptions.checkpoint_requested_at != null) {
          set.checkpoint_requested_at = checkpointOptions.checkpoint_requested_at;
        }

        return {
          updateOne: {
            filter: { user_id: checkpointOptions.user_id, sync_rules_id: checkpointOptions.sync_rules_id },
            update: {
              $set: set,
              ...(checkpointOptions.checkpoint_requested_at == null ? { $unset: { checkpoint_requested_at: 1 } } : {})
            },
            upsert: true
          }
        };
      }),
      { session, ordered: false }
    );
  }

  protected override async resolveTablesInternal(
    options: storage.ResolveTablesOptions
  ): Promise<storage.ResolveTablesResult> {
    const syncRules = options.parsedSyncConfig?.hydratedSyncConfig ?? this.sync_rules;
    const reconcile = options.reconcileSourceTables ?? storage.defaultSourceTableReconciler;
    const { connection_id, source } = options;
    const { schema, name, objectId, replicaIdColumns, connectionTag, sendsCompleteRows } = source;

    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));

    let result: storage.ResolveTablesResult | null = null;
    const session = this.session;
    await this.withTransaction(async () => {
      const col = this.db.sourceTablesV1(this.replicationStreamId);

      // Find records that overlap by name or relation id.
      const orClauses: Record<string, unknown>[] = [{ schema_name: schema, table_name: name }];
      if (objectId != null) {
        orClauses.push({ relation_id: objectId });
      }
      const candidateDocs = await col
        .find({ group_id: this.replicationStreamId, connection_id, $or: orClauses }, { session })
        .toArray();

      const candidateTables = candidateDocs.map((doc) =>
        this.hydrateSourceTable(doc, connectionTag, syncRules, sendsCompleteRows)
      );
      const candidates = candidateTables.map((table) => table.clone());
      const resolution = await reconcile({ source, candidates });
      storage.validateSourceTableCandidateResolution(candidates, resolution);

      for (const { id, sourceMetadata } of storage.diffSourceTableUpdates(candidateTables, resolution)) {
        await col.updateOne({ _id: mongoTableId(id) }, { $set: { source_metadata: sourceMetadata } }, { session });
      }

      const materializedResolution = storage.materializeSourceTableResolution(candidateTables, resolution);
      const compatibleTables = materializedResolution.compatibleTables;

      if (!syncRules.tableSyncsAny(source)) {
        // Still reconcile old mappings so renames out of scope remove previously replicated data.
        result = {
          tables: [],
          dropTables: [...materializedResolution.incompatibleTables, ...compatibleTables]
        };
        return;
      }

      // V1 keeps one record, preferring one that has already been snapshotted.
      const reuse = compatibleTables.find((table) => table.snapshotComplete) ?? compatibleTables[0] ?? null;

      let sourceTable: storage.SourceTable;
      if (reuse != null) {
        sourceTable = reuse;
      } else {
        const doc: SourceTableDocumentV1 = {
          _id: options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId(),
          group_id: this.replicationStreamId,
          connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: null,
          replica_id_columns2: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined,
          source_metadata: resolution.newTableValues.sourceMetadata
        };
        await col.insertOne(doc, { session });
        sourceTable = this.hydrateSourceTable(doc, connectionTag, syncRules, sendsCompleteRows);
      }

      const dropTables = [
        ...materializedResolution.incompatibleTables,
        // V1 only keeps one SourceTable per physical table.
        ...compatibleTables.filter((table) => !storage.sourceTableIdEquals(table.id, sourceTable.id))
      ];

      result = { tables: [sourceTable], dropTables };
    });

    return result!;
  }

  /**
   * Build a SourceTable from its persisted v1 record.
   */
  private hydrateSourceTable(
    doc: SourceTableDocumentV1,
    connectionTag: string,
    syncRules: HydratedSyncConfig,
    sendsCompleteRows: boolean | undefined
  ): storage.SourceTable {
    const ref = { connectionTag, schema: doc.schema_name, name: doc.table_name };
    const table = new storage.SourceTable({
      id: doc._id,
      ref,
      objectId: doc.relation_id,
      replicaIdColumns:
        doc.replica_id_columns2?.map(
          (column) => ({ name: column.name, typeId: column.type_oid, type: column.type }) satisfies ColumnDescriptor
        ) ?? [],
      snapshotComplete: doc.snapshot_done ?? true,
      sourceMetadata: doc.source_metadata ?? null,
      ...syncRules.getMatchingSources(ref)
    });
    table.syncEvent = syncRules.tableTriggersEvent(ref);
    table.syncData = table.bucketDataSources.length > 0;
    table.syncParameters = table.parameterLookupSources.length > 0;
    table.storeCurrentData = sendsCompleteRows !== true;
    table.snapshotStatus =
      doc.snapshot_status == null
        ? undefined
        : {
            lastKey: doc.snapshot_status.last_key?.buffer ?? null,
            totalEstimatedCount: doc.snapshot_status.total_estimated_count,
            replicatedCount: doc.snapshot_status.replicated_count
          };
    return table;
  }

  protected override async getSourceTableStatusInternal(
    table: storage.SourceTable
  ): Promise<storage.SourceTable | null> {
    const doc = await this.db.sourceTablesV1(this.replicationStreamId).findOne(
      {
        group_id: this.replicationStreamId,
        _id: mongoTableId(table.id)
      },
      { session: this.session }
    );
    if (doc == null) {
      return null;
    }

    // Use the default storeCurrentData value when reading outside resolution.
    return this.hydrateSourceTable(doc, table.ref.connectionTag, this.sync_rules, undefined);
  }

  protected override async commitInternal(
    lsn: string,
    options?: storage.BucketBatchCommitOptions
  ): Promise<storage.CheckpointResult> {
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    using _ = this.tracer.span('storage', 'commit');

    const updateCheckpoint = async (persistedOp = this.persisted_op) => {
      const now = new Date();

      const can_checkpoint = {
        $and: [
          { $eq: ['$snapshot_done', true] },
          {
            $or: [{ $eq: ['$last_checkpoint_lsn', null] }, { $lte: ['$last_checkpoint_lsn', { $literal: lsn }] }]
          },
          {
            $or: [{ $eq: ['$no_checkpoint_before', null] }, { $lte: ['$no_checkpoint_before', { $literal: lsn }] }]
          }
        ]
      };

      const new_keepalive_op = {
        $cond: [
          can_checkpoint,
          { $literal: null },
          {
            $toString: {
              $max: [{ $toLong: '$keepalive_op' }, { $literal: persistedOp }, 0n]
            }
          }
        ]
      };

      const new_last_checkpoint = {
        $cond: [
          can_checkpoint,
          {
            $max: ['$last_checkpoint', { $literal: persistedOp }, { $toLong: '$keepalive_op' }, 0n]
          },
          '$last_checkpoint'
        ]
      };

      const preUpdateDocument = (await this.db.sync_rules.findOneAndUpdate(
        MongoSyncRulesLock.ownerFilter(this.replicationStreamId, this.options.replicationLock),
        [
          {
            $set: {
              _can_checkpoint: can_checkpoint,
              _not_empty: createEmptyCheckpoints
                ? true
                : {
                    $or: [
                      { $literal: createEmptyCheckpoints },
                      { $ne: ['$keepalive_op', new_keepalive_op] },
                      { $ne: ['$last_checkpoint', new_last_checkpoint] }
                    ]
                  }
            }
          },
          {
            $set: {
              ...MongoSyncRulesLock.heartbeatUpdate(),
              last_checkpoint_lsn: {
                $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: lsn }, '$last_checkpoint_lsn']
              },
              last_checkpoint_ts: {
                $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: now }, '$last_checkpoint_ts']
              },

              last_fatal_error: { $literal: null },
              last_fatal_error_ts: { $literal: null },
              keepalive_op: new_keepalive_op,
              last_checkpoint: new_last_checkpoint,
              snapshot_lsn: {
                $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: null }, '$snapshot_lsn']
              }
            }
          },
          {
            $unset: ['_can_checkpoint', '_not_empty']
          }
        ],
        {
          session: this.session,
          returnDocument: 'before',
          projection: {
            snapshot_done: 1,
            last_checkpoint_lsn: 1,
            no_checkpoint_before: 1,
            keepalive_op: 1,
            last_checkpoint: 1
          }
        }
      )) as SyncRuleDocumentV1;

      return MongoSyncRulesLock.assertOwned(preUpdateDocument);
    };

    const finishCheckpoint = async (preUpdateDocument: SyncRuleDocumentV1, lastOp?: InternalOpId) => {
      await this.db.write_checkpoints.updateMany(
        {
          processed_at_lsn: null,
          'lsns.1': { $lte: lsn }
        },
        {
          $set: {
            processed_at_lsn: lsn
          }
        },
        {
          session: this.session
        }
      );

      const checkpointState = calculateCheckpointState({
        lsn,
        snapshotDone: preUpdateDocument.snapshot_done === true,
        lastCheckpointLsn: preUpdateDocument.last_checkpoint_lsn,
        noCheckpointBefore: preUpdateDocument.no_checkpoint_before,
        keepaliveOp: preUpdateDocument.keepalive_op == null ? null : BigInt(preUpdateDocument.keepalive_op),
        lastCheckpoint: preUpdateDocument.last_checkpoint,
        persistedOp: lastOp ?? this.persistedOpHead(preUpdateDocument),
        createEmptyCheckpoints
      });
      return { checkpointState, preUpdateDocument };
    };

    const { checkpointState, preUpdateDocument } = await this.flushAndCommit(
      async (_stream, lastOp) => finishCheckpoint(await updateCheckpoint(lastOp), lastOp),
      // Without a flush, the checkpoint update itself fences the transaction.
      () => this.withFencedTransaction(updateCheckpoint, finishCheckpoint),
      options
    );
    if (checkpointState.checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThrottled > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn} / ${checkpointState.newKeepaliveOp}. Current state: ${JSON.stringify(
            {
              snapshot_done: preUpdateDocument.snapshot_done,
              last_checkpoint_lsn: preUpdateDocument.last_checkpoint_lsn,
              no_checkpoint_before: preUpdateDocument.no_checkpoint_before
            }
          )}`
        );
        this.lastWaitingLogThrottled = Date.now();
      }
    } else {
      if (checkpointState.checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn} / ${checkpointState.newLastCheckpoint}`);
      }
      await this.autoActivate(lsn);
      await this.db.notifyCheckpoint();
      this.persisted_op = null;
    }

    return {
      checkpointBlocked: checkpointState.checkpointBlocked,
      checkpointCreated: checkpointState.checkpointCreated
    };
  }

  protected override async setResumeLsnInternal(lsn: string): Promise<void> {
    using _ = this.tracer.span('storage', 'set_resume_lsn');
    // Losing occasional resume LSN would only reprocess source changes.
    // Keep the lease check atomic, but do not wait for majority replication of this resume hint.
    await this.updateStreamMetadata({ snapshot_lsn: { $literal: lsn } }, {}, { w: 1 });
  }

  protected override async markAllSnapshotDoneInternal(no_checkpoint_before_lsn: string): Promise<void> {
    await this.updateStreamMetadata(this.snapshotDoneUpdate(no_checkpoint_before_lsn));
  }

  private snapshotDoneUpdate(no_checkpoint_before_lsn: string) {
    return {
      snapshot_done: true,
      no_checkpoint_before: { $max: ['$no_checkpoint_before', { $literal: no_checkpoint_before_lsn }] }
    };
  }

  protected override async markSnapshotDoneInternal(
    no_checkpoint_before_lsn: string,
    options?: { throwOnConflict?: boolean }
  ): Promise<void> {
    await this.withTransaction(async () => {
      // Protect against race conditions
      const blockingTables = await this.db
        .sourceTablesV1(this.replicationStreamId)
        .find(
          {
            group_id: this.replicationStreamId,
            snapshot_done: false
          },
          {
            session: this.session,
            projection: { schema_name: 1, table_name: 1 }
          }
        )
        .toArray();

      if (blockingTables.length > 0) {
        if (options?.throwOnConflict ?? true) {
          throw new ReplicationAssertionError(
            `Cannot mark snapshot done while source tables still require snapshotting. Tables: ${blockingTables.map((t) => `${t.schema_name}.${t.table_name}`).join(', ')}`
          );
        } else {
          return;
        }
      }

      await this.updateStreamMetadataInTransaction(this.snapshotDoneUpdate(no_checkpoint_before_lsn));
    });
  }

  protected override async markTableSnapshotRequiredInternal(_table: storage.SourceTable): Promise<void> {
    await this.updateStreamMetadata({ snapshot_done: false });
  }

  protected override async markTableSnapshotDoneInternal(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));

    await this.withTransaction(async () => {
      const writes = this.db.createWriteBatch(session, { ordered: false });
      writes.updateMany(
        this.db.sourceTablesV1(this.replicationStreamId),
        { _id: { $in: ids } },
        {
          $set: {
            snapshot_done: true
          },
          $unset: {
            snapshot_status: 1
          }
        }
      );

      if (no_checkpoint_before_lsn != null) {
        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId
          },
          {
            $max: {
              no_checkpoint_before: no_checkpoint_before_lsn
            }
          }
        );
      }
      await writes.execute();
    });
    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      return copy;
    });
  }

  private async autoActivate(lsn: string): Promise<void> {
    if (!this.needsActivation) {
      return;
    }

    const session = this.session;
    let activated = false;
    let needsFutureActivationCheck = true;
    await this.withTransaction(async () => {
      // Reset on transaction retries.
      activated = false;
      needsFutureActivationCheck = true;

      const doc = (await this.db.sync_rules.findOne(
        { _id: this.replicationStreamId },
        { session }
      )) as SyncRuleDocumentV1;
      if (doc && doc.state == storage.SyncRuleState.PROCESSING && doc.snapshot_done && doc.last_checkpoint != null) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId
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
            _id: { $ne: this.replicationStreamId },
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
          },
          {
            $set: {
              state: storage.SyncRuleState.STOP
            }
          },
          { session }
        );
        activated = true;
      } else if (doc?.state != storage.SyncRuleState.PROCESSING) {
        needsFutureActivationCheck = false;
      }
    }, session);
    if (activated) {
      this.logger.info(`Activated new replication stream at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivation = false;
    } else if (!needsFutureActivationCheck) {
      this.needsActivation = false;
    }
  }
}
