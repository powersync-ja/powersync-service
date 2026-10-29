import * as lib_mongo from '@powersync/lib-service-mongodb';
import {
  ErrorCode,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { EventDefinitionId } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { canCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { OpIdRangeExhausted } from '../MongoIdSequence.js';
import { MongoParsedSyncConfigSet } from '../MongoParsedSyncConfigSet.js';
import { MongoWriteBatch } from '../MongoWriteBatch.js';
import { OperationBatch } from '../OperationBatch.js';
import { stopReplicationStreamPipeline } from '../SyncRuleStateUpdate.js';
import { PreparedPublication } from '../common/PreparedPublication.js';
import { LoadedSourceRecord } from '../common/SourceRecordStore.js';
import { SyncRuleDocumentBase } from '../models.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { ReplicationPipelineV3 } from './ReplicationPipelineV3.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { CustomCheckpointRequestDocumentV3, ReplicationStreamDocumentV3, SourceTableDocumentV3 } from './models.js';
import {
  createNewSourceTable,
  overlappingSourceTableFilter,
  planSourceTableReconciliation,
  sourceTableDesiredResolution,
  sourceTableFromDocument,
  SourceTableReconciliationContext
} from './source-table-utils.js';

/**
 * Maximum unfinished payload preparations within one flush. Two allow one
 * group's uploads to overlap the next block's reads, evaluation and uploads,
 * while stopping further block reads if both preparations stall.
 *
 * This bounds active preparation work, not total retained groups or bytes:
 * completed groups remain retained until publication. Input and transaction
 * limits still bound that prefix. This is a conservative starting point;
 * increasing it needs throughput and retained-memory measurements.
 */
const MAX_CONCURRENT_PAYLOAD_PREPARATIONS = 2;
/** Event-only rows need a count boundary too; they never fill OperationBatch. */
const MAX_CHECKPOINTS_PER_INPUT = 2000;

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare readonly db: VersionedPowerSyncMongoV3;

  private readonly store: SourceRecordStoreV3;
  private readonly pipeline: ReplicationPipelineV3;
  private queueAutomaticFlush = false;
  private pendingPublications = 0;
  /** Avoid repeating idempotent createIndexes calls for every checkpoint flush. */
  private readonly initializedCustomCheckpointEventIds = new Set<EventDefinitionId>();
  // Used only to validate supplied checkpoint ids; it does not initialize collections.
  private readonly knownEventIds: ReadonlySet<EventDefinitionId>;
  private readonly syncConfigIds: bson.ObjectId[];
  private needsActivationV3 = true;
  private lastWaitingLogThrottledV3 = 0;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.pipeline = options.replicationLock.replicationPipeline ??= new ReplicationPipelineV3(
      options.replicationLock,
      options.opIdAllocator
    );
    const syncConfigIds = options.syncConfigIds ?? [];
    if (syncConfigIds.length == 0) {
      throw new ReplicationAssertionError('Missing sync config id for v3 batch');
    }
    this.syncConfigIds = syncConfigIds;
    this.knownEventIds = new Set(this.mapping.allEventDefinitionIds());
    this.store = new SourceRecordStoreV3(this.db, this.replicationStreamId, this.mapping);
  }

  override async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    return super.withWriterLock(async () => {
      if (this.pipeline.inputOwner !== this) {
        await this.pipeline.inputOwner?.sealPipelineInput();
      }
      await this.pipeline.capacity();
      this.pipeline.inputOwner = this;
      try {
        const result = await this.saveInternal(record);
        if (this.write_checkpoint_batch.length >= MAX_CHECKPOINTS_PER_INPUT) {
          return this.automaticFlush();
        }
        return result;
      } catch (error) {
        this.pipeline.fail(error);
        throw error;
      }
    });
  }

  /** Seal on writer switches so only one stream-wide input block accumulates. */
  async sealPipelineInput(): Promise<storage.FlushedResult | null> {
    if (!this.batch?.hasData() && this.write_checkpoint_batch.length === 0) {
      return null;
    }
    this.queueAutomaticFlush = true;
    try {
      return await this.flushInternal();
    } finally {
      this.queueAutomaticFlush = false;
    }
  }

  protected override async automaticFlush(): Promise<storage.FlushedResult | null> {
    return this.sealPipelineInput();
  }

  /** Metadata, explicit flushes and checkpoints drain the admitted stream prefix. */
  protected override async withWriterLock<T>(callback: () => Promise<T>, sealInput = true): Promise<T> {
    return super.withWriterLock(async () => {
      if (sealInput) {
        await this.pipeline.inputOwner?.sealPipelineInput();
      }
      await this.pipeline.drain();
      return callback();
    });
  }

  override async [Symbol.asyncDispose](): Promise<void> {
    const pending = this.pendingPublications > 0 || this.batch?.hasData() || this.write_checkpoint_batch.length > 0;
    if (pending) {
      this.pipeline.fail(new ReplicationAbortedError('Disposed writer with pending replication work'));
    }
    await this.options.replicationLock.writerMutex.exclusiveLock(async () => {
      if (pending) {
        await this.pipeline.join();
      }
      if (this.pipeline.inputOwner === this) {
        this.pipeline.inputOwner = undefined;
      }
      await super[Symbol.asyncDispose]();
    });
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatchV3 {
    return new PersistedBatchV3(this.db, this.replicationStreamId, this.mapping, writtenSize, {
      logger: this.logger,
      objectStorage: this.options.objectStorage,
      inlineThresholdBytes: this.options.inlineThresholdBytes,
      signal: this.options.signal,
      objectStorageUsageWriterId: this.objectStorageUsageWriterId
    });
  }

  protected get sourceRecordStore(): SourceRecordStoreV3 {
    return this.store;
  }

  protected override async withTransaction<T>(
    callback: (stream: SyncRuleDocumentBase) => Promise<T>,
    session = this.session
  ): Promise<T> {
    return this.withFencedTransaction(
      () => this.fence(session, { state: 1, sync_configs: 1 }),
      async (stream) => {
        this.assertPreparationContext(stream as ReplicationStreamDocumentV3);
        return callback(stream);
      },
      session
    );
  }

  protected override async flushInner(
    options?: storage.BatchBucketFlushOptions,
    checkpoint?: (stream: SyncRuleDocumentBase, lastOp: InternalOpId, writes?: MongoWriteBatch) => Promise<void>,
    projection?: lib_mongo.mongo.Document
  ): Promise<storage.FlushedResult | null> {
    using _span = this.tracer.span('storage', 'flush');
    await this.hooks?.beforeBatchFlush?.(this);
    const input = this.batch;
    const checkpoints = this.write_checkpoint_batch.slice();
    const allocator = this.pipeline.allocator;
    await allocator.ensureCapacity();
    const contextProjection = { ...projection, state: 1, last_persisted_op: 1, sync_configs: 1 };
    if (checkpoints.length > 0) {
      await this.prepareCustomWriteCheckpoints();
    }
    await this.pipeline.capacity();
    const overlay = this.pipeline.beginRead();
    let prepared: Awaited<ReturnType<MongoBucketBatchV3['prepareRows']>>;
    try {
      prepared = await this.prepareRows(this.session, input, overlay);
    } finally {
      this.pipeline.endRead();
    }
    const { publications, repairs, resumeBatch, changes, hasWrites, ready } = prepared;
    const retainedBytes =
      prepared.retainedBytes +
      checkpoints.reduce((bytes, checkpoint) => bytes + (bson.calculateObjectSize(checkpoint) + 256) * 4, 0);
    // Failure must stop admission even if capacity is occupied by an older
    // publisher and this prefix has not yet entered the FIFO.
    void ready.catch((error: unknown) => this.pipeline.fail(error));
    const publishCheckpoint = checkpoint != null && resumeBatch == null;
    let lastOp = prepared.lastOp;
    let checkpointOp: bigint | undefined;
    if (checkpoints.length > 0) {
      while (true) {
        try {
          checkpointOp = allocator.sequence(lastOp).next();
          lastOp = checkpointOp;
          break;
        } catch (error) {
          if (!(error instanceof OpIdRangeExhausted)) {
            throw error;
          }
          await allocator.reserve();
        }
      }
    }
    // Reservations are globally disjoint and this lease's allocator consumes IDs
    // assigned to sealed work, even if publication is abandoned. The writer mutex
    // serializes application under this lease; the shared overlay covers pending writes;
    // the transaction fence excludes a different lease owner. Thus preparation
    // does not need to read the persisted head to allocate IDs or reuse records.
    allocator.consume(lastOp);
    const clearError = !this.clearedError && hasWrites;
    const publish = async (session: lib_mongo.mongo.ClientSession) => {
      await this.publishPrepared(
        lastOp,
        contextProjection,
        publications,
        options,
        async (stream, writes) => {
          // An empty flush allocates no ID. Use the durable head from the
          // fresh fence for its checkpoint and returned flushed operation.
          if (lastOp === 0n) {
            lastOp = this.persistedOpHead(stream);
          }
          if (clearError) {
            writes.updateOne(
              this.db.sync_rules,
              { _id: this.replicationStreamId },
              {
                $set: { last_fatal_error: null, last_fatal_error_ts: null }
              }
            );
          }
          if (checkpointOp != null) {
            this.queueCustomWriteCheckpoints(writes, checkpointOp, checkpoints);
          }
          if (publishCheckpoint) {
            // Checkpoint decisions use this attempt's freshly fenced state.
            await checkpoint(stream, lastOp, writes);
          } else {
            this.onReplicationTransactionFlush(writes, lastOp);
          }
        },
        session
      );
      this.clearedError ||= clearError;
      this.last_flushed_op = lastOp;
      // markRecordUnavailable only synchronously queues connector-owned repair
      // keys. It must not mutate input, perform async work, or re-enter storage:
      // the writer mutex is still held. Delivery must finish before flush() returns;
      // WalStream drains that queue before commit()/ack().
      // Once committed, these writes cannot be rolled back if the callback
      // throws. Invalidate the whole local lease while the mutex is held, so
      // no writer sharing it (including queued calls) can advance progress
      // past an undelivered repair. A fresh replication attempt must replay
      // from the old source position and redetect incomplete rows.
      try {
        const notify = this.markRecordUnavailable;
        if (notify != null) {
          for (const record of repairs) {
            notify(record);
          }
        }
      } catch (error) {
        this.options.replicationLock.invalidate(
          new ReplicationAbortedError(
            'Committed replication repair notification failed; restart replication before advancing source progress',
            error
          )
        );
        throw error;
      }
      await this.hooks?.afterBatchFlush?.(this);
    };
    // Repairs stay synchronous so the connector can repair rows before source
    // progress advances. Ordinary rows may pipeline even for repair-capable sources.
    const shouldQueue = this.queueAutomaticFlush && repairs.length === 0;
    if (shouldQueue) {
      await this.pipeline.capacity(retainedBytes);
      this.batch = resumeBatch;
      this.write_checkpoint_batch.splice(0, checkpoints.length);
      this.pendingPublications++;
      const receipt = this.pipeline.enqueue(changes, retainedBytes, ready, publish);
      void receipt
        .finally(() => {
          this.pendingPublications--;
        })
        .catch(() => {});
      return null;
    }
    await this.pipeline.drain();
    await ready;
    await publish(this.session);
    this.batch = resumeBatch;
    this.write_checkpoint_batch.splice(0, checkpoints.length);
    return { flushed_op: lastOp };
  }

  private assertPreparationContext(stream: ReplicationStreamDocumentV3): void {
    if ([storage.SyncRuleState.STOP, storage.SyncRuleState.TERMINATED].includes(stream.state)) {
      throw new ReplicationAbortedError('Replication stream has stopped');
    }
    this.options.parsedSyncConfig.assertPublicationConfigs(stream.sync_configs);
  }

  /**
   * Internal publisher: called either by a durable barrier or by the lease FIFO.
   * finish() must only queue writes independent of the final data group, using
   * the fresh fence and planned IDs; it must not execute the supplied batch or
   * read results from unpublished data. Activation and cleanup remain separate.
   */
  private async publishPrepared(
    lastOp: InternalOpId,
    projection: lib_mongo.mongo.Document,
    publications: PreparedPublication[],
    options: storage.BatchBucketFlushOptions | undefined,
    finish: (stream: ReplicationStreamDocumentV3, writes: MongoWriteBatch) => Promise<void>,
    session = this.session
  ): Promise<void> {
    const tracer = session === this.session ? this.tracer : this.pipeline.tracer;
    // This gate covers every sealed batch in the prefix, including truncate/drop.
    // The transaction must never wait for source reads, evaluation or S3 PUTs.
    // The FIFO / durable barrier has joined readiness before opening a
    // transaction. Keep this idempotent gate for truncate/drop too.
    for (const publication of publications) {
      await publication.preparePayloads();
    }
    let attempts = 0;
    const retryDeadline = Date.now() + 90_000;
    try {
      await this.withFencedTransaction(
        () => this.fence(session, projection),
        async (document) => {
          attempts++;
          if (attempts > 20 && Date.now() > retryDeadline) {
            throw new ServiceError(ErrorCode.PSYNC_S1402, 'Max transaction tries exceeded');
          }
          const stream = document as ReplicationStreamDocumentV3;
          this.assertPreparationContext(stream);
          const head = this.persistedOpHead(stream);
          if (lastOp !== 0n && lastOp <= head) {
            throw new ReplicationAssertionError(
              `Prepared operation ${lastOp} does not advance persisted stream head ${head}`
            );
          }
          // Earlier groups may replace the same source record or bucket state.
          // Preserve their execution order; only the final group's writes are
          // independent of the head/checkpoint tail built from this fresh fence.
          for (const publication of publications.slice(0, -1)) {
            using _persist = tracer.span('storage', 'persist_flush');
            await publication.publish(session, options);
          }
          const writes = this.db.createWriteBatch(session, { ordered: false });
          await finish(stream, writes);
          const finalPublication = publications.at(-1);
          if (finalPublication != null) {
            using _persist = tracer.span('storage', 'persist_flush');
            await finalPublication.publish(session, options, writes);
          } else {
            await writes.execute();
          }
        },
        session,
        'Publishing prepared work',
        tracer
      );
    } catch (error) {
      // Publication failed after the driver's retries or our attempt limit.
      // Leave uploaded objects and markers alone: the commit may have succeeded.
      // Stop every writer on this lease and recover from persisted source progress.
      this.options.replicationLock.invalidate(
        new ReplicationAbortedError('Publication failed; restart replication from persisted progress', error)
      );
      throw error;
    }
  }

  private async prepareRows(
    session: lib_mongo.mongo.ClientSession,
    input: OperationBatch | null,
    overlay: ReadonlyMap<string, LoadedSourceRecord>
  ) {
    const publications: PreparedPublication[] = [];
    const repairs: storage.SaveUpdate[] = [];
    let resumeBatch: OperationBatch | null = null;
    let lastOp = 0n;
    let hasWrites = false;
    let retainedBytes = 0;
    const changes = new Map<string, LoadedSourceRecord>();
    if (input == null) {
      return {
        publications,
        repairs,
        resumeBatch,
        lastOp,
        hasWrites,
        changes,
        retainedBytes,
        ready: Promise.resolve()
      };
    }
    using _preparation = this.tracer.span('storage', 'prepare');
    const allocator = this.pipeline.allocator;
    let sizes: Map<string, number> | undefined;
    // Keep the existing size-based read partitioning, including per-table raw
    // data storage and snapshot skip-existing exceptions. Move these reads;
    // publication never repeats them as a validation pass.
    if (
      this.options.storeCurrentData &&
      !this.skipExistingRows &&
      input.batch.some((op) => op.record.sourceTable.storeCurrentData)
    ) {
      sizes = await this.sourceRecordStore.loadSizes(
        session,
        input.batch
          .filter((op) => op.record.sourceTable.storeCurrentData)
          .map((op) => ({ sourceTableId: mongoTableId(op.record.sourceTable.id), replicaId: op.beforeId }))
      );
    }
    // Only this bounded publication's observations/evaluated rows are retained.
    // No state is shared with another pending publication or another writer.
    for (const [key, record] of overlay) {
      sizes?.set(key, record.data?.length() ?? 0);
    }
    const records = new Map<string, LoadedSourceRecord | null>(overlay);
    let writtenSize = 0;
    // Overlap packing/uploads with the next block's reads and evaluation. The
    // existing input and transaction limits still bound this application prefix.
    // Its readiness is transferred to the lease FIFO after row application.
    const pending = new Set<Promise<void>>();
    let preparationFailure: { error: unknown } | undefined;
    const prepareGroup = (group: PersistedBatchV3) => {
      retainedBytes += group.estimatedRetainedBytes;
      const publication = group.seal();
      // Paths may become externally visible as soon as preparation starts, so
      // never reuse these IDs even if a later read or evaluation fails.
      allocator.consume(lastOp);
      publications.push(publication);
      const task = publication
        .preparePayloads()
        .catch((error: unknown) => {
          preparationFailure ??= { error };
        })
        .finally(() => pending.delete(task));
      pending.add(task);
    };
    try {
      for (const block of input.batched(sizes)) {
        if (resumeBatch != null) {
          for (const op of block) {
            resumeBatch.push(op);
          }
          continue;
        }
        // Do not load another application block while all preparation slots are
        // occupied. Completed groups remain owned by this bounded input flush.
        if (pending.size >= MAX_CONCURRENT_PAYLOAD_PREPARATIONS) {
          await Promise.race(pending);
        }
        if (preparationFailure != null) {
          throw preparationFailure.error;
        }
        this.options.replicationLock.throwIfAborted();
        this.pipeline.advanceReadTime();
        const missing = block.filter((op) => !records.has(op.internalBeforeKey));
        if (missing.length > 0) {
          using _lookup = this.tracer.span('storage', 'lookup');
          const loaded = await this.sourceRecordStore.loadDocuments(
            session,
            missing.map((op) => ({
              sourceTableId: mongoTableId(op.record.sourceTable.id),
              replicaId: op.beforeId
            })),
            this.skipExistingRows
          );
          for (const op of missing) {
            records.set(op.internalBeforeKey, loaded.get(op.internalBeforeKey) ?? null);
          }
        }
        let group: PersistedBatchV3 | null = this.createPersistedBatch(writtenSize);
        for (const op of block) {
          if (resumeBatch != null) {
            resumeBatch.push(op);
            continue;
          }
          let next: LoadedSourceRecord | null;
          while (true) {
            const rowRepairs: storage.SaveUpdate[] = [];
            const row = this.createPersistedBatch(0);
            const sequence = allocator.sequence(lastOp);
            try {
              using _evaluate = this.tracer.span('evaluate');
              next = this.saveOperation(
                row,
                op,
                records.get(op.internalBeforeKey) ?? null,
                sequence,
                this.markRecordUnavailable == null
                  ? undefined
                  : (record) => {
                      rowRepairs.push(record);
                    }
              );
              group!.append(row);
              hasWrites ||= row.currentSize > 0;
              repairs.push(...rowRepairs);
              lastOp = sequence.last();
              break;
            } catch (error) {
              if (!(error instanceof OpIdRangeExhausted)) {
                throw error;
              }
              // Retry only the unfinished row; its payload has not been uploaded.
              this.options.replicationLock.throwIfAborted();
              await allocator.reserve();
            }
          }
          if (op.afterId == null || !storage.replicaIdEquals(op.beforeId, op.afterId)) {
            // Preserve evaluation's existing repeated-row behavior: a deleted
            // identity no longer supplies raw values or membership to later rows.
            records.set(op.internalBeforeKey, null);
            // Persisted soft deletes still exist for snapshot skip-existing
            // lookups, even though repeated streaming updates use empty state.
            changes.set(op.internalBeforeKey, {
              sourceTableId: mongoTableId(op.record.sourceTable.id),
              replicaId: op.beforeId,
              cacheKey: op.internalBeforeKey,
              data: null,
              buckets: [],
              lookups: []
            });
          }
          if (next != null) {
            records.set(op.internalAfterKey!, next);
            changes.set(op.internalAfterKey!, next);
            sizes?.set(op.internalAfterKey!, next.data?.length() ?? 0);
          }
          if (group!.shouldFlushTransaction()) {
            prepareGroup(group!);
            group = null;
            resumeBatch = new OperationBatch();
          }
        }
        if (group != null) {
          writtenSize = group.currentSize;
          prepareGroup(group);
        }
      }
    } catch (error) {
      // Application failure must join every upload already started by this
      // prefix. Successful application transfers readiness to the lease FIFO.
      if (this.queueAutomaticFlush) {
        this.pipeline.fail(error);
      }
      await Promise.all(pending);
      throw error;
    }
    const ready = Promise.all(pending).then(() => {
      if (preparationFailure != null) {
        throw preparationFailure.error;
      }
    });
    // Checkpoint ID reservation can yield before readiness is registered with
    // the FIFO; observe rejection now without changing the original receipt.
    void ready.catch(() => {});
    return {
      publications,
      repairs,
      resumeBatch: resumeBatch?.hasData() ? resumeBatch : null,
      lastOp,
      hasWrites,
      changes,
      retainedBytes,
      ready
    };
  }

  protected override async truncateSingleInternal(sourceTable: storage.SourceTable): Promise<InternalOpId> {
    const sourceId = mongoTableId(sourceTable.id);
    const session = this.session;
    const projection = { state: 1, sync_configs: 1, last_persisted_op: 1 };
    const allocator = this.pipeline.allocator;
    while (true) {
      await allocator.ensureCapacity();
      // Preserve the existing bounded table scan. Hard deletion lets the next
      // scan resume at remaining live records without retaining a table overlay.
      const records = await this.sourceRecordStore.loadTruncateBatch(session, sourceId, 2000);
      const group = this.createPersistedBatch(0);
      let lastOp = 0n;
      let processed = 0;
      for (const record of records) {
        while (true) {
          const row = this.createPersistedBatch(0);
          const sequence = allocator.sequence(lastOp);
          try {
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
            // Since this is not from streaming replication, we can do a hard delete.
            row.hardDeleteCurrentData(sourceId, record.replicaId);
            group.append(row);
            lastOp = sequence.last();
            break;
          } catch (error) {
            if (!(error instanceof OpIdRangeExhausted)) {
              throw error;
            }
            this.options.replicationLock.throwIfAborted();
            await allocator.reserve();
          }
        }
        processed++;
        if (group.shouldFlushTransaction()) {
          break;
        }
      }
      allocator.consume(lastOp);
      await this.publishPrepared(lastOp, projection, [group.seal()], undefined, async (stream, writes) => {
        if (lastOp === 0n) {
          lastOp = this.persistedOpHead(stream);
        }
        this.onReplicationTransactionFlush(writes, lastOp);
      });
      if (processed === records.length && records.length < 2000) {
        return lastOp;
      }
    }
  }

  protected override persistedOpHead(stream: SyncRuleDocumentBase): InternalOpId {
    return (stream as ReplicationStreamDocumentV3).last_persisted_op ?? 0n;
  }

  protected override onReplicationTransactionFlush(writes: MongoWriteBatch, lastOp: InternalOpId): void {
    // Durably advance the stream-level head of persisted ops within the flush transaction.
    // This ensures a checkpoint created later (even by an empty commit, or by a freshly-appended
    // config that replicates nothing) covers all ops persisted before a potential crash.
    writes.updateOne(
      this.db.sync_rules,
      {
        _id: this.replicationStreamId
      },
      {
        $max: {
          last_persisted_op: lastOp
        }
      }
    );
  }

  private selectedSyncConfigObjectIds(syncConfigIds: string[]): bson.ObjectId[] {
    const selectedIds = new Set(this.syncConfigIds.map((id) => id.toHexString()));
    return syncConfigIds
      .filter((id) => selectedIds.has(id) && bson.ObjectId.isValid(id))
      .map((id) => new bson.ObjectId(id));
  }

  private relevantSyncConfigIds(table: storage.SourceTable): bson.ObjectId[] {
    const bucketDataSourceIds = table.bucketDataSources.map((source) => this.mapping.bucketSourceId(source));
    const parameterLookupSourceIds = table.parameterLookupSources.map((source) =>
      this.mapping.parameterLookupId(source)
    );
    return this.selectedSyncConfigObjectIds(
      this.mapping.syncConfigIdsForSourceTable(
        this.syncConfigIds.map((id) => id.toHexString()),
        table.ref,
        bucketDataSourceIds,
        parameterLookupSourceIds,
        [...table.eventDefinitionIds!]
      )
    );
  }

  private relevantSyncConfigIdsForTables(tables: storage.SourceTable[]): bson.ObjectId[] {
    const ids = new Map<string, bson.ObjectId>();
    for (const table of tables) {
      for (const id of this.relevantSyncConfigIds(table)) {
        ids.set(id.toHexString(), id);
      }
    }
    return [...ids.values()];
  }

  private snapshotBlockingSourceTablesFilter(): Record<string, unknown> {
    const clauses = this.syncConfigIds.flatMap((syncConfigId) => {
      const filter = this.mapping.snapshotBlockingSourceTablesFilter(syncConfigId.toHexString()) as {
        $or?: Record<string, unknown>[];
      };
      return filter.$or ?? [];
    });

    if (clauses.length == 0) {
      return {
        snapshot_done: false,
        _id: { $exists: false }
      };
    }

    return {
      snapshot_done: false,
      $or: clauses
    };
  }

  protected async cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]) {
    for (const table of sourceTables) {
      await this.db
        .sourceRecords(this.replicationStreamId, mongoTableId(table.id))
        .drop()
        .catch((error) => {
          if (lib_mongo.isMongoNamespaceNotFoundError(error)) {
            return;
          }
          throw error;
        });
    }
  }

  protected override async resolveTablesInternal(
    options: storage.ResolveTablesOptions
  ): Promise<storage.ResolveTablesResult> {
    // The test-only override is a whole parsed set, so the sync rules and the mapping
    // used below always come from the same parse.
    const parsedOverride = options.parsedSyncConfig as MongoParsedSyncConfigSet | undefined;
    const syncConfig = parsedOverride?.hydratedSyncConfig ?? this.sync_rules;
    const mapping = parsedOverride?.mapping ?? this.mapping;
    const eventById = (parsedOverride ?? this.options.parsedSyncConfig).eventById;

    const { connection_id, source } = options;
    const reconcile = options.reconcileSourceTables ?? storage.defaultSourceTableReconciler;
    const identity = {
      schema: source.schema,
      name: source.name,
      objectId: source.objectId,
      replicaIdColumns: source.replicaIdColumns.map((column) => ({
        name: column.name,
        type: column.type,
        type_oid: column.typeId
      }))
    };

    let result: storage.ResolveTablesResult | null = null;
    const session = this.session;

    await this.withTransaction(async () => {
      const col = this.db.sourceTables(this.replicationStreamId);

      // Find records that overlap by name or relation id.
      const candidateDocs = await col
        .find(overlappingSourceTableFilter(connection_id, identity), { session })
        .toArray();

      const candidateTables = candidateDocs.map((doc) =>
        sourceTableFromDocument(doc, source.connectionTag, syncConfig, mapping, eventById)
      );
      const candidates = candidateTables.map((table) => table.clone());
      const resolution = await reconcile({ source, candidates });
      storage.validateSourceTableCandidateResolution(candidates, resolution);

      // Persist metadata from the reconciler without mutating the queried documents.
      for (const { id, sourceMetadata } of storage.diffSourceTableUpdates(candidateTables, resolution)) {
        await col.updateOne({ _id: mongoTableId(id) }, { $set: { source_metadata: sourceMetadata } }, { session });
      }

      const context: SourceTableReconciliationContext = {
        connectionId: connection_id,
        connectionTag: source.connectionTag,
        identity,
        storeCurrentData: source.sendsCompleteRows !== true,
        syncConfig,
        mapping,
        desired: sourceTableDesiredResolution(syncConfig, source, mapping, eventById),
        sourceCompatibleTables: resolution.compatibleTables,
        newTableSourceMetadata: resolution.newTableValues.sourceMetadata
      };

      // Plan record reuse, membership changes, creation, and removal.
      const plan = planSourceTableReconciliation(candidateDocs, context);

      // Persist narrowing for incomplete snapshots only. Snapshot-complete docs keep stale
      // coverage ids so compatible future configs can reuse already-snapshotted data.
      // Narrowing occurs after removing a sync config, meaning we don't process those
      // definitions anymore.
      for (const update of plan.narrowingUpdates) {
        await col.updateOne(
          { _id: update.id },
          {
            $set: {
              bucket_data_source_ids: update.memberships.bucketDataSourceIds,
              parameter_lookup_source_ids: update.memberships.parameterLookupSourceIds,
              event_definition_ids: update.memberships.eventDefinitionIds
            }
          },
          { session }
        );
      }

      // Any desired membership not covered by an existing doc gets a new source table.
      // That table snapshots only the uncovered memberships.
      if (plan.newTableMemberships != null) {
        const id = options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId();
        const { doc, table } = createNewSourceTable(id, plan.newTableMemberships, context);

        await col.insertOne(doc, { session });
        await this.db.initializeSourceRecordsCollection(this.replicationStreamId, doc._id, session);
        plan.tables.push(table);
      }

      result = {
        tables: plan.tables,
        dropTables: plan.dropDocs.map((doc) =>
          sourceTableFromDocument(doc, context.connectionTag, syncConfig, mapping, eventById)
        )
      };
    }, session);

    return result!;
  }

  protected override async getSourceTableStatusInternal(
    table: storage.SourceTable
  ): Promise<storage.SourceTable | null> {
    const doc = (await this.db
      .sourceTables(this.replicationStreamId)
      .findOne({ _id: mongoTableId(table.id) }, { session: this.session })) as SourceTableDocumentV3 | null;
    if (doc == null) {
      return null;
    }

    return sourceTableFromDocument(
      doc,
      table.ref.connectionTag,
      this.sync_rules,
      this.mapping,
      this.options.parsedSyncConfig.eventById
    );
  }

  protected override async commitInternal(
    lsn: string,
    options?: storage.BucketBatchCommitOptions
  ): Promise<storage.CheckpointResult> {
    this.options.replicationLock.throwIfAborted();
    if (this.markRecordUnavailable != null && this.batch?.hasData()) {
      // A combined data+checkpoint commit cannot return control to the connector
      // to process repair notifications between those two durable boundaries.
      // Require its explicit flush -> repair -> commit ordering even when this
      // particular input might turn out not to need repair. An empty batch does
      // not prove its external repair queue is drained; that remains the caller's
      // responsibility. keepalive() delegates here and has the same requirement.
      throw new ReplicationAssertionError(
        'Flush pending rows and process markRecordUnavailable repairs before committing source progress'
      );
    }
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    using _ = this.tracer.span('storage', 'commit');

    const checkpoint = async (
      stream: SyncRuleDocumentBase,
      lastOp?: InternalOpId,
      additionalWrites?: MongoWriteBatch
    ) => {
      const now = new Date();
      const preUpdateDocument = stream as ReplicationStreamDocumentV3;
      this.assertPreparationContext(preUpdateDocument);
      // Combined commits join the final data group's batch. Empty commits own
      // and execute their batch here. Decisions depend only on the fresh fence
      // and planned IDs, so no read of the queued data writes is required.
      const writes = additionalWrites ?? this.db.createWriteBatch(this.session, { ordered: false });
      writes.updateMany(
        this.db.write_checkpoints,
        { processed_at_lsn: null, 'lsns.1': { $lte: lsn } },
        { $set: { processed_at_lsn: lsn } }
      );

      const states =
        preUpdateDocument?.sync_configs?.filter((config) => this.syncConfigIds.some((id) => id.equals(config._id))) ??
        [];
      if (states.length == 0) {
        throw new ReplicationAssertionError(
          `Failed to update checkpoint - no matching sync_config for _id: ${this.replicationStreamId}/${this.syncConfigIds
            .map((id) => id.toHexString())
            .join(',')}`
        );
      }
      // The replication job / MongoBucketBatch must be constructed with all replicating (PROCESSING / ACTIVE)
      // sync configs in the stream, otherwise we'll get inconsistencies. Configs in other states (e.g. STOP)
      // remain embedded in the document and are ignored here.
      const missingSyncConfig = preUpdateDocument!.sync_configs.find(
        (config) =>
          [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE].includes(config.state) &&
          !this.syncConfigIds.some((id) => id.equals(config._id))
      );
      if (missingSyncConfig != null) {
        throw new ReplicationAssertionError(`Replication job not configured for sync config ${missingSyncConfig._id}`);
      }

      // Effective head of the stream's op sequence.
      // A combined flush supplies its new head; an empty commit uses the fenced persisted head.
      const newCheckpoint = lastOp ?? this.persistedOpHead(preUpdateDocument);

      let checkpointBlocked = false;
      let checkpointCreated = false;
      let checkpointLogState: unknown = null;
      const unblockedConfigIds: bson.ObjectId[] = [];

      for (const state of states) {
        if (state.last_checkpoint != null && state.last_checkpoint > newCheckpoint) {
          // last_persisted_op is $max-advanced durably in the same transaction as every flush, and
          // checkpoints are only ever created at that head, so a checkpoint past the head means the
          // op sequence or the stored state is corrupt.
          throw new ReplicationAssertionError(
            `Invariant violation: sync config ${state._id} has last_checkpoint ${state.last_checkpoint} > stream head ${newCheckpoint}`
          );
        }

        const canCheckpoint = canCheckpointState(lsn, {
          snapshotDone: state.snapshot_done === true,
          lastCheckpointLsn: state.last_checkpoint_lsn,
          noCheckpointBefore: state.no_checkpoint_before
        });

        if (!canCheckpoint) {
          checkpointBlocked = true;
          // Log the first blocked config's state.
          checkpointLogState ??= {
            snapshot_done: state.snapshot_done,
            last_checkpoint_lsn: state.last_checkpoint_lsn,
            no_checkpoint_before: state.no_checkpoint_before
          };
          continue;
        }

        checkpointCreated ||= createEmptyCheckpoints || state.last_checkpoint !== newCheckpoint;
        unblockedConfigIds.push(state._id);
      }

      // Every commit advances the stream's resume position: commit() flushes first, so all
      // source changes up to this lsn have been persisted, even when checkpoints are blocked.
      // In the future we could also advance this on flush, when the connector provides the
      // current position (see setResumeLsn, which connectors may already call after flushing).
      const resumeLsnUpdate = {
        resume_lsn: lsn,
        ...(lastOp == null ? {} : { last_persisted_op: lastOp })
      };

      if (unblockedConfigIds.length > 0) {
        // All unblocked configs get the SAME new value, so we apply it with a single updateOne
        // (single-document atomicity).
        const updateSet: Record<string, any> = {
          last_fatal_error: null,
          last_fatal_error_ts: null
        };
        // Only advance checkpoint fields when an actual (non-empty) checkpoint is created, matching
        // the previous per-config / v1 behaviour.
        if (checkpointCreated) {
          updateSet['sync_configs.$[config].last_checkpoint'] = newCheckpoint;
          updateSet['sync_configs.$[config].last_checkpoint_lsn'] = lsn;
          updateSet['last_checkpoint_ts'] = now;
        }

        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: unblockedConfigIds }
          },
          { $set: updateSet, $max: resumeLsnUpdate },
          {
            arrayFilters: checkpointCreated ? [{ 'config._id': { $in: unblockedConfigIds } }] : undefined
          }
        );
      } else {
        // All selected configs are blocked - only update keepalive/error tracking and the
        // resume position.
        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId
          },
          {
            $set: {
              last_fatal_error: null,
              last_fatal_error_ts: null
            },
            $max: resumeLsnUpdate
          }
        );
      }

      if (additionalWrites == null) {
        await writes.execute();
      }
      return { checkpointBlocked, checkpointCreated, checkpointLogState, newCheckpoint };
    };
    const projection = { state: 1, sync_configs: 1, last_persisted_op: 1 };
    const { checkpointBlocked, checkpointCreated, checkpointLogState, newCheckpoint } = await this.flushAndCommit(
      checkpoint,
      () => this.withFencedTransaction(() => this.fence(this.session, projection), checkpoint),
      options,
      projection
    );
    if (checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThrottledV3 > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn}. Current state: ${JSON.stringify(checkpointLogState)}`
        );
        this.lastWaitingLogThrottledV3 = Date.now();
      }
    } else {
      if (checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn}`);
      }
      await this.autoActivateV3(lsn);
      // All configs are now checkpointed at newCheckpoint (the stream head).
      await this.sourceRecordStore.postCommitCleanup(newCheckpoint, this.logger);
    }
    if (checkpointCreated) {
      await this.db.notifyCheckpoint();
    }
    return {
      checkpointBlocked,
      checkpointCreated
    };
  }

  protected override async setResumeLsnInternal(lsn: string): Promise<void> {
    using _ = this.tracer.span('storage', 'set_resume_lsn');
    // Losing occasional resume LSN would only reprocess source changes.
    // Keep the lease check atomic, but do not wait for majority replication of this resume hint.
    await this.updateStreamMetadata({ resume_lsn: { $literal: lsn } }, {}, { w: 1 });
  }

  private async autoActivateV3(lsn: string): Promise<void> {
    if (!this.needsActivationV3) {
      return;
    }

    const session = this.session;
    let activated = false;
    let needsFutureActivationCheck = true;
    await this.withTransaction(async () => {
      // Reset on transaction retries.
      needsFutureActivationCheck = true;
      activated = false;

      const doc = await this.db.sync_rules.findOne(
        {
          _id: this.replicationStreamId,
          'sync_configs._id': { $in: this.syncConfigIds }
        },
        {
          session,
          projection: {
            state: 1,
            sync_configs: 1
          }
        }
      );
      const states =
        (doc as ReplicationStreamDocumentV3)?.sync_configs?.filter((config) =>
          this.syncConfigIds.some((id) => id.equals(config._id))
        ) ?? [];
      if (doc == null || states.length == 0) {
        return;
      }

      const processingStates = states.filter((state) => state.state == storage.SyncRuleState.PROCESSING);
      if (
        doc.state == storage.SyncRuleState.PROCESSING &&
        processingStates.length == states.length &&
        states.every((state) => state.snapshot_done && state.last_checkpoint != null)
      ) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: this.syncConfigIds }
          },
          {
            $set: {
              state: storage.SyncRuleState.ACTIVE,
              'sync_configs.$[config].state': storage.SyncRuleState.ACTIVE
            }
          },
          {
            session,
            arrayFilters: [{ 'config._id': { $in: this.syncConfigIds } }]
          }
        );

        await this.db.sync_rules.updateMany(
          {
            _id: { $ne: this.replicationStreamId },
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
          },
          stopReplicationStreamPipeline(),
          { session }
        );
        activated = true;
      } else if (
        doc.state == storage.SyncRuleState.ACTIVE &&
        processingStates.length > 0 &&
        processingStates.every((state) => state.snapshot_done && state.last_checkpoint != null)
      ) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: processingStates.map((state) => state._id) }
          },
          {
            $set: {
              'sync_configs.$[activeConfig].state': storage.SyncRuleState.STOP,
              'sync_configs.$[processingConfig].state': storage.SyncRuleState.ACTIVE
            }
          },
          {
            session,
            arrayFilters: [
              { 'activeConfig.state': storage.SyncRuleState.ACTIVE },
              { 'processingConfig._id': { $in: processingStates.map((state) => state._id) } }
            ]
          }
        );
        activated = true;
      } else if (doc.state != storage.SyncRuleState.PROCESSING && doc.state != storage.SyncRuleState.ACTIVE) {
        needsFutureActivationCheck = false;
      } else if (doc.state == storage.SyncRuleState.ACTIVE && processingStates.length == 0) {
        needsFutureActivationCheck = false;
      }
    }, session);
    if (activated) {
      this.logger.info(`Activated new replication stream at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivationV3 = false;
    } else if (!needsFutureActivationCheck) {
      this.needsActivationV3 = false;
    }
  }

  private syncConfigMetadataUpdate(syncConfigIds: bson.ObjectId[], set: lib_mongo.mongo.Document) {
    return {
      sync_configs: {
        $map: {
          input: '$sync_configs',
          as: 'config',
          in: {
            $cond: [
              { $in: ['$$config._id', { $literal: syncConfigIds }] },
              { $mergeObjects: ['$$config', set] },
              '$$config'
            ]
          }
        }
      }
    };
  }

  private snapshotDoneUpdate(no_checkpoint_before_lsn: string) {
    return this.syncConfigMetadataUpdate(this.syncConfigIds, {
      snapshot_done: true,
      no_checkpoint_before: { $max: ['$$config.no_checkpoint_before', { $literal: no_checkpoint_before_lsn }] }
    });
  }

  protected override async markAllSnapshotDoneInternal(no_checkpoint_before_lsn: string): Promise<void> {
    await this.updateStreamMetadata(this.snapshotDoneUpdate(no_checkpoint_before_lsn), {
      'sync_configs._id': { $in: this.syncConfigIds }
    });
  }

  protected override async markSnapshotDoneInternal(
    no_checkpoint_before_lsn: string,
    options?: { throwOnConflict?: boolean }
  ): Promise<void> {
    await this.withTransaction(async () => {
      // Protect against race conditions
      const blockingTables = await this.db
        .sourceTables(this.replicationStreamId)
        .find(this.snapshotBlockingSourceTablesFilter(), {
          session: this.session,
          projection: { schema_name: 1, table_name: 1 }
        })
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

      await this.updateStreamMetadataInTransaction(this.snapshotDoneUpdate(no_checkpoint_before_lsn), {
        'sync_configs._id': { $in: this.syncConfigIds }
      });
    });
  }

  protected override async markTableSnapshotRequiredInternal(table: storage.SourceTable): Promise<void> {
    const syncConfigIds = this.relevantSyncConfigIds(table);
    if (syncConfigIds.length == 0) {
      return;
    }
    await this.updateStreamMetadata(this.syncConfigMetadataUpdate(syncConfigIds, { snapshot_done: false }), {
      'sync_configs._id': { $in: syncConfigIds }
    });
  }

  protected override async markTableSnapshotDoneInternal(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));
    const syncConfigIds = this.relevantSyncConfigIdsForTables(tables);

    await this.withTransaction(async () => {
      const writes = this.db.createWriteBatch(session, { ordered: false });
      writes.updateMany(
        this.db.sourceTables(this.replicationStreamId),
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

      if (no_checkpoint_before_lsn != null && syncConfigIds.length > 0) {
        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: syncConfigIds }
          },
          {
            $max: {
              'sync_configs.$[config].no_checkpoint_before': no_checkpoint_before_lsn
            }
          },
          {
            // Only set for sync configs that use this table
            arrayFilters: [{ 'config._id': { $in: syncConfigIds } }]
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

  private queueCustomWriteCheckpoints(
    writes: MongoWriteBatch,
    opId: InternalOpId,
    checkpoints: storage.CustomWriteCheckpointOptions[]
  ): void {
    if (checkpoints.length == 0) {
      return;
    }

    const checkpointsByEvent = Map.groupBy(checkpoints, (checkpoint) =>
      this.validateCustomCheckpointEventId(checkpoint.event_id)
    );

    for (const [eventId, checkpoints] of checkpointsByEvent) {
      this.batchCreateEventCustomWriteCheckpoints(writes, opId, eventId, checkpoints);
    }
  }

  protected override async prepareCustomWriteCheckpoints(): Promise<void> {
    // Most events never produce custom checkpoints, so create collections lazily
    // only for the event ids present in this batch. This hook runs before the
    // replication transaction because MongoDB cannot create indexes within it.
    const eventIds = new Set(
      this.write_checkpoint_batch.map((checkpoint) => this.validateCustomCheckpointEventId(checkpoint.event_id))
    );
    for (const eventId of eventIds) {
      if (this.initializedCustomCheckpointEventIds.has(eventId)) {
        continue;
      }
      await this.db.initializeCustomCheckpointRequestsCollection({
        replicationStreamId: this.replicationStreamId,
        eventId
      });
      this.initializedCustomCheckpointEventIds.add(eventId);
    }
  }

  private validateCustomCheckpointEventId(eventId: EventDefinitionId | undefined): EventDefinitionId {
    if (eventId == null) {
      throw new ServiceAssertionError('V3 custom checkpoints require an event definition id');
    }

    if (!this.knownEventIds.has(eventId)) {
      throw new ServiceAssertionError(`Unknown custom checkpoint event definition ${eventId}`);
    }
    return eventId;
  }

  private batchCreateEventCustomWriteCheckpoints(
    writes: MongoWriteBatch,
    opId: InternalOpId,
    eventId: EventDefinitionId,
    checkpoints: storage.CustomWriteCheckpointOptions[]
  ): void {
    // A repeated user within an event replaces the complete checkpoint state.
    const uniqueCheckpoints = new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, checkpoint]));
    writes.bulkWriteUnordered(
      this.db.customCheckpointRequests({
        eventId,
        replicationStreamId: this.replicationStreamId
      }),
      [...uniqueCheckpoints.values()].map((checkpointOptions) => {
        const set: Partial<CustomCheckpointRequestDocumentV3> = {
          user_id: checkpointOptions.user_id,
          checkpoint: checkpointOptions.checkpoint,
          op_id: opId
        };
        if (checkpointOptions.checkpoint_requested_at != null) {
          set.checkpoint_requested_at = checkpointOptions.checkpoint_requested_at;
        }

        return {
          updateOne: {
            filter: {
              user_id: checkpointOptions.user_id
            },
            update: {
              $set: set,
              ...(checkpointOptions.checkpoint_requested_at == null
                ? {
                    $unset: {
                      checkpoint_requested_at: 1
                    }
                  }
                : {})
            },
            upsert: true
          }
        };
      })
    );
  }
}
