import { storage } from '@powersync/service-core';
import { mongoTableId } from '../../utils/util.js';
import { PersistedBatch } from './common/PersistedBatch.js';
import { LoadedSourceRecord, SourceRecordStore } from './common/SourceRecordStore.js';
import { MongoIdSequence, OpIdRangeExhausted } from './MongoIdSequence.js';
import { MongoOpIdAllocator } from './MongoOpIdAllocator.js';
import { MongoPublicationWriter, PipelineContext } from './MongoReplicationPipeline.js';
import { OperationBatch, RecordOperation } from './OperationBatch.js';

/** Applies one writer's input using the stream's shared membership and ID order. */
export class MongoReplicationApplication {
  constructor(
    private readonly pipeline: MongoPublicationWriter,
    private readonly allocator: MongoOpIdAllocator,
    private readonly sourceRecordStore: SourceRecordStore,
    private readonly createPersistedBatch: (writtenSize: number) => PersistedBatch,
    private readonly applyOperation: (
      batch: PersistedBatch,
      operation: RecordOperation,
      before: LoadedSourceRecord | null,
      sequence: MongoIdSequence
    ) => LoadedSourceRecord | null,
    private readonly storeCurrentData: boolean,
    private readonly skipExistingRows: boolean,
    private readonly eagerPublication: boolean
  ) {}

  async apply(input: OperationBatch, before?: () => Promise<void>): Promise<void> {
    await this.pipeline.prepare(async (context) => {
      const { session, state } = context;
      let sizes: Map<string, number> | undefined;
      // Only load sizes when current row data is stored. Snapshot existence
      // checks load IDs only and do not need the size query.
      //
      // A previous attempt batched by MongoDB query results (roughly 48 MB),
      // but that changes source operation order. Loading sizes first takes an
      // extra query and lets OperationBatch retain source order while bounding
      // the current-data reads. The per-table flag refines the source setting.
      if (this.storeCurrentData && !this.skipExistingRows) {
        const lookups = input.batch
          .filter((op) => op.record.sourceTable.storeCurrentData)
          .map((op) => ({ sourceTableId: mongoTableId(op.record.sourceTable.id), replicaId: op.beforeId }));
        if (lookups.length > 0) {
          sizes = await this.sourceRecordStore.loadSizes(session, lookups);
          for (const [key, value] of state) {
            sizes.set(key, value?.data?.length() ?? 0);
          }
        }
      }
      for (const records of input.batched(sizes)) {
        const loaded = await this.sourceRecordStore.loadDocuments(
          session,
          records.map((op) => ({
            sourceTableId: mongoTableId(op.record.sourceTable.id),
            replicaId: op.beforeId
          })),
          this.skipExistingRows
        );
        for (const op of records) {
          this.pipeline.check();
          const before = state.has(op.internalBeforeKey)
            ? state.get(op.internalBeforeKey)!
            : (loaded.get(op.internalBeforeKey) ?? null);
          const after = await this.applyRow(context, (row, sequence) => this.applyOperation(row, op, before, sequence));
          const group = context.group!;
          if (after != null) {
            state.set(op.internalAfterKey!, after);
            group.changes.set(op.internalAfterKey!, after);
            loaded.set(op.internalAfterKey!, after);
            sizes?.set(op.internalAfterKey!, after.data?.length() ?? 0);
          }
          if (op.afterId == null || !storage.replicaIdEquals(op.beforeId, op.afterId)) {
            // A tombstone still counts as existing during a resumed snapshot.
            const deleted: LoadedSourceRecord = {
              sourceTableId: mongoTableId(op.record.sourceTable.id),
              replicaId: op.beforeId,
              cacheKey: op.internalBeforeKey,
              data: null,
              buckets: [],
              lookups: []
            };
            const membership = this.sourceRecordStore.retainsDeletes ? deleted : null;
            state.set(op.internalBeforeKey, membership);
            group.changes.set(op.internalBeforeKey, membership);
            loaded.delete(op.internalBeforeKey);
          }
          if (group.batch.shouldPublish()) {
            context.group = undefined;
            const receipt = await this.pipeline.submit(group);
            if (this.eagerPublication) {
              await receipt.persisted;
            }
          }
        }
      }
    }, before);
  }

  async applyRow<T>(
    context: PipelineContext,
    apply: (row: PersistedBatch, sequence: MongoIdSequence) => T
  ): Promise<T> {
    const allocator = this.allocator;
    await allocator.ensureCapacity();
    for (;;) {
      this.pipeline.check();
      const sequence = allocator.sequence(context.lastOp);
      const row = this.createPersistedBatch(0);
      let result: T;
      try {
        result = apply(row, sequence);
        if (row.currentSize > 0 && sequence.last() === context.lastOp) {
          // Membership-only changes must also advance the prepared prefix.
          sequence.next();
        }
      } catch (error) {
        if (!(error instanceof OpIdRangeExhausted)) {
          throw error;
        }
        // This row has not touched the group or overlay. Reuse its reserved IDs
        // after extending the range; earlier rows and uploads remain immutable.
        await allocator.reserve();
        continue;
      }
      context.lastOp = sequence.last();
      // Consume now, before yielding: other writers sharing this allocator must
      // never use IDs assigned to unpublished operations.
      allocator.consume(context.lastOp);
      const group = (context.group ??= { batch: this.createPersistedBatch(0), changes: new Map() });
      group.batch.append(row);
      return result;
    }
  }
}
