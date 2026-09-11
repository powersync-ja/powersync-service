import { storage } from '@powersync/service-core';
import { mongoTableId } from '../../utils/util.js';
import { PersistedBatch } from './common/PersistedBatch.js';
import { LoadedSourceRecord, SourceRecordStore } from './common/SourceRecordStore.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { MongoPublicationWriter } from './MongoReplicationPipeline.js';
import { OperationBatch, RecordOperation } from './OperationBatch.js';

/** Applies one writer's input using the stream's shared membership and ID order. */
export class MongoReplicationApplication {
  constructor(
    private readonly pipeline: MongoPublicationWriter,
    private readonly sourceRecordStore: SourceRecordStore,
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
    await this.pipeline.prepare(async (application) => {
      const { session } = application;
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
          application.overlaySizes(sizes);
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
          const before = application.lookup(op.internalBeforeKey, loaded.get(op.internalBeforeKey));
          const after = await application.applyRow((row, sequence) => this.applyOperation(row, op, before, sequence));
          if (after != null) {
            application.recordMembership(op.internalAfterKey!, after);
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
            application.recordMembership(op.internalBeforeKey, membership);
            loaded.delete(op.internalBeforeKey);
          }
          await application.publishIfFull(this.eagerPublication);
        }
      }
    }, before);
  }
}
