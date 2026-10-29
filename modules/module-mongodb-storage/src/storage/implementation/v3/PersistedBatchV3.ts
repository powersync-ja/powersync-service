import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionMapping, InternalOpId, storage } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { BatchOperationBuffer } from '../common/BatchOperationBuffer.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import {
  BucketStateUpdate,
  PersistedBatch,
  PersistedBatchOptions,
  SaveBucketDataOptions,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from '../common/PersistedBatch.js';
import { PreparedPublication } from '../common/PreparedPublication.js';
import { SourceRecordLookupState } from '../common/SourceRecordStore.js';
import { MongoWriteBatch } from '../MongoWriteBatch.js';
import { serializeBucketData } from './bucket-format.js';
import { chunkBucketData } from './chunking.js';
import { DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS } from './compaction-constants.js';
import {
  BucketDataDocumentV3,
  BucketStateDocumentV3,
  CurrentDataDocumentV3,
  serializeParameterLookup,
  SourceTableDocumentV3,
  taggedBucketParameterDocumentToTagged
} from './models.js';
import { ObjectStorage } from './object-storage/ObjectStorage.js';
import { ObjectStorageLifecycle, PreparedObjectStorageUpload } from './object-storage/ObjectStorageLifecycle.js';
import { ObjectStorageUsage } from './object-storage/ObjectStorageUsage.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export const DEFAULT_INLINE_THRESHOLD_BYTES = 16 * 1024;

export interface PersistedBatchV3Options extends PersistedBatchOptions {
  objectStorage?: ObjectStorage;
  inlineThresholdBytes?: number;
  /**
   * Aborts in-flight object storage uploads when replication stops.
   */
  signal?: AbortSignal;
  objectStorageUsageWriterId?: string;
}

interface SourceRecordWrite {
  sourceTableId: bson.ObjectId;
  operation: mongo.AnyBulkWriteOperation<CurrentDataDocumentV3>;
}

export class PersistedBatchV3 implements PersistedBatch, PreparedPublication {
  // Upserts and soft deletes supply the complete source-record state, including
  // pending_delete. Keep the final state per key, without removing any history.
  private readonly currentData = new Map<string, SourceRecordWrite>();
  private readonly sourceTablePendingDeletes = new Map<string, InternalOpId>();
  private readonly objectStorageLifecycle?: ObjectStorageLifecycle;
  private readonly objectStorageUsage?: ObjectStorageUsage;

  private readonly operations: BatchOperationBuffer;
  private readonly inlineThresholdBytes: number;
  private readonly signal?: AbortSignal;
  private sealed = false;
  private preparation?: Promise<void>;
  private payloadsReady = false;

  constructor(
    private readonly db: VersionedPowerSyncMongoV3,
    private readonly group_id: number,
    private readonly mapping: BucketDefinitionMapping,
    writtenSize: number,
    options?: PersistedBatchV3Options
  ) {
    this.operations = new BatchOperationBuffer(
      group_id,
      writtenSize,
      {
        // V3-specific definition validation; v1 does not persist definition ids.
        checkDefinitionId: (definitionId) => {
          if (definitionId == null) {
            throw new ReplicationAssertionError('Expected v3 bucket when incrementalReprocessing is enabled');
          }
          return definitionId;
        },
        getBucketDefinitionId: (source) => mapping.bucketSourceId(source)
      },
      options?.logger
    );
    this.inlineThresholdBytes = options?.inlineThresholdBytes ?? DEFAULT_INLINE_THRESHOLD_BYTES;
    this.signal = options?.signal;
    if (options?.objectStorage) {
      this.objectStorageLifecycle = new ObjectStorageLifecycle(db, group_id, options.objectStorage);
      this.objectStorageUsage = new ObjectStorageUsage(db, group_id, options.objectStorageUsageWriterId);
    }
  }

  get currentSize(): number {
    return this.operations.currentSize;
  }

  /**
   * BSON bytes plus conservative headroom for JS objects, membership maps,
   * packed chunks and upload buffers. Separate from transaction-size targets.
   * This estimate intentionally charges superseded representations until commit.
   */
  get estimatedRetainedBytes(): number {
    let bytes = 0;
    for (const operation of this.operations.bucketData) {
      bytes += bson.calculateObjectSize(operation) + 256;
    }
    for (const parameter of this.operations.bucketParameters) {
      bytes += bson.calculateObjectSize(parameter) + 256;
    }
    for (const entry of this.currentData.values()) {
      bytes += bson.calculateObjectSize(entry.operation) + 256;
    }
    for (const state of this.operations.bucketStates.values()) {
      bytes += bson.calculateObjectSize(state) + 256;
    }
    return bytes * 4;
  }

  saveBucketData(options: SaveBucketDataOptions): void {
    this.assertMutable();
    this.operations.saveBucketData(options);
  }

  shouldFlushTransaction(): boolean {
    return this.operations.shouldFlushTransaction(this.currentData.size);
  }

  append(row: PersistedBatchV3): void {
    this.assertMutable();
    this.operations.append(row.operations);
    for (const [key, value] of row.currentData) {
      this.currentData.set(key, value);
    }
    for (const [key, value] of row.sourceTablePendingDeletes) {
      const previous = this.sourceTablePendingDeletes.get(key) ?? 0n;
      this.sourceTablePendingDeletes.set(key, value > previous ? value : previous);
    }
  }

  /** Return the sealed view of this batch without starting I/O or moving its data. */
  seal(): PreparedPublication {
    this.assertMutable();
    this.sealed = true;
    return this;
  }

  private assertMutable(): void {
    if (this.sealed) {
      throw new Error('Cannot modify a sealed publication');
    }
  }

  /** Cache readiness, including failure; the v3 batch owns the entire lifecycle. */
  preparePayloads(): Promise<void> {
    if (!this.sealed) {
      throw new ReplicationAssertionError('Cannot prepare an unsealed publication');
    }
    return (this.preparation ??= Promise.resolve()
      .then(() => this.preparePayloadsImpl())
      .then(() => {
        this.payloadsReady = true;
      }));
  }

  async publish(
    session: mongo.ClientSession,
    options?: storage.BucketBatchCommitOptions,
    additionalWrites?: MongoWriteBatch
  ) {
    // Never start preparation from a transaction attempt. The eager coordinator
    // awaits readiness for the entire bounded prefix before opening a transaction.
    if (!this.payloadsReady) {
      throw new ReplicationAssertionError('Prepare publication payloads before starting the transaction');
    }
    return this.write(session, options, additionalWrites);
  }

  private async write(
    session: mongo.ClientSession,
    options?: storage.BucketBatchCommitOptions,
    additionalWrites?: MongoWriteBatch
  ) {
    const startAt = performance.now();
    const writes = additionalWrites ?? this.db.createWriteBatch(session, { ordered: false });
    // Checked marker removal, references and usage must commit together. Recheck
    // expiry on every attempt; neither an expired nor a missing marker is a
    // transient MongoDB error that may replay this generation indefinitely.
    this.objectStorageLifecycle?.publishUploads(this.preparedUploads, writes);
    if (this.operations.bucketDataCount > 0) {
      await this.queueBucketData(writes);
    }
    if (this.operations.bucketParameters.length > 0) {
      this.queueBucketParameters(writes);
    }
    if (this.currentData.size > 0) {
      this.queueCurrentData(writes);
    }
    if (this.operations.bucketStates.size > 0) {
      this.queueBucketStates(writes);
    }
    await writes.execute();
    return this.operations.logFlush(startAt, this.currentData.size, options);
  }

  saveParameterData(data: SaveParameterDataOptions) {
    this.assertMutable();
    const { sourceTable, sourceKey, evaluated } = data;
    const remaining_lookups = new Map<string, SourceRecordLookupState>();

    for (let lookup of data.existing_lookups) {
      if (lookup.indexId == null) {
        throw new ReplicationAssertionError('Expected lookup when incrementalReprocessing is enabled');
      }
      if (sourceTable.parameterLookupSourceIds != null && !sourceTable.parameterLookupSourceIds.has(lookup.indexId)) {
        // The parameter index is not active anymore.
        // We don't cleanup these references upfront, but do need to ignore them after the definition is removed.
        continue;
      }
      remaining_lookups.set(`${lookup.indexId}.${lookup.lookup.toString('base64')}`, lookup);
    }

    for (let result of evaluated) {
      const sourceDefinitionId = this.mapping.parameterLookupId(result.lookup.source);
      const binLookup = serializeParameterLookup(result.lookup);
      remaining_lookups.delete(`${sourceDefinitionId}.${binLookup.toString('base64')}`);

      const op_id = data.op_seq.next();
      this.operations.debugLastOpId = op_id;
      const values = {
        _id: op_id,
        key: {
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup: binLookup,
        bucket_parameters: result.bucketParameters
      };
      this.operations.bucketParameters.push({
        ...values,
        index: sourceDefinitionId
      });

      this.operations.currentSize += 200;
    }

    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.operations.debugLastOpId = op_id;
      const indexId = lookup.indexId;
      if (indexId == null) {
        throw new ReplicationAssertionError('Expected lookup when incrementalReprocessing is enabled');
      }
      const values = {
        _id: op_id,
        key: {
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup: lookup.lookup,
        bucket_parameters: []
      };
      this.operations.bucketParameters.push({
        ...values,
        index: indexId
      });

      this.operations.currentSize += 200;
    }
  }

  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId) {
    this.assertMutable();
    this.currentData.set(this.currentDataKey(sourceTableId, replicaId), {
      sourceTableId,
      operation: {
        deleteOne: {
          filter: { _id: replicaId }
        }
      }
    });
    this.operations.currentSize += 50;
  }

  softDeleteCurrentData(
    sourceTableId: bson.ObjectId,
    replicaId: storage.ReplicaId,
    checkpointGreaterThan: InternalOpId
  ) {
    this.assertMutable();
    this.currentData.set(this.currentDataKey(sourceTableId, replicaId), {
      sourceTableId,
      operation: {
        updateOne: {
          filter: { _id: replicaId },
          update: {
            $set: {
              data: null,
              buckets: [],
              lookups: [],
              pending_delete: checkpointGreaterThan
            }
          },
          upsert: true
        }
      }
    });
    const sourceTableKey = sourceTableId.toHexString();
    const existingPendingDelete = this.sourceTablePendingDeletes.get(sourceTableKey);
    if (existingPendingDelete == null || checkpointGreaterThan > existingPendingDelete) {
      this.sourceTablePendingDeletes.set(sourceTableKey, checkpointGreaterThan);
    }

    this.operations.currentSize += 50;
  }

  upsertCurrentData(values: UpsertCurrentDataOptions) {
    this.assertMutable();
    const buckets = values.buckets.map((bucket) => {
      if (bucket.definitionId == null) {
        throw new ReplicationAssertionError('Expected bucket when incrementalReprocessing is enabled');
      }
      return {
        def: bucket.definitionId,
        bucket: bucket.bucket,
        table: bucket.table,
        id: bucket.id
      };
    });
    const lookups = values.lookups.map((lookup) => {
      if (lookup.indexId == null) {
        throw new ReplicationAssertionError('Expected lookup when incrementalReprocessing is enabled');
      }
      return {
        i: lookup.indexId,
        l: lookup.lookup
      };
    });

    this.currentData.set(this.currentDataKey(values.sourceTableId, values.replicaId), {
      sourceTableId: values.sourceTableId,
      operation: {
        updateOne: {
          filter: { _id: values.replicaId },
          update: {
            $set: {
              data: values.data,
              buckets,
              lookups
            },
            $unset: { pending_delete: 1 }
          },
          upsert: true
        }
      }
    });
    this.operations.currentSize += (values.data?.length() ?? 0) + 100;
  }

  private preparedBucketWrites?: { definitionId: BucketDefinitionId; documents: BucketDataDocumentV3[] }[];
  private readonly preparedUsageDeltas = new Map<BucketDefinitionId, bigint>();
  private preparedUploads: PreparedObjectStorageUpload[] = [];

  private async preparePayloadsImpl(): Promise<void> {
    const operationsByDefinition = new Map<BucketDefinitionId, BucketDataDoc[]>();
    for (const document of this.operations.bucketData) {
      const existing = operationsByDefinition.get(document.bucketKey.definitionId) ?? [];
      existing.push(document);
      operationsByDefinition.set(document.bucketKey.definitionId, existing);
    }

    const uploads: { path: string; payload: Uint8Array }[] = [];
    this.preparedBucketWrites = Array.from(operationsByDefinition, ([definitionId, documents]) => {
      const operationsByBucket = Map.groupBy(documents, (document) => document.bucketKey.bucket);
      const lifecycle = this.objectStorageLifecycle;
      const preparedDocuments: BucketDataDocumentV3[] = [];

      for (const [bucket, ops] of operationsByBucket) {
        this.operations.resetBucketPersistedBytes(definitionId, bucket);
        for (const chunk of chunkBucketData(ops)) {
          const serialized = serializeBucketData(bucket, chunk);
          this.operations.incrementBucketPersistedChunk(definitionId, bucket, serialized.size);
          if (lifecycle == null || serialized.size <= this.inlineThresholdBytes) {
            preparedDocuments.push(serialized);
            continue;
          }

          const minOp = chunk[0].o;
          const maxOp = chunk[chunk.length - 1].o;
          const { ops: bucketOps, ...metadata } = serialized;
          const path = lifecycle.allocatePath(definitionId, bucket, minOp, maxOp);
          // Seal paths and bytes before starting any upload. Publication reads
          // only the resulting metadata and never regenerates an object.
          const payload = bson.serialize({ ops: bucketOps! });
          preparedDocuments.push({
            ...metadata,
            storage_ref: { path, file_size: payload.byteLength }
          });
          uploads.push({ path, payload });
        }
      }

      const delta = preparedDocuments.reduce((sum, document) => sum + ObjectStorageUsage.bytes(document), 0n);
      if (delta !== 0n) {
        this.preparedUsageDeltas.set(definitionId, delta);
      }
      return { definitionId, documents: preparedDocuments };
    });

    const lifecycle = this.objectStorageLifecycle;
    if (uploads.length === 0 || lifecycle == null) {
      return;
    }
    this.signal?.throwIfAborted();
    // Seal every path/byte first, then register the whole group durably. No PUT
    // may start after a partial or ambiguous registration failure.
    this.preparedUploads = await lifecycle.prepareUploads(
      uploads.map((upload) => upload.path),
      new Date()
    );
    this.signal?.throwIfAborted();
    await lifecycle.storeUploads(
      uploads.map(({ payload }, index) => ({ upload: this.preparedUploads[index], payload })),
      { signal: this.signal }
    );
  }

  private async queueBucketData(writes: MongoWriteBatch) {
    if (this.preparedBucketWrites == null) {
      throw new ReplicationAssertionError('Publication payloads have not been prepared');
    }
    for (const { definitionId, documents } of this.preparedBucketWrites) {
      writes.bulkWriteUnordered(
        this.db.bucketData(this.group_id, definitionId),
        documents.map((document) => ({ insertOne: { document } }))
      );
    }
    this.objectStorageUsage?.applyDeltas(this.preparedUsageDeltas, writes);
  }

  private queueBucketParameters(writes: MongoWriteBatch): void {
    const operationsByIndex = new Map<string, typeof this.operations.bucketParameters>();
    for (const document of this.operations.bucketParameters) {
      const existing = operationsByIndex.get(document.index) ?? [];
      existing.push(document);
      operationsByIndex.set(document.index, existing);
    }

    for (const [indexId, documents] of operationsByIndex.entries()) {
      writes.bulkWriteUnordered(
        this.db.parameterIndex(this.group_id, indexId),
        documents.map((document) => ({
          insertOne: {
            document: taggedBucketParameterDocumentToTagged(document)
          }
        }))
      );
    }
  }

  private queueCurrentData(writes: MongoWriteBatch): void {
    const operationsBySourceTable = new Map<string, SourceRecordWrite[]>();
    for (const operation of this.currentData.values()) {
      const sourceTableId = operation.sourceTableId.toHexString();
      const existing = operationsBySourceTable.get(sourceTableId) ?? [];
      existing.push(operation);
      operationsBySourceTable.set(sourceTableId, existing);
    }

    const sourceTableUpdates: mongo.AnyBulkWriteOperation<SourceTableDocumentV3>[] = [
      ...this.sourceTablePendingDeletes.entries()
    ].map(([key, value]) => {
      return {
        updateOne: {
          filter: { _id: new bson.ObjectId(key) },
          update: {
            $max: {
              latest_pending_delete: value
            }
          }
        }
      };
    });

    if (sourceTableUpdates.length > 0) {
      writes.bulkWriteUnordered(this.db.sourceTables(this.group_id), sourceTableUpdates);
    }

    for (const operations of operationsBySourceTable.values()) {
      const sourceTableId = operations[0]!.sourceTableId;
      writes.bulkWriteUnordered(
        this.db.sourceRecords(this.group_id, sourceTableId),
        operations.map((entry) => entry.operation)
      );
    }
  }

  private queueBucketStates(writes: MongoWriteBatch): void {
    writes.bulkWriteUnordered(this.db.bucketState(this.group_id), this.getBucketStateUpdates());
  }

  private currentDataKey(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): string {
    return Buffer.from(bson.serialize({ t: sourceTableId, k: replicaId })).toString('base64');
  }

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<BucketStateDocumentV3>[] {
    return Array.from(this.operations.bucketStates.values()).map((state: BucketStateUpdate) => {
      if (state.definitionId == null) {
        throw new ReplicationAssertionError('Expected bucket definition id when incrementalReprocessing is enabled');
      }
      return {
        updateOne: {
          filter: {
            _id: {
              d: state.definitionId,
              b: state.bucket
            }
          },
          // A pipeline update makes initialisation and scheduling one atomic
          // writer operation. In particular, a later write cannot move an
          // already-due compact check into the future.
          update: [
            {
              $set: {
                last_op: state.lastOp,
                bucket_stats: {
                  count: { $add: [{ $ifNull: ['$bucket_stats.count', 0] }, state.incrementCount] },
                  bytes: { $add: [{ $ifNull: ['$bucket_stats.bytes', 0n] }, BigInt(state.incrementBytes)] },
                  chunks: { $add: [{ $ifNull: ['$bucket_stats.chunks', 0] }, state.incrementChunks] }
                },
                first_uncompacted_write: { $ifNull: ['$first_uncompacted_write', '$$NOW'] },
                next_compact_check: {
                  $let: {
                    vars: {
                      requested: {
                        $dateAdd: {
                          startDate: '$$NOW',
                          unit: 'millisecond',
                          amount: DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS
                        }
                      }
                    },
                    in: {
                      $cond: [
                        { $lt: [{ $ifNull: ['$next_compact_check', '$$requested'] }, '$$requested'] },
                        '$next_compact_check',
                        '$$requested'
                      ]
                    }
                  }
                }
              }
            }
          ],
          upsert: true
        }
      } satisfies mongo.AnyBulkWriteOperation<BucketStateDocumentV3>;
    });
  }
}
