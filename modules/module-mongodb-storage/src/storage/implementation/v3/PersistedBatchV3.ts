import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionMapping, InternalOpId, storage } from '@powersync/service-core';
import { BucketDataSource, BucketDefinitionId } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import {
  BucketStateUpdate,
  PersistedBatch,
  PersistedBatchOptions,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from '../common/PersistedBatch.js';
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
import { ObjectStorageLifecycle, PreparedObjectStorageUpload } from './object-storage/ObjectStorageLifecycle.js';
import { ObjectStorageUsage } from './object-storage/ObjectStorageUsage.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

interface SourceRecordWrite {
  sourceTableId: bson.ObjectId;
  operation: mongo.AnyBulkWriteOperation<CurrentDataDocumentV3>;
}

export class PersistedBatchV3 extends PersistedBatch {
  // Upserts and soft deletes supply the complete source-record state, including
  // pending_delete. Keep the final state per key, without removing any history.
  private metadataSize = 0;
  currentData = new Map<string, SourceRecordWrite>();
  sourceTablePendingDeletes = new Map<string, InternalOpId>();
  protected readonly objectStorageLifecycle?: ObjectStorageLifecycle;
  protected readonly objectStorageUsage?: ObjectStorageUsage;

  declare protected readonly db: VersionedPowerSyncMongoV3;

  constructor(
    db: VersionedPowerSyncMongoV3,
    group_id: number,
    mapping: BucketDefinitionMapping,
    writtenSize: number,
    options?: PersistedBatchOptions
  ) {
    super(db, group_id, mapping, writtenSize, options);
    if (this.objectStorage) {
      this.objectStorageLifecycle = new ObjectStorageLifecycle(this.db, this.group_id, this.objectStorage);
      this.objectStorageUsage = new ObjectStorageUsage(this.db, this.group_id, this.objectStorageUsageWriterId);
    }
  }

  // Abstract override from PersistedBatch (V3-specific error message)

  protected override checkDefinitionId(definitionId: BucketDefinitionId | null): BucketDefinitionId {
    if (definitionId == null) {
      throw new ReplicationAssertionError('Expected v3 bucket when incrementalReprocessing is enabled');
    }
    return definitionId;
  }

  // Concrete implementations from PersistedBatchShared

  protected getBucketDefinitionId(bucketSource: BucketDataSource): BucketDefinitionId {
    return this.mapping.bucketSourceId(bucketSource);
  }

  saveParameterData(data: SaveParameterDataOptions) {
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
      this.debugLastOpId = op_id;
      const values = {
        _id: op_id,
        key: {
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup: binLookup,
        bucket_parameters: result.bucketParameters
      };
      this.bucketParameters.push({
        ...values,
        index: sourceDefinitionId
      });

      const size = bson.calculateObjectSize(values);
      this.currentSize += size;
      this.metadataSize += size;
    }

    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
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
      this.bucketParameters.push({
        ...values,
        index: indexId
      });

      const size = bson.calculateObjectSize(values);
      this.currentSize += size;
      this.metadataSize += size;
    }
  }

  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId) {
    this.setCurrentData(replicaId, {
      sourceTableId,
      operation: {
        deleteOne: {
          filter: { _id: replicaId }
        }
      }
    });
  }

  softDeleteCurrentData(
    sourceTableId: bson.ObjectId,
    replicaId: storage.ReplicaId,
    checkpointGreaterThan: InternalOpId
  ) {
    this.setCurrentData(replicaId, {
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
  }

  upsertCurrentData(values: UpsertCurrentDataOptions) {
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

    this.setCurrentData(values.replicaId, {
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
  }

  private setCurrentData(replicaId: storage.ReplicaId, write: SourceRecordWrite) {
    this.currentData.set(this.currentDataKey(write.sourceTableId, replicaId), write);
    // Include identities and membership arrays; count repeated writes conservatively.
    const size = bson.calculateObjectSize(write.operation);
    this.currentSize += size;
    this.metadataSize += size;
  }

  /** Merge one fully evaluated row. Failed range allocations never mutate the group. */
  append(row: PersistedBatchV3): void {
    for (const operation of row.bucketData) {
      this.bucketData.push(operation);
    }
    for (const parameter of row.bucketParameters) {
      this.bucketParameters.push(parameter);
    }
    for (const [key, value] of row.currentData) {
      this.currentData.set(key, value);
    }
    for (const [key, value] of row.sourceTablePendingDeletes) {
      const previous = this.sourceTablePendingDeletes.get(key) ?? 0n;
      this.sourceTablePendingDeletes.set(key, value > previous ? value : previous);
    }
    for (const [key, value] of row.bucketStates) {
      const previous = this.bucketStates.get(key);
      this.bucketStates.set(
        key,
        previous == null
          ? value
          : {
              ...value,
              incrementCount: previous.incrementCount + value.incrementCount,
              incrementBytes: previous.incrementBytes + value.incrementBytes,
              incrementChunks: previous.incrementChunks + value.incrementChunks
            }
      );
    }
    this.currentSize += row.currentSize;
    this.metadataSize += row.metadataSize;
    this.debugLastOpId = row.debugLastOpId ?? this.debugLastOpId;
  }

  protected get currentDataCount() {
    return this.currentData.size;
  }

  // Flush methods

  override shouldPublish() {
    // Input blocks bound source-row preparation. Publication has separate limits
    // for payload bytes, BSON metadata, metadata work and high-fanout bucket ops.
    // A single source row is indivisible and may exceed a target.
    return (
      this.currentSize >= 24 * 1024 * 1024 ||
      this.metadataSize >= 8 * 1024 * 1024 ||
      this.currentDataCount +
        this.bucketParameters.length +
        2 * this.bucketStates.size +
        this.sourceTablePendingDeletes.size >=
        16_000 ||
      this.bucketDataCount >= 64_000
    );
  }

  private preparation?: Promise<void>;
  private preparedWrites: { definitionId: BucketDefinitionId; documents: BucketDataDocumentV3[] }[] = [];
  private preparedUploads: PreparedObjectStorageUpload[] = [];

  override prepare(onUploadError?: (error: unknown) => void): Promise<void> {
    return (this.preparation ??= this.prepareBucketData(onUploadError));
  }

  private async prepareBucketData(onUploadError?: (error: unknown) => void): Promise<void> {
    const uploads: { path: string; document: BucketDataDocumentV3 }[] = [];
    const lifecycle = this.objectStorageLifecycle;
    const byDefinition = Map.groupBy(this.bucketData, (document) => document.bucketKey.definitionId);
    this.preparedWrites = Array.from(byDefinition, ([definitionId, operations]) => {
      const documents: BucketDataDocumentV3[] = [];
      for (const [bucket, ops] of Map.groupBy(operations, (document) => document.bucketKey.bucket)) {
        this.resetBucketPersistedBytes(definitionId, bucket);
        for (const chunk of chunkBucketData(ops)) {
          const document = serializeBucketData(bucket, chunk);
          this.incrementBucketPersistedChunk(definitionId, bucket, document.size);
          documents.push(document);
          if (lifecycle != null && document.size > this.inlineThresholdBytes) {
            uploads.push({
              path: lifecycle.allocatePath(definitionId, bucket, chunk[0].o, chunk[chunk.length - 1].o),
              document
            });
          }
        }
      }
      return { definitionId, documents };
    });
    if (lifecycle == null || uploads.length === 0) {
      return;
    }

    // Persist orphan markers before starting any PUT, including PUTs whose response
    // may be lost. Publication removes these in the reference-creating transaction.
    this.preparedUploads = await lifecycle.prepareUploads(uploads.map((upload) => upload.path));
    // Object storage supplies the shared request limiter. Drain every started PUT
    // on failure so disposal cannot release write access while uploads run.
    const results = await Promise.allSettled(
      uploads.map(async ({ path, document }) => {
        try {
          const { fileSize } = await lifecycle.bucketData.store(path, document.ops!, { signal: this.signal });
          document.storage_ref = { path, file_size: fileSize };
          delete document.ops;
        } catch (error) {
          // Notify immediately so the pipeline can stop admission and cancel
          // sibling uploads, while still joining every started request below.
          onUploadError?.(error);
          throw error;
        }
      })
    );
    for (const result of results) {
      if (result.status === 'rejected') {
        throw result.reason;
      }
    }
  }

  protected async queueBucketData(writes: MongoWriteBatch) {
    await this.prepare();
    const usageDeltas = new Map<BucketDefinitionId, bigint>();
    for (const { definitionId, documents } of this.preparedWrites) {
      writes.bulkWriteUnordered(
        this.db.bucketData(this.group_id, definitionId),
        documents.map((document) => ({ insertOne: { document } }))
      );
      const delta = documents.reduce((sum, document) => sum + ObjectStorageUsage.bytes(document), 0n);
      if (delta !== 0n) {
        usageDeltas.set(definitionId, delta);
      }
    }
    this.objectStorageUsage?.applyDeltas(usageDeltas, writes);
    this.objectStorageLifecycle?.publishUploads(this.preparedUploads, writes);
  }

  protected queueBucketParameters(writes: MongoWriteBatch): void {
    const operationsByIndex = new Map<string, typeof this.bucketParameters>();
    for (const document of this.bucketParameters) {
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

  protected queueCurrentData(writes: MongoWriteBatch): void {
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

  protected queueBucketStates(writes: MongoWriteBatch): void {
    writes.bulkWriteUnordered(this.db.bucketState(this.group_id), this.getBucketStateUpdates());
  }

  protected resetCurrentData() {
    this.metadataSize = 0;
    this.preparation = undefined;
    this.preparedWrites = [];
    this.preparedUploads = [];
    this.currentData.clear();
    this.sourceTablePendingDeletes.clear();
  }

  private currentDataKey(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): string {
    return Buffer.from(bson.serialize({ t: sourceTableId, k: replicaId })).toString('base64');
  }

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<BucketStateDocumentV3>[] {
    return Array.from(this.bucketStates.values()).map((state: BucketStateUpdate) => {
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
