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
  SourceRecordInsertConflict,
  SourceRecordSnapshotConflict,
  UpsertCurrentDataOptions
} from '../common/PersistedBatch.js';
import { SourceRecordLookupState } from '../common/SourceRecordStore.js';
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

export class PersistedBatchV3 extends PersistedBatch {
  private metadataSize = 0;
  currentData: {
    sourceTableId: bson.ObjectId;
    operation: mongo.AnyBulkWriteOperation<CurrentDataDocumentV3>;
    skipExistingOnConflict?: boolean;
  }[] = [];
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

      const size = bson.calculateObjectSize(values) + 32;
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

      const size = bson.calculateObjectSize(values) + 32;
      this.currentSize += size;
      this.metadataSize += size;
    }
  }

  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId) {
    this.currentData.push({
      sourceTableId,
      operation: {
        deleteOne: {
          filter: { _id: replicaId }
        }
      }
    });
    this.accountCurrentData();
  }

  softDeleteCurrentData(
    sourceTableId: bson.ObjectId,
    replicaId: storage.ReplicaId,
    checkpointGreaterThan: InternalOpId
  ) {
    this.currentData.push({
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

    this.accountCurrentData();
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

    this.currentData.push({
      sourceTableId: values.sourceTableId,
      skipExistingOnConflict: values.skipExistingOnConflict,
      operation: values.assumeNew
        ? {
            insertOne: { document: { _id: values.replicaId, data: values.data, buckets, lookups } }
          }
        : {
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
    this.accountCurrentData();
  }

  private accountCurrentData() {
    const size = bson.calculateObjectSize(this.currentData.at(-1)!.operation);
    this.currentSize += size;
    this.metadataSize += size;
  }

  override shouldPublish(): boolean {
    // Preparation keeps its small input/read limits. Publication can combine
    // several blocks into useful per-bucket objects, without counting every
    // offloaded bucket operation as a separate MongoDB write. These are soft
    // limits checked after each source row, as with the legacy transaction limit.
    return (
      this.currentSize >= 24 * 1024 * 1024 ||
      this.metadataSize >= 8 * 1024 * 1024 ||
      this.currentDataCount + this.bucketParameters.length + 2 * this.bucketStates.size >= 16_000 ||
      this.bucketDataCount >= 64_000
    );
  }

  protected get currentDataCount() {
    return this.currentData.length;
  }

  // Flush methods

  private preparation?: Promise<void>;
  private preparedWrites?: { definitionId: BucketDefinitionId; documents: BucketDataDocumentV3[] }[];
  private preparedUploads: PreparedObjectStorageUpload[] = [];

  override prepare(): Promise<void> {
    return (this.preparation ??= this.prepareBucketData());
  }

  private async prepareBucketData(): Promise<void> {
    using packing = storage.ReplicationDiagnostics.active?.span('publication.packing_sync');
    const byDefinition = Map.groupBy(this.bucketData, (document) => document.bucketKey.definitionId);
    const uploads: { path: string; document: BucketDataDocumentV3 }[] = [];
    this.preparedWrites = Array.from(byDefinition, ([definitionId, operations]) => {
      const documents: BucketDataDocumentV3[] = [];
      for (const [bucket, ops] of Map.groupBy(operations, (document) => document.bucketKey.bucket)) {
        this.resetBucketPersistedBytes(definitionId, bucket);
        for (const chunk of chunkBucketData(ops)) {
          const document = serializeBucketData(bucket, chunk);
          this.incrementBucketPersistedChunk(definitionId, bucket, document.size);
          documents.push(document);
          if (this.objectStorageLifecycle != null && document.size > this.inlineThresholdBytes) {
            uploads.push({
              path: this.objectStorageLifecycle.allocatePath(definitionId, bucket, chunk[0].o, chunk.at(-1)!.o),
              document
            });
          }
        }
      }
      return { definitionId, documents };
    });

    packing?.end();
    if (uploads.length === 0) return;
    const lifecycle = this.objectStorageLifecycle!;
    // Markers must predate the publication transaction and survive its rollback.
    using markers = storage.ReplicationDiagnostics.active?.span('storage.upload_markers');
    this.preparedUploads = await lifecycle.prepareUploads(uploads.map((upload) => upload.path));
    markers?.end();
    // The object storage applies its shared request limit. Settle every request
    // before returning an error, including PUTs completing after another failed.
    const results = await Promise.allSettled(
      uploads.map(async ({ path, document }) => {
        using upload = storage.ReplicationDiagnostics.active?.span('publication.s3_upload');
        const { fileSize } = await lifecycle.bucketData.store(path, document.ops!, { signal: this.signal });
        delete document.ops;
        document.storage_ref = { path, file_size: fileSize };
      })
    );
    for (const result of results) {
      if (result.status === 'rejected') throw result.reason;
    }
  }

  protected async flushBucketData(session: mongo.ClientSession) {
    if (this.preparation == null && this.objectStorage != null && session.inTransaction()) {
      throw new ReplicationAssertionError('S3 replication payloads must be prepared before the transaction');
    }
    await this.prepare();
    const usageDeltas = new Map<BucketDefinitionId, bigint>();
    for (const { definitionId, documents } of this.preparedWrites!) {
      if (documents.length === 0) continue;
      using insert = storage.ReplicationDiagnostics.active?.span('transaction.bucket_data.insert');
      await this.db.bucketData(this.group_id, definitionId).bulkWrite(
        documents.map((document) => ({ insertOne: { document } })),
        { session, ordered: false }
      );
      insert?.end();
      const delta = documents.reduce((sum, document) => sum + ObjectStorageUsage.bytes(document), 0n);
      if (delta !== 0n) usageDeltas.set(definitionId, delta);
    }
    if (this.objectStorageLifecycle) {
      using timing = storage.ReplicationDiagnostics.active?.span('transaction.bucket_data.publish_uploads');
      await this.objectStorageLifecycle.publishUploads(this.preparedUploads, session);
    }
    if (this.objectStorageUsage) {
      using timing = storage.ReplicationDiagnostics.active?.span('transaction.bucket_data.usage');
      await this.objectStorageUsage.applyDeltas(usageDeltas, session);
    }
  }

  protected async flushBucketParameters(session: mongo.ClientSession) {
    const operationsByIndex = new Map<string, typeof this.bucketParameters>();
    for (const document of this.bucketParameters) {
      const existing = operationsByIndex.get(document.index) ?? [];
      existing.push(document);
      operationsByIndex.set(document.index, existing);
    }

    for (const [indexId, documents] of operationsByIndex.entries()) {
      await this.db.parameterIndex(this.group_id, indexId).bulkWrite(
        documents.map((document) => ({
          insertOne: {
            document: taggedBucketParameterDocumentToTagged(document)
          }
        })),
        {
          session,
          ordered: false
        }
      );
    }
  }

  protected async flushCurrentData(session: mongo.ClientSession) {
    const operationsBySourceTable = new Map<string, typeof this.currentData>();
    for (const operation of this.currentData) {
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
      using timing = storage.ReplicationDiagnostics.active?.span('transaction.current_data.source_tables');
      await this.db.sourceTables(this.group_id).bulkWrite(sourceTableUpdates, { session, ordered: false });
    }

    for (const operations of operationsBySourceTable.values()) {
      using timing = storage.ReplicationDiagnostics.active?.span('transaction.current_data.membership_write');
      const sourceTableId = operations[0]!.sourceTableId;
      try {
        await this.db.sourceRecords(this.group_id, sourceTableId).bulkWrite(
          operations.map((entry) => entry.operation),
          { session, ordered: !this.canReorderMembershipWrites(operations) }
        );
      } catch (error) {
        // A duplicate aborts the transaction. Only retry conflicts from our
        // optimistic source-record inserts, not other writes or constraints.
        if (!(error instanceof mongo.MongoBulkWriteError)) throw error;
        const writeErrors = Array.isArray(error.writeErrors) ? error.writeErrors : [error.writeErrors];
        if (
          writeErrors.length === 0 ||
          !writeErrors.every((write) => {
            const operation = operations[write.index]?.operation;
            return write.code === 11000 && operation != null && 'insertOne' in operation;
          })
        ) {
          throw error;
        }
        if (operations.some((entry) => entry.skipExistingOnConflict)) {
          // Snapshot rows must not overwrite existing records or publish their
          // speculative bucket operations. Rebuild from source input instead.
          throw new SourceRecordSnapshotConflict('Retry snapshot with source-record lookups', { cause: error });
        }
        // Disable the assumption for the entire publication, including tables
        // already written in the now-aborted transaction. Preserve write order.
        for (const entry of this.currentData) {
          if (!('insertOne' in entry.operation)) continue;
          const document = entry.operation.insertOne.document;
          entry.operation = {
            updateOne: {
              filter: { _id: document._id },
              update: {
                $set: { data: document.data, buckets: document.buckets, lookups: document.lookups },
                $unset: { pending_delete: 1 }
              },
              upsert: true
            }
          };
        }
        throw new SourceRecordInsertConflict('Retry publication with membership upserts', { cause: error });
      }
    }
  }

  private canReorderMembershipWrites(operations: typeof this.currentData): boolean {
    // Ordered bulks split at every insert/update/delete transition. For mixed
    // CDC input this can mean thousands of sequential database round trips.
    // Reordering is safe only when every operation targets a different record.
    const keys = new Set<string>();
    for (const { operation } of operations) {
      const id: unknown =
        'insertOne' in operation
          ? operation.insertOne.document._id
          : 'updateOne' in operation
            ? operation.updateOne.filter._id
            : 'deleteOne' in operation
              ? operation.deleteOne.filter._id
              : undefined;
      let key: string;
      if (typeof id === 'string') key = `string:${id}`;
      else if (storage.isUUID(id)) key = `uuid:${id.toHexString()}`;
      else if (id != null && typeof id === 'object' && '_bsontype' in id && id._bsontype === 'ObjectId') {
        // ObjectIds may come from another installed copy of bson.
        key = `objectid:${(id as bson.ObjectId).toHexString()}`;
      }
      // BSON numeric representations can compare equal in MongoDB despite
      // differing encodings, including inside compound IDs. Be conservative.
      else return false;
      if (keys.has(key)) return false;
      keys.add(key);
    }
    return true;
  }

  protected async flushBucketStates(session: mongo.ClientSession) {
    await this.db.bucketState(this.group_id).bulkWrite(this.getBucketStateUpdates(), {
      session,
      ordered: false
    });
  }

  protected resetCurrentData() {
    this.metadataSize = 0;
    this.currentData = [];
    this.sourceTablePendingDeletes.clear();
    this.preparation = undefined;
    this.preparedWrites = undefined;
    this.preparedUploads = [];
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
