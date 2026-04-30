import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { BucketDataSource } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import {
  BucketStateUpdate,
  PersistedBatch,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from '../common/PersistedBatch.js';
import { SourceTableKey } from '../models.js';
import {
  BucketDataDocumentV5,
  BucketParameterDocumentV5,
  BucketStateDocumentV5,
  CurrentDataDocumentV5,
  serializeBucketDataV5,
  SourceTableDocumentV5,
  taggedBucketParameterDocumentToV5
} from './models.js';
import { serializeParameterLookupV5 } from './MongoParameterLookupV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class PersistedBatchV5 extends PersistedBatch {
  declare protected readonly db: VersionedPowerSyncMongoV5;

  currentData: { sourceTableId: bson.ObjectId; operation: mongo.AnyBulkWriteOperation<CurrentDataDocumentV5> }[] = [];
  sourceTablePendingDeletes = new Map<string, InternalOpId>();

  protected checkDefinitionId(definitionId: BucketDefinitionId | null): BucketDefinitionId {
    if (definitionId == null) {
      // This is required for V5 storage.
      throw new ReplicationAssertionError('Expected v5 bucket when incrementalReprocessing is enabled');
    }
    return definitionId;
  }

  protected getBucketDefinitionId(bucketSource: BucketDataSource): BucketDefinitionId {
    return this.mapping.bucketSourceId(bucketSource);
  }

  saveParameterData(data: SaveParameterDataOptions) {
    const { sourceTable, sourceKey, evaluated } = data;
    const remaining_lookups = new Map<string, SaveParameterDataOptions['existing_lookups'][number]>();

    for (let lookup of data.existing_lookups) {
      if (lookup.indexId == null) {
        throw new ReplicationAssertionError('Expected v5 lookup when incrementalReprocessing is enabled');
      }
      remaining_lookups.set(`${lookup.indexId}.${lookup.lookup.toString('base64')}`, lookup);
    }

    for (let result of evaluated) {
      const sourceDefinitionId = this.mapping.parameterLookupId(result.lookup.source);
      const binLookup = serializeParameterLookupV5(result.lookup);
      remaining_lookups.delete(`${sourceDefinitionId}.${binLookup.toString('base64')}`);

      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
      const values: BucketParameterDocumentV5 = {
        _id: op_id,
        key: {
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        } satisfies SourceTableKey,
        lookup: binLookup,
        bucket_parameters: result.bucketParameters
      };
      this.bucketParameters.push({
        ...values,
        index: sourceDefinitionId
      });

      this.currentSize += 200;
    }

    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
      const indexId = lookup.indexId;
      if (indexId == null) {
        throw new ReplicationAssertionError('Expected v5 lookup when incrementalReprocessing is enabled');
      }
      const values: BucketParameterDocumentV5 = {
        _id: op_id,
        key: {
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        } satisfies SourceTableKey,
        lookup: lookup.lookup,
        bucket_parameters: []
      };
      this.bucketParameters.push({
        ...values,
        index: indexId
      });

      this.currentSize += 200;
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
    this.currentSize += 50;
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
              buckets: [] as CurrentDataDocumentV5['buckets'],
              lookups: [] as CurrentDataDocumentV5['lookups'],
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

    this.currentSize += 50;
  }

  upsertCurrentData(values: UpsertCurrentDataOptions) {
    const buckets = values.buckets.map((bucket) => {
      if (bucket.definitionId == null) {
        throw new ReplicationAssertionError('Expected v5 bucket when incrementalReprocessing is enabled');
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
        throw new ReplicationAssertionError('Expected v5 lookup when incrementalReprocessing is enabled');
      }
      return {
        i: lookup.indexId,
        l: lookup.lookup
      };
    });

    this.currentData.push({
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
    this.currentSize += (values.data?.length() ?? 0) + 100;
  }

  protected get currentDataCount() {
    return this.currentData.length;
  }

  protected async flushBucketData(session: mongo.ClientSession) {
    const operationsByDefinition = new Map<BucketDefinitionId, typeof this.bucketData>();
    for (const document of this.bucketData) {
      const existing = operationsByDefinition.get(document.bucketKey.definitionId) ?? [];
      existing.push(document);
      operationsByDefinition.set(document.bucketKey.definitionId, existing);
    }

    for (const [definitionId, documents] of operationsByDefinition.entries()) {
      const operationsByBucket = new Map<string, typeof documents>();
      for (const document of documents) {
        const existing = operationsByBucket.get(document.bucketKey.bucket) ?? [];
        existing.push(document);
        operationsByBucket.set(document.bucketKey.bucket, existing);
      }

      const inserts: mongo.AnyBulkWriteOperation<BucketDataDocumentV5>[] = [];
      for (const [bucket, ops] of operationsByBucket.entries()) {
        const chunks = this.chunkBucketData(ops);
        for (const chunk of chunks) {
          inserts.push({
            insertOne: {
              document: serializeBucketDataV5(bucket, chunk)
            }
          });
        }
      }

      if (inserts.length > 0) {
        await this.db.bucketDataV5(this.group_id, definitionId).bulkWrite(inserts, {
          session,
          ordered: false
        });
      }
    }
  }

  private chunkBucketData(operations: BucketDataDoc[]): BucketDataDoc[][] {
    const chunks: BucketDataDoc[][] = [];
    let currentChunk: BucketDataDoc[] = [];
    let currentSize = 0;
    const maxDocSize = 1024 * 1024; // 1MB threshold

    for (const op of operations) {
      const opSize = op.data?.length ?? 0;

      if (currentSize + opSize > maxDocSize && currentChunk.length > 0) {
        chunks.push(currentChunk);
        currentChunk = [];
        currentSize = 0;
      }

      currentChunk.push(op);
      currentSize += opSize;
    }

    if (currentChunk.length > 0) {
      chunks.push(currentChunk);
    }

    return chunks;
  }

  protected async flushBucketParameters(session: mongo.ClientSession) {
    const operationsByIndex = new Map<string, typeof this.bucketParameters>();
    for (const document of this.bucketParameters) {
      const existing = operationsByIndex.get(document.index) ?? [];
      existing.push(document);
      operationsByIndex.set(document.index, existing);
    }

    for (const [indexId, documents] of operationsByIndex.entries()) {
      await this.db.parameterIndexV5(this.group_id, indexId).bulkWrite(
        documents.map((document) => ({
          insertOne: {
            document: taggedBucketParameterDocumentToV5(document)
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

    const sourceTableUpdates: mongo.AnyBulkWriteOperation<SourceTableDocumentV5>[] = [
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
      await this.db.sourceTablesV5(this.group_id).bulkWrite(sourceTableUpdates, { session, ordered: false });
    }

    for (const operations of operationsBySourceTable.values()) {
      const sourceTableId = operations[0]!.sourceTableId;
      await this.db.sourceRecordsV5(this.group_id, sourceTableId).bulkWrite(
        operations.map((entry) => entry.operation),
        {
          session,
          ordered: true
        }
      );
    }
  }

  protected async flushBucketStates(session: mongo.ClientSession) {
    await this.db.bucketStateV5(this.group_id).bulkWrite(this.getBucketStateUpdates(), {
      session,
      ordered: false
    });
  }

  protected resetCurrentData() {
    this.currentData = [];
    this.sourceTablePendingDeletes.clear();
  }

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<BucketStateDocumentV5>[] {
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
          update: {
            $set: {
              last_op: state.lastOp
            },
            $inc: {
              'estimate_since_compact.count': state.incrementCount,
              'estimate_since_compact.bytes': state.incrementBytes
            }
          },
          upsert: true
        }
      } satisfies mongo.AnyBulkWriteOperation<BucketStateDocumentV5>;
    });
  }
}
