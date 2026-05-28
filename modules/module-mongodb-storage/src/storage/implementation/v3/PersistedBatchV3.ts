import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { BucketDataSource } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { BucketParameterDocument, taggedBucketParameterDocumentToTagged } from '../common/models.js';
import {
  BucketStateUpdate,
  PersistedBatch,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from '../common/PersistedBatch.js';
import { BucketDocumentFormatAdapter } from '../document-formats/bucket-document-format.js';
import { serializeParameterLookup } from '../document-formats/parameter-lookup.js';

export class PersistedBatchV3 extends PersistedBatch {
  private formatAdapter = new BucketDocumentFormatAdapter();

  currentData: { sourceTableId: bson.ObjectId; operation: mongo.AnyBulkWriteOperation<any> }[] = [];
  sourceTablePendingDeletes = new Map<string, InternalOpId>();

  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

  // Collection accessors

  protected parameterIndex(indexId: string): mongo.Collection<BucketParameterDocument> {
    return this.db.parameterIndex(this.group_id, indexId);
  }

  protected sourceTables(): mongo.Collection<any> {
    return this.db.sourceTables(this.group_id);
  }

  protected sourceRecords(sourceTableId: bson.ObjectId): mongo.Collection<any> {
    return this.db.sourceRecords(this.group_id, sourceTableId);
  }

  protected bucketState(): mongo.Collection<any> {
    return this.db.bucketState(this.group_id);
  }

  protected serializeParameterLookup(lookup: any): bson.Binary {
    return serializeParameterLookup(lookup);
  }

  protected taggedBucketParameterDocumentToTagged(doc: any): any {
    return taggedBucketParameterDocumentToTagged(doc);
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
    const remaining_lookups = new Map<string, SaveParameterDataOptions['existing_lookups'][number]>();

    for (let lookup of data.existing_lookups) {
      if (lookup.indexId == null) {
        throw new ReplicationAssertionError('Expected lookup when incrementalReprocessing is enabled');
      }
      remaining_lookups.set(`${lookup.indexId}.${lookup.lookup.toString('base64')}`, lookup);
    }

    for (let result of evaluated) {
      const sourceDefinitionId = this.mapping.parameterLookupId(result.lookup.source);
      const binLookup = this.serializeParameterLookup(result.lookup);
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

      this.currentSize += 200;
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

    this.currentSize += 50;
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

  // Flush methods

  protected async flushBucketData(session: mongo.ClientSession) {
    const operationsByDefinition = new Map<BucketDefinitionId, any[]>();
    for (const document of this.bucketData) {
      const existing = operationsByDefinition.get(document.bucketKey.definitionId) ?? [];
      existing.push(document);
      operationsByDefinition.set(document.bucketKey.definitionId, existing);
    }

    for (const [definitionId, documents] of operationsByDefinition.entries()) {
      const operationsByBucket = new Map<string, any[]>();
      for (const document of documents) {
        const existing = operationsByBucket.get(document.bucketKey.bucket) ?? [];
        existing.push(document);
        operationsByBucket.set(document.bucketKey.bucket, existing);
      }

      const inserts: mongo.AnyBulkWriteOperation<any>[] = [];
      for (const [bucket, ops] of operationsByBucket.entries()) {
        const serialized = this.formatAdapter.serializeForBulkWrite(bucket, ops);
        inserts.push(...serialized);
      }

      if (inserts.length > 0) {
        await this.db.bucketData(this.group_id, definitionId).bulkWrite(inserts, {
          session,
          ordered: false
        });
      }
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
      await this.parameterIndex(indexId).bulkWrite(
        documents.map((document) => ({
          insertOne: {
            document: this.taggedBucketParameterDocumentToTagged(document)
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

    const sourceTableUpdates: mongo.AnyBulkWriteOperation<any>[] = [...this.sourceTablePendingDeletes.entries()].map(
      ([key, value]) => {
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
      }
    );

    if (sourceTableUpdates.length > 0) {
      await this.sourceTables().bulkWrite(sourceTableUpdates, { session, ordered: false });
    }

    for (const operations of operationsBySourceTable.values()) {
      const sourceTableId = operations[0]!.sourceTableId;
      await this.sourceRecords(sourceTableId).bulkWrite(
        operations.map((entry) => entry.operation),
        {
          session,
          ordered: true
        }
      );
    }
  }

  protected async flushBucketStates(session: mongo.ClientSession) {
    await this.bucketState().bulkWrite(this.getBucketStateUpdates(), {
      session,
      ordered: false
    });
  }

  protected resetCurrentData() {
    this.currentData = [];
    this.sourceTablePendingDeletes.clear();
  }

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<any>[] {
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
      } satisfies mongo.AnyBulkWriteOperation<any>;
    });
  }
}
