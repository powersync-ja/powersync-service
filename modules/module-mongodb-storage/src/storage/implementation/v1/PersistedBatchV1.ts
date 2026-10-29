import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { BatchOperationBuffer } from '../common/BatchOperationBuffer.js';

import { mongoTableId } from '../../../utils/util.js';
import { EMPTY_DATA } from '../MongoBucketBatchShared.js';
import { MongoWriteBatch } from '../MongoWriteBatch.js';
import {
  BucketStateUpdate,
  PersistedBatch,
  PersistedBatchOptions,
  SaveBucketDataOptions,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from '../common/PersistedBatch.js';
import { LEGACY_BUCKET_DATA_DEFINITION_ID, LEGACY_BUCKET_PARAMETER_INDEX_ID, SourceKey } from '../models.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';
import {
  BucketParameterDocument,
  BucketStateDocumentV1,
  CurrentDataDocument,
  serializeBucketDataV1,
  taggedBucketParameterDocumentToV1
} from './models.js';

export class PersistedBatchV1 implements PersistedBatch {
  private readonly operations: BatchOperationBuffer;

  // Each upsert supplies the complete source-record state. Only its final
  // state matters within a flush; bucket and parameter history is kept separately.
  private readonly currentData = new Map<string, mongo.AnyBulkWriteOperation<CurrentDataDocument>>();

  constructor(
    private readonly db: VersionedPowerSyncMongoV1,
    private readonly group_id: number,
    writtenSize: number,
    options?: PersistedBatchOptions
  ) {
    this.operations = new BatchOperationBuffer(
      group_id,
      writtenSize,
      {
        checkDefinitionId: () => LEGACY_BUCKET_DATA_DEFINITION_ID,
        getBucketDefinitionId: () => LEGACY_BUCKET_DATA_DEFINITION_ID
      },
      options?.logger
    );
  }

  get currentSize(): number {
    return this.operations.currentSize;
  }

  saveBucketData(options: SaveBucketDataOptions): void {
    this.operations.saveBucketData(options);
  }

  shouldFlushTransaction(): boolean {
    return this.operations.shouldFlushTransaction(this.currentData.size);
  }

  /**
   * Keeps track of bulkwrite operations within a transaction.
   *
   * There may be multiple of these batches per transaction, but it may not span
   * multiple transactions.
   */
  async flush(session: mongo.ClientSession, options?: storage.BucketBatchCommitOptions) {
    const startAt = performance.now();
    const writes = this.db.createWriteBatch(session, { ordered: false });
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
    const stats = this.operations.logFlush(startAt, this.currentData.size, options);
    this.currentData.clear();
    this.operations.clear();
    return stats;
  }

  saveParameterData(data: SaveParameterDataOptions) {
    const { sourceTable, sourceKey, evaluated } = data;
    const remaining_lookups = new Map<string, bson.Binary>();

    for (let lookup of data.existing_lookups) {
      if (lookup.indexId != null) {
        throw new ReplicationAssertionError('Unexpected v3 lookup when incrementalReprocessing is disabled');
      }
      remaining_lookups.set(lookup.lookup.toString('base64'), lookup.lookup);
    }

    for (let result of evaluated) {
      const binLookup = storage.serializeLookup(result.lookup);
      remaining_lookups.delete(binLookup.toString('base64'));

      const op_id = data.op_seq.next();
      this.operations.debugLastOpId = op_id;
      const values: BucketParameterDocument = {
        _id: op_id,
        key: {
          g: this.group_id,
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup: binLookup,
        bucket_parameters: result.bucketParameters
      };
      this.operations.bucketParameters.push({
        ...values,
        index: LEGACY_BUCKET_PARAMETER_INDEX_ID
      });

      this.operations.currentSize += 200;
    }

    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.operations.debugLastOpId = op_id;
      const values: BucketParameterDocument = {
        _id: op_id,
        key: {
          g: this.group_id,
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup,
        bucket_parameters: []
      };
      this.operations.bucketParameters.push({
        ...values,
        index: LEGACY_BUCKET_PARAMETER_INDEX_ID
      });

      this.operations.currentSize += 200;
    }
  }

  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId) {
    this.currentData.set(this.currentDataKey(sourceTableId, replicaId), {
      deleteOne: {
        filter: { _id: this.currentDataId(sourceTableId, replicaId) }
      }
    });
    this.operations.currentSize += 50;
  }

  softDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId, _checkpointGreaterThan: bigint) {
    this.hardDeleteCurrentData(sourceTableId, replicaId);
  }

  upsertCurrentData(values: UpsertCurrentDataOptions) {
    const buckets = values.buckets.map((bucket) => {
      if (bucket.definitionId != null) {
        throw new ReplicationAssertionError('Unexpected v3 bucket when incrementalReprocessing is disabled');
      }
      return {
        bucket: bucket.bucket,
        table: bucket.table,
        id: bucket.id
      };
    });
    const lookups = values.lookups.map((lookup) => {
      if (lookup.indexId != null) {
        throw new ReplicationAssertionError('Unexpected v3 lookup when incrementalReprocessing is disabled');
      }
      return lookup.lookup;
    });

    this.currentData.set(this.currentDataKey(values.sourceTableId, values.replicaId), {
      updateOne: {
        filter: { _id: this.currentDataId(values.sourceTableId, values.replicaId) },
        update: {
          $set: {
            data: values.data ?? EMPTY_DATA,
            buckets,
            lookups
          }
        },
        upsert: true
      }
    });
    this.operations.currentSize += (values.data?.length() ?? 0) + 100;
  }

  private async queueBucketData(writes: MongoWriteBatch) {
    writes.bulkWriteUnordered(
      this.db.bucketDataV1,
      this.operations.bucketData.map((document) => ({
        insertOne: {
          document: serializeBucketDataV1(document)
        }
      }))
    );
  }

  private queueBucketParameters(writes: MongoWriteBatch): void {
    writes.bulkWriteUnordered(
      this.db.parameterIndexV1,
      this.operations.bucketParameters.map((document) => ({
        insertOne: {
          document: taggedBucketParameterDocumentToV1(document)
        }
      }))
    );
  }

  private queueCurrentData(writes: MongoWriteBatch): void {
    if (this.currentData.size == 0) {
      return;
    }

    writes.bulkWriteUnordered(this.db.sourceRecordsV1, [...this.currentData.values()]);
  }

  private queueBucketStates(writes: MongoWriteBatch): void {
    writes.bulkWriteUnordered(this.db.bucketStateV1, this.getBucketStateUpdates());
  }

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<BucketStateDocumentV1>[] {
    return Array.from(this.operations.bucketStates.values()).map((state: BucketStateUpdate) => {
      return {
        updateOne: {
          filter: {
            _id: {
              g: this.group_id,
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
      } satisfies mongo.AnyBulkWriteOperation<BucketStateDocumentV1>;
    });
  }

  private currentDataKey(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): string {
    return Buffer.from(bson.serialize({ t: sourceTableId, k: replicaId })).toString('base64');
  }

  private currentDataId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): SourceKey {
    return {
      g: this.group_id,
      t: sourceTableId,
      k: replicaId
    };
  }
}
