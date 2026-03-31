import { mongo } from '@powersync/lib-service-mongodb';
import { JSONBig } from '@powersync/service-jsonbig';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import * as bson from 'bson';

import { currentBucketKey, EMPTY_DATA, MAX_ROW_SIZE } from './MongoBucketBatchShared.js';
import {
  PersistedBatch,
  SaveBucketDataOptions,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from './PersistedBatch.js';
import {
  BucketParameterDocument,
  CurrentDataDocument,
  LEGACY_BUCKET_DATA_DEFINITION_ID,
  LEGACY_BUCKET_PARAMETER_INDEX_ID,
  SourceKey,
  taggedBucketParameterDocumentToV1,
  taggedBucketDataDocumentToV1
} from './models.js';
import { mongoTableId, replicaIdToSubkey } from '../../utils/util.js';

export class PersistedBatchV1 extends PersistedBatch {
  currentData: mongo.AnyBulkWriteOperation<CurrentDataDocument>[] = [];

  saveBucketData(options: SaveBucketDataOptions) {
    const remaining_buckets = new Map<string, SaveBucketDataOptions['before_buckets'][number]>();
    for (let bucket of options.before_buckets) {
      if (bucket.definitionId != null) {
        throw new ReplicationAssertionError('Unexpected v3 bucket when incrementalReprocessing is disabled');
      }
      remaining_buckets.set(currentBucketKey(bucket), bucket);
    }

    const dchecksum = BigInt(utils.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey)));

    for (const evaluated of options.evaluated) {
      const key = currentBucketKey({
        definitionId: null,
        bucket: evaluated.bucket,
        table: evaluated.table,
        id: evaluated.id
      });

      const recordData = JSONBig.stringify(evaluated.data);
      const checksum = utils.hashData(evaluated.table, evaluated.id, recordData);
      if (recordData.length > MAX_ROW_SIZE) {
        this.logger.error(`Row ${key} too large: ${recordData.length} bytes. Removing.`);
        continue;
      }

      remaining_buckets.delete(key);
      const byteEstimate = recordData.length + 200;
      this.currentSize += byteEstimate;

      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.addBucketDataPut({
        op_id,
        definitionId: LEGACY_BUCKET_DATA_DEFINITION_ID,
        bucket: evaluated.bucket,
        sourceTableId: options.table.id,
        sourceKey: options.sourceKey,
        table: evaluated.table,
        rowId: evaluated.id,
        checksum: BigInt(checksum),
        data: recordData
      });
      this.incrementBucket(evaluated.bucket, op_id, byteEstimate);
    }

    for (let bucket of remaining_buckets.values()) {
      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.addBucketDataRemove({
        op_id,
        definitionId: LEGACY_BUCKET_DATA_DEFINITION_ID,
        bucket: bucket.bucket,
        sourceTableId: options.table.id,
        sourceKey: options.sourceKey,
        table: bucket.table,
        rowId: bucket.id,
        checksum: dchecksum
      });
      this.currentSize += 200;
      this.incrementBucket(bucket.bucket, op_id, 200);
    }
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
      this.debugLastOpId = op_id;
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
      this.bucketParameters.push({
        ...values,
        index: LEGACY_BUCKET_PARAMETER_INDEX_ID
      });

      this.currentSize += 200;
    }

    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
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
      this.bucketParameters.push({
        ...values,
        index: LEGACY_BUCKET_PARAMETER_INDEX_ID
      });

      this.currentSize += 200;
    }
  }

  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId) {
    this.currentData.push({
      deleteOne: {
        filter: { _id: this.currentDataId(sourceTableId, replicaId) }
      }
    });
    this.currentSize += 50;
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

    this.currentData.push({
      updateOne: {
        filter: { _id: this.currentDataId(values.sourceTableId, values.replicaId) },
        update: {
          $set: {
            data: values.data ?? EMPTY_DATA,
            buckets,
            lookups
          },
          $unset: { pending_delete: 1 }
        },
        upsert: true
      }
    });
    this.currentSize += (values.data?.length() ?? 0) + 100;
  }

  protected get currentDataCount() {
    return this.currentData.length;
  }

  protected async flushBucketData(session: mongo.ClientSession) {
    await this.db.v1_bucket_data.bulkWrite(
      this.bucketData.map((document) => ({
        insertOne: {
          document: taggedBucketDataDocumentToV1(this.group_id, document)
        }
      })),
      {
        session,
        ordered: false
      }
    );
  }

  protected async flushBucketParameters(session: mongo.ClientSession) {
    await this.db.parameterIndexV1.bulkWrite(
      this.bucketParameters.map((document) => ({
        insertOne: {
          document: taggedBucketParameterDocumentToV1(document)
        }
      })),
      {
        session,
        ordered: false
      }
    );
  }

  protected async flushCurrentData(session: mongo.ClientSession) {
    const operationsBySourceTable = new Map<string, typeof this.currentData>();
    for (const operation of this.currentData) {
      const sourceTableId = this.getSourceTableIdHex(operation);
      if (sourceTableId == null) {
        throw new ReplicationAssertionError('Missing source table id for current_data operation');
      }
      const existing = operationsBySourceTable.get(sourceTableId) ?? [];
      existing.push(operation);
      operationsBySourceTable.set(sourceTableId, existing);
    }

    for (const operations of operationsBySourceTable.values()) {
      const firstOperation = operations[0]!;
      const sourceTableId = this.getSourceTableId(firstOperation);
      if (sourceTableId == null) {
        throw new ReplicationAssertionError('Missing source table id for current_data bulkWrite');
      }
      await this.db.v1_current_data(this.group_id, sourceTableId).bulkWrite(operations, {
        session,
        ordered: true
      });
    }
  }

  protected resetCurrentData() {
    this.currentData = [];
  }

  private getSourceTableIdHex(operation: mongo.AnyBulkWriteOperation<CurrentDataDocument>): string | undefined {
    return this.getSourceTableId(operation)?.toHexString();
  }

  private currentDataId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): SourceKey {
    return {
      g: this.group_id,
      t: sourceTableId,
      k: replicaId
    };
  }

  private getSourceTableId(operation: mongo.AnyBulkWriteOperation<CurrentDataDocument>): bson.ObjectId | undefined {
    if ('updateOne' in operation) {
      return operation.updateOne.filter._id?.t;
    }
    if ('deleteOne' in operation) {
      return operation.deleteOne.filter._id?.t;
    }
    return undefined;
  }
}
