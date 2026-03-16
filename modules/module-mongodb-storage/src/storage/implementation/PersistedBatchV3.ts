import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { mongoTableId, replicaIdToSubkey } from '../../utils/util.js';
import { currentBucketKey, MAX_ROW_SIZE } from './MongoBucketBatchShared.js';
import {
  PersistedBatch,
  SaveBucketDataOptions,
  SaveParameterDataOptions,
  UpsertCurrentDataOptions
} from './PersistedBatch.js';
import {
  BucketParameterDocumentV3,
  CurrentBucketV3,
  CurrentDataDocumentV3,
  isCurrentBucketV3,
  isRecordedLookupV3,
  RecordedLookupV3,
  SourceKey
} from './models.js';

export class PersistedBatchV3 extends PersistedBatch {
  currentData: mongo.AnyBulkWriteOperation<CurrentDataDocumentV3>[] = [];

  saveBucketData(options: SaveBucketDataOptions) {
    const remaining_buckets = new Map<string, CurrentBucketV3>();
    for (let bucket of options.before_buckets) {
      if (!isCurrentBucketV3(bucket)) {
        throw new ReplicationAssertionError('Expected v3 bucket when incrementalReprocessing is enabled');
      }
      remaining_buckets.set(currentBucketKey(bucket), bucket);
    }

    const dchecksum = BigInt(utils.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey)));

    for (const evaluated of options.evaluated) {
      const sourceDefinitionId = this.mapping.bucketSourceId(evaluated.source);
      const key = currentBucketKey({
        def: sourceDefinitionId,
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
    const remaining_lookups = new Map<string, RecordedLookupV3>();

    for (let lookup of data.existing_lookups) {
      if (!isRecordedLookupV3(lookup)) {
        throw new ReplicationAssertionError('Expected v3 lookup when incrementalReprocessing is enabled');
      }
      remaining_lookups.set(`${lookup.i}.${lookup.l.toString('base64')}`, lookup);
    }

    for (let result of evaluated) {
      const sourceDefinitionId = this.mapping.parameterLookupId(result.lookup.source);
      const binLookup = storage.serializeLookup(result.lookup);
      remaining_lookups.delete(`${sourceDefinitionId}.${binLookup.toString('base64')}`);

      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
      const values: BucketParameterDocumentV3 = {
        _id: op_id,
        index: sourceDefinitionId,
        key: {
          g: this.group_id,
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup: binLookup,
        bucket_parameters: result.bucketParameters
      };
      this.bucketParameters.push({
        insertOne: {
          document: values
        }
      });

      this.currentSize += 200;
    }

    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
      const values: BucketParameterDocumentV3 = {
        _id: op_id,
        index: lookup.i,
        key: {
          g: this.group_id,
          t: mongoTableId(sourceTable.id),
          k: sourceKey
        },
        lookup: lookup.l,
        bucket_parameters: []
      };
      this.bucketParameters.push({
        insertOne: {
          document: values
        }
      });

      this.currentSize += 200;
    }
  }

  hardDeleteCurrentData(id: SourceKey) {
    this.currentData.push({
      deleteOne: {
        filter: { _id: id }
      }
    });
    this.currentSize += 50;
  }

  softDeleteCurrentData(id: SourceKey, checkpointGreaterThan: bigint) {
    this.currentData.push({
      updateOne: {
        filter: { _id: id },
        update: {
          $set: {
            data: null,
            buckets: [] as CurrentDataDocumentV3['buckets'],
            lookups: [] as CurrentDataDocumentV3['lookups'],
            pending_delete: checkpointGreaterThan
          }
        },
        upsert: true
      }
    });
    this.currentSize += 50;
  }

  upsertCurrentData(values: UpsertCurrentDataOptions) {
    const buckets = values.buckets.map((bucket) => {
      if (!isCurrentBucketV3(bucket)) {
        throw new ReplicationAssertionError('Expected v3 bucket when incrementalReprocessing is enabled');
      }
      return bucket;
    });
    const lookups = values.lookups.map((lookup) => {
      if (!isRecordedLookupV3(lookup)) {
        throw new ReplicationAssertionError('Expected v3 lookup when incrementalReprocessing is enabled');
      }
      return lookup;
    });

    this.currentData.push({
      updateOne: {
        filter: { _id: values.id },
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
    });
    this.currentSize += (values.data?.length() ?? 0) + 100;
  }

  protected get currentDataCount() {
    return this.currentData.length;
  }

  protected async flushCurrentData(session: mongo.ClientSession) {
    await this.db.v3_current_data.bulkWrite(this.currentData, {
      session,
      ordered: true
    });
  }

  protected resetCurrentData() {
    this.currentData = [];
  }
}
