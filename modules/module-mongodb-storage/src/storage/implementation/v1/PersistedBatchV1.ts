import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';

import { mongoTableId } from '../../../utils/util.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { EMPTY_DATA } from '../MongoBucketBatchShared.js';
import {
  BucketStateUpdate,
  PersistedBatch,
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

export class PersistedBatchV1 extends PersistedBatch {
  declare protected readonly db: VersionedPowerSyncMongoV1;

  currentData: mongo.AnyBulkWriteOperation<CurrentDataDocument>[] = [];

  protected checkDefinitionId(_definitionId: BucketDefinitionId | null): BucketDefinitionId {
    // V1 storage doesn't persist the id, and we don't use it.
    return LEGACY_BUCKET_DATA_DEFINITION_ID;
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
          }
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
    await this.db.bucketDataV1.bulkWrite(
      this.bucketData.map((document) => ({
        insertOne: {
          document: serializeBucketDataV1(document)
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
    if (this.currentData.length == 0) {
      return;
    }

    await this.db.sourceRecordsV1.bulkWrite(this.currentData, {
      session,
      ordered: true
    });
  }

  protected async flushBucketStates(session: mongo.ClientSession) {
    await this.db.bucketStateV1.bulkWrite(this.getBucketStateUpdates(), {
      session,
      ordered: false
    });
  }

  protected resetCurrentData() {
    this.currentData = [];
  }

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<BucketStateDocumentV1>[] {
    return Array.from(this.bucketStates.values()).map((state: BucketStateUpdate) => {
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

  private currentDataId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): SourceKey {
    return {
      g: this.group_id,
      t: sourceTableId,
      k: replicaId
    };
  }
}
