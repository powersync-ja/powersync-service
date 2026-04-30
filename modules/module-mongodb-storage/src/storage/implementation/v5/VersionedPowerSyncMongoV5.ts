import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import { CommonSourceTableDocument } from '../models.js';
import {
  BucketDataDocumentV5,
  BucketParameterDocumentV5,
  BucketStateDocumentV5,
  CurrentDataDocumentV5,
  SourceTableDocumentV5
} from './models.js';

export class VersionedPowerSyncMongoV5 extends BaseVersionedPowerSyncMongo {
  sourceRecordsV5(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV5> {
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<CurrentDataDocumentV5>(collectionName);
  }

  async listSourceRecordCollectionsV5(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV5>[]> {
    return this.listCollectionsByPrefix<CurrentDataDocumentV5>(`source_records_${replicationStreamId}_`);
  }

  async initializeSourceRecordsCollection(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    await this.sourceRecordsV5(replicationStreamId, sourceTableId).createIndex(
      {
        pending_delete: 1
      },
      {
        partialFilterExpression: { pending_delete: { $exists: true } },
        name: 'pending_delete'
      }
    );
  }

  commonSourceTables(replicationStreamId: number): mongo.Collection<CommonSourceTableDocument> {
    return this.sourceTablesV5(replicationStreamId) as mongo.Collection<CommonSourceTableDocument>;
  }

  bucketStateV5(replicationStreamId: number): mongo.Collection<BucketStateDocumentV5> {
    return this.db.collection(`bucket_state_${replicationStreamId}`);
  }

  parameterIndexV5(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV5> {
    return this.db.collection(`parameter_index_${replicationStreamId}_${indexId}`);
  }

  sourceTablesV5(replicationStreamId: number): mongo.Collection<SourceTableDocumentV5> {
    return this.db.collection<SourceTableDocumentV5>(this.sourceTableCollectionName(replicationStreamId));
  }

  async initializeStreamStorage(replicationStreamId: number) {
    const sourceTables = this.sourceTablesV5(replicationStreamId);
    const bucketState = this.bucketStateV5(replicationStreamId);
    await sourceTables.createIndex(
      {
        connection_id: 1,
        schema_name: 1,
        table_name: 1,
        relation_id: 1
      },
      {
        name: 'source_lookup'
      }
    );
    await sourceTables.createIndex(
      {
        latest_pending_delete: 1
      },
      {
        partialFilterExpression: { latest_pending_delete: { $exists: true } },
        name: 'latest_pending_delete'
      }
    );
    await bucketState.createIndex(
      {
        last_op: 1
      },
      { name: 'bucket_updates', unique: true }
    );
    await bucketState.createIndex(
      {
        'estimate_since_compact.count': -1
      },
      { name: 'dirty_count' }
    );
  }

  bucketDataV5(replicationStreamId: number, definitionId: BucketDefinitionId) {
    return this.db.collection<BucketDataDocumentV5>(`bucket_data_${replicationStreamId}_${definitionId}`);
  }

  listBucketDataCollectionsV5(replicationStreamId: number) {
    return this.upstream.listBucketDataCollectionsV3(replicationStreamId);
  }

  async listParameterIndexCollectionsV5(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV5>; indexId: ParameterIndexId }[]> {
    const prefix = `parameter_index_${replicationStreamId}_`;
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => ({
        collection: this.db.collection<BucketParameterDocumentV5>(collection.name),
        indexId: collection.name.slice(prefix.length)
      }));
  }
}
