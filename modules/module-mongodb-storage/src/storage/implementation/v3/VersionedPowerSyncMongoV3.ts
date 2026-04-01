import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import { CommonSourceTableDocument } from '../common/models.js';
import {
  BucketDataDocumentV3,
  BucketParameterDocumentV3,
  BucketStateDocumentV3,
  CurrentDataDocumentV3,
  SourceTableDocumentV3
} from './models.js';

export class VersionedPowerSyncMongoV3 extends BaseVersionedPowerSyncMongo {
  sourceRecordsV3(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV3> {
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<CurrentDataDocumentV3>(collectionName);
  }

  async listSourceRecordCollectionsV3(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV3>[]> {
    return this.listCollectionsByPrefix<CurrentDataDocumentV3>(`source_records_${replicationStreamId}_`);
  }

  async initializeSourceRecordsCollection(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    await this.sourceRecordsV3(replicationStreamId, sourceTableId).createIndex(
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
    return this.sourceTablesV3(replicationStreamId) as mongo.Collection<CommonSourceTableDocument>;
  }

  bucketStateV3(replicationStreamId: number): mongo.Collection<BucketStateDocumentV3> {
    return this.db.collection(`bucket_state_${replicationStreamId}`);
  }

  parameterIndexV3(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV3> {
    return this.db.collection(`parameter_index_${replicationStreamId}_${indexId}`);
  }

  sourceTablesV3(replicationStreamId: number): mongo.Collection<SourceTableDocumentV3> {
    return this.db.collection<SourceTableDocumentV3>(this.sourceTableCollectionName(replicationStreamId));
  }

  async initializeStreamStorage(replicationStreamId: number) {
    const sourceTables = this.sourceTablesV3(replicationStreamId);
    const bucketState = this.bucketStateV3(replicationStreamId);
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

  bucketDataV3(groupId: number, definitionId: BucketDefinitionId) {
    return this.db.collection<BucketDataDocumentV3>(`bucket_data_${groupId}_${definitionId}`);
  }

  listBucketDataCollectionsV3(groupId: number) {
    return this.upstream.listBucketDataCollectionsV3(groupId);
  }

  async listParameterIndexCollectionsV3(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]> {
    const prefix = `parameter_index_${replicationStreamId}_`;
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => ({
        collection: this.db.collection<BucketParameterDocumentV3>(collection.name),
        indexId: collection.name.slice(prefix.length)
      }));
  }
}
