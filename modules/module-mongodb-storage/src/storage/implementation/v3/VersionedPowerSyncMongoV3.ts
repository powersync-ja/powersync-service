import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import {
  BucketParameterDocumentV3,
  BucketStateDocumentV3,
  CommonSourceTableDocument,
  CurrentDataDocument,
  CurrentDataDocumentV3,
  SourceTableDocumentV3
} from '../models.js';

export class VersionedPowerSyncMongoV3 extends BaseVersionedPowerSyncMongo {
  get sourceRecordsV1(): mongo.Collection<CurrentDataDocument> {
    this.assertV1Enabled('current_data collection should not be used when incrementalReprocessing is enabled');
    return this.db.collection<CurrentDataDocument>('__invalid__');
  }

  get bucketStateV1(): mongo.Collection<any> {
    this.assertV1Enabled('bucket_state collection should not be used when incrementalReprocessing is enabled');
    return this.db.collection('__invalid__');
  }

  sourceRecordsV3(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV3> {
    this.assertV3Enabled('v3_current_data collection should not be used when incrementalReprocessing is disabled');
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<CurrentDataDocumentV3>(collectionName);
  }

  async listSourceRecordCollectionsV3(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV3>[]> {
    this.assertV3Enabled('v3_current_data collection should not be used when incrementalReprocessing is disabled');
    return this.listCollectionsByPrefix<CurrentDataDocumentV3>(`source_records_${replicationStreamId}_`);
  }

  async initializeSourceRecordsCollection(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    this.assertV3Enabled(
      'source_records collection initialization should not be used when incrementalReprocessing is disabled'
    );
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
    this.assertV3Enabled('v3 bucket_state collection should not be used when incrementalReprocessing is disabled');
    return this.upstream.bucketStateV3(replicationStreamId);
  }

  sourceTablesV3(replicationStreamId: number): mongo.Collection<SourceTableDocumentV3> {
    this.assertV3Enabled('source_tables v3 collection should not be used when incrementalReprocessing is disabled');
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

  get v1_bucket_data(): mongo.Collection<any> {
    this.assertV1Enabled('bucket_data collection should not be used when incrementalReprocessing is enabled');
    return this.db.collection('__invalid__');
  }

  bucket_data_v3(groupId: number, definitionId: BucketDefinitionId) {
    this.assertV3Enabled('v3 bucket_data collections should not be used when incrementalReprocessing is disabled');
    return this.upstream.bucketDataV3(groupId, definitionId);
  }

  listBucketDataCollectionsV3(groupId: number) {
    this.assertV3Enabled('v3 bucket_data collections should not be used when incrementalReprocessing is disabled');
    return this.upstream.listBucketDataCollectionsV3(groupId);
  }

  get parameterIndexV1(): mongo.Collection<any> {
    this.assertV1Enabled('bucket_parameters collection should not be used when incrementalReprocessing is enabled');
    return this.db.collection('__invalid__');
  }

  parameterIndexV3(replicationStreamId: number, indexId: ParameterIndexId) {
    this.assertV3Enabled(
      'v3 bucket_parameters collections should not be used when incrementalReprocessing is disabled'
    );
    return this.upstream.parameterIndexV3(replicationStreamId, indexId);
  }

  async listParameterIndexCollectionsV3(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]> {
    this.assertV3Enabled(
      'v3 bucket_parameters collections should not be used when incrementalReprocessing is disabled'
    );

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
