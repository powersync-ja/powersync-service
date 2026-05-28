import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import { CommonSourceTableDocument } from '../models.js';
import {
  BucketDataDocumentV3,
  BucketParameterDocumentV3,
  BucketStateDocumentV3,
  CurrentDataDocumentV3,
  SourceTableDocumentV3,
  SyncConfigDefinition
} from './models.js';

type UpstreamType = ConstructorParameters<typeof BaseVersionedPowerSyncMongo>[0];
type StorageConfigType = ConstructorParameters<typeof BaseVersionedPowerSyncMongo>[1];

export class VersionedPowerSyncMongoV3 extends BaseVersionedPowerSyncMongo {
  constructor(upstream: UpstreamType, storageConfig: StorageConfigType) {
    super(upstream, storageConfig);
  }

  get syncConfigDefinitions() {
    return this.db.collection<SyncConfigDefinition>('sync_config');
  }

  // Generic collection accessors

  sourceRecords<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number,
    sourceTableId: mongo.ObjectId
  ): mongo.Collection<T> {
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<T>(collectionName);
  }

  sourceRecordsV3(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV3> {
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<CurrentDataDocumentV3>(collectionName);
  }

  async listSourceRecordCollections<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number
  ): Promise<mongo.Collection<T>[]> {
    return this.listCollectionsByPrefix<T>(`source_records_${replicationStreamId}_`);
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

  bucketState<T extends mongo.Document = mongo.Document>(replicationStreamId: number): mongo.Collection<T> {
    return this.db.collection<T>(`bucket_state_${replicationStreamId}`);
  }

  bucketStateV3(replicationStreamId: number): mongo.Collection<BucketStateDocumentV3> {
    return this.db.collection(`bucket_state_${replicationStreamId}`);
  }

  parameterIndex<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<T> {
    return this.db.collection<T>(`parameter_index_${replicationStreamId}_${indexId}`);
  }

  parameterIndexV3(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV3> {
    return this.db.collection(`parameter_index_${replicationStreamId}_${indexId}`);
  }

  sourceTables<T extends mongo.Document = mongo.Document>(replicationStreamId: number): mongo.Collection<T> {
    return this.db.collection<T>(this.sourceTableCollectionName(replicationStreamId));
  }

  sourceTablesV3(replicationStreamId: number): mongo.Collection<SourceTableDocumentV3> {
    return this.db.collection<SourceTableDocumentV3>(this.sourceTableCollectionName(replicationStreamId));
  }

  bucketData<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number,
    definitionId: BucketDefinitionId
  ): mongo.Collection<T> {
    return this.db.collection<T>(`bucket_data_${replicationStreamId}_${definitionId}`);
  }

  bucketDataV3(replicationStreamId: number, definitionId: BucketDefinitionId) {
    return this.db.collection<BucketDataDocumentV3>(`bucket_data_${replicationStreamId}_${definitionId}`);
  }

  listBucketDataCollections(replicationStreamId: number) {
    return this.listCollectionsByPrefix(`bucket_data_${replicationStreamId}_`);
  }

  listBucketDataCollectionsV3(replicationStreamId: number) {
    return this.upstream.listBucketDataCollectionsV3(replicationStreamId);
  }

  async listParameterIndexCollections(replicationStreamId: number) {
    const prefix = `parameter_index_${replicationStreamId}_`;
    const collections = await this.listCollectionsByPrefix(prefix);

    return collections.map((collection) => ({
      collection,
      indexId: collection.collectionName.slice(prefix.length)
    }));
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
}
