import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '@powersync/service-sync-rules';
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

export class VersionedPowerSyncMongoV3 extends BaseVersionedPowerSyncMongo {
  constructor(
    upstream: ConstructorParameters<typeof BaseVersionedPowerSyncMongo>[0],
    storageConfig: ConstructorParameters<typeof BaseVersionedPowerSyncMongo>[1]
  ) {
    super(upstream, storageConfig);
  }

  get syncConfigDefinitions() {
    return this.db.collection<SyncConfigDefinition>('sync_config');
  }

  sourceRecords(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV3> {
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<CurrentDataDocumentV3>(collectionName);
  }

  async listSourceRecordCollections(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV3>[]> {
    return this.listCollectionsByPrefix<CurrentDataDocumentV3>(`source_records_${replicationStreamId}_`);
  }

  async initializeSourceRecordsCollection(
    replicationStreamId: number,
    sourceTableId: mongo.ObjectId,
    session?: mongo.ClientSession
  ) {
    await this.sourceRecords(replicationStreamId, sourceTableId).createIndex(
      {
        pending_delete: 1
      },
      {
        partialFilterExpression: { pending_delete: { $exists: true } },
        name: 'pending_delete',
        session
      }
    );
  }

  commonSourceTables(replicationStreamId: number): mongo.Collection<CommonSourceTableDocument> {
    return this.sourceTables(replicationStreamId) as any as mongo.Collection<CommonSourceTableDocument>;
  }

  bucketState(replicationStreamId: number): mongo.Collection<BucketStateDocumentV3> {
    return this.db.collection(`bucket_state_${replicationStreamId}`);
  }

  parameterIndex(replicationStreamId: number, indexId: ParameterIndexId): mongo.Collection<BucketParameterDocumentV3> {
    return this.db.collection(`parameter_index_${replicationStreamId}_${indexId}`);
  }

  sourceTables(replicationStreamId: number): mongo.Collection<SourceTableDocumentV3> {
    return this.db.collection<SourceTableDocumentV3>(this.sourceTableCollectionName(replicationStreamId));
  }

  bucketData(replicationStreamId: number, definitionId: BucketDefinitionId): mongo.Collection<BucketDataDocumentV3> {
    return this.db.collection<BucketDataDocumentV3>(`bucket_data_${replicationStreamId}_${definitionId}`);
  }

  pendingS3Deletes(replicationStreamId: number): mongo.Collection<{ _id: string }> {
    return this.db.collection<{ _id: string }>(`pending_s3_deletes_${replicationStreamId}`);
  }

  listBucketDataCollections(replicationStreamId: number) {
    return this.listCollectionsByPrefix(`bucket_data_${replicationStreamId}_`);
  }

  async listParameterIndexCollections(replicationStreamId: number) {
    const prefix = `parameter_index_${replicationStreamId}_`;
    const collections = await this.listCollectionsByPrefix(prefix);

    return collections.map((collection) => ({
      collection,
      indexId: collection.collectionName.slice(prefix.length)
    }));
  }

  async initializeStreamStorage(replicationStreamId: number) {
    const sourceTables = this.sourceTables(replicationStreamId);
    const bucketState = this.bucketState(replicationStreamId);
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
