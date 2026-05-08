import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import { CommonSourceTableDocument } from '../models.js';

type UpstreamType = ConstructorParameters<typeof BaseVersionedPowerSyncMongo>[0];
type StorageConfigType = ConstructorParameters<typeof BaseVersionedPowerSyncMongo>[1];

/**
 * Parameterized versioned MongoDB collection accessor.
 *
 * This class contains all the shared logic between V3 and V5 implementations.
 * The V3/V5 subclasses only add version-specific method names and type annotations.
 */
export class VersionedPowerSyncMongo extends BaseVersionedPowerSyncMongo {
  constructor(
    upstream: UpstreamType,
    storageConfig: StorageConfigType,
    private versionSuffix: 'V3' | 'V5'
  ) {
    super(upstream, storageConfig);
  }

  sourceRecords<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number,
    sourceTableId: mongo.ObjectId
  ): mongo.Collection<T> {
    const collectionName = this.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
    return this.db.collection<T>(collectionName);
  }

  async listSourceRecordCollections<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number
  ): Promise<mongo.Collection<T>[]> {
    return this.listCollectionsByPrefix<T>(`source_records_${replicationStreamId}_`);
  }

  async initializeSourceRecordsCollection(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    await this.sourceRecords(replicationStreamId, sourceTableId).createIndex(
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
    return this.sourceTables(replicationStreamId) as any;
  }

  bucketState<T extends mongo.Document = mongo.Document>(replicationStreamId: number): mongo.Collection<T> {
    return this.db.collection<T>(`bucket_state_${replicationStreamId}`);
  }

  parameterIndex<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<T> {
    return this.db.collection<T>(`parameter_index_${replicationStreamId}_${indexId}`);
  }

  sourceTables<T extends mongo.Document = mongo.Document>(replicationStreamId: number): mongo.Collection<T> {
    return this.db.collection<T>(this.sourceTableCollectionName(replicationStreamId));
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

  bucketData<T extends mongo.Document = mongo.Document>(
    replicationStreamId: number,
    definitionId: BucketDefinitionId
  ): mongo.Collection<T> {
    return this.db.collection<T>(`bucket_data_${replicationStreamId}_${definitionId}`);
  }

  listBucketDataCollections(replicationStreamId: number) {
    // V3 and V5 share the same bucket_data_* collection naming scheme,
    // so we can use the shared prefix helper instead of delegating to a
    // version-specific upstream method.
    return this.listCollectionsByPrefix(`bucket_data_${replicationStreamId}_`);
  }

  async listParameterIndexCollections(replicationStreamId: number) {
    const prefix = `parameter_index_${replicationStreamId}_`;
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => ({
        collection: this.db.collection(collection.name),
        indexId: collection.name.slice(prefix.length)
      }));
  }
}
