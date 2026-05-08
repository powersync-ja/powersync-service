import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { PowerSyncMongo } from '../db.js';
import {
  BucketDataDocumentV3,
  BucketParameterDocument,
  BucketStateDocument,
  CurrentDataDocument,
  SourceTableDocument
} from './models.js';

export class VersionedPowerSyncMongoV3 extends VersionedPowerSyncMongo {
  constructor(upstream: PowerSyncMongo, storageConfig: any) {
    super(upstream, storageConfig, 'V3');
  }

  sourceRecordsV3(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocument> {
    return this.sourceRecords<CurrentDataDocument>(replicationStreamId, sourceTableId);
  }

  listSourceRecordCollectionsV3(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocument>[]> {
    return this.listSourceRecordCollections<CurrentDataDocument>(replicationStreamId);
  }

  bucketStateV3(replicationStreamId: number): mongo.Collection<BucketStateDocument> {
    return this.bucketState<BucketStateDocument>(replicationStreamId);
  }

  parameterIndexV3(replicationStreamId: number, indexId: ParameterIndexId): mongo.Collection<BucketParameterDocument> {
    return this.parameterIndex<BucketParameterDocument>(replicationStreamId, indexId);
  }

  sourceTablesV3(replicationStreamId: number): mongo.Collection<SourceTableDocument> {
    return this.sourceTables<SourceTableDocument>(replicationStreamId);
  }

  bucketDataV3(replicationStreamId: number, definitionId: BucketDefinitionId): mongo.Collection<BucketDataDocumentV3> {
    return this.bucketData<BucketDataDocumentV3>(replicationStreamId, definitionId);
  }

  listBucketDataCollectionsV3(replicationStreamId: number) {
    return this.listBucketDataCollections(replicationStreamId);
  }

  listParameterIndexCollectionsV3(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocument>; indexId: ParameterIndexId }[]> {
    // The base method returns a generic type; we know the concrete type because
    // this wrapper is only used for V3 storage.
    return this.listParameterIndexCollections(replicationStreamId) as unknown as Promise<
      { collection: mongo.Collection<BucketParameterDocument>; indexId: ParameterIndexId }[]
    >;
  }
}
