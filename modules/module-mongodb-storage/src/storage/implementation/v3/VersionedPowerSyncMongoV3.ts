import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { PowerSyncMongo } from '../db.js';
import {
  BucketDataDocumentV3,
  BucketParameterDocumentV3,
  BucketStateDocumentV3,
  CurrentDataDocumentV3,
  SourceTableDocumentV3
} from './models.js';

export class VersionedPowerSyncMongoV3 extends VersionedPowerSyncMongo {
  constructor(upstream: PowerSyncMongo, storageConfig: any) {
    super(upstream, storageConfig, 'V3');
  }

  sourceRecordsV3(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV3> {
    return this.sourceRecords<CurrentDataDocumentV3>(replicationStreamId, sourceTableId);
  }

  listSourceRecordCollectionsV3(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV3>[]> {
    return this.listSourceRecordCollections<CurrentDataDocumentV3>(replicationStreamId);
  }

  bucketStateV3(replicationStreamId: number): mongo.Collection<BucketStateDocumentV3> {
    return this.bucketState<BucketStateDocumentV3>(replicationStreamId);
  }

  parameterIndexV3(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV3> {
    return this.parameterIndex<BucketParameterDocumentV3>(replicationStreamId, indexId);
  }

  sourceTablesV3(replicationStreamId: number): mongo.Collection<SourceTableDocumentV3> {
    return this.sourceTables<SourceTableDocumentV3>(replicationStreamId);
  }

  bucketDataV3(replicationStreamId: number, definitionId: BucketDefinitionId): mongo.Collection<BucketDataDocumentV3> {
    return this.bucketData<BucketDataDocumentV3>(replicationStreamId, definitionId);
  }

  listBucketDataCollectionsV3(replicationStreamId: number) {
    return this.listBucketDataCollections(replicationStreamId);
  }

  listParameterIndexCollectionsV3(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]> {
    return this.listParameterIndexCollections(replicationStreamId) as unknown as Promise<
      { collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]
    >;
  }
}
