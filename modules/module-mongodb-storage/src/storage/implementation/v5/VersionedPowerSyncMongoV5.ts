import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { PowerSyncMongo } from '../db.js';
import {
  BucketDataDocumentV5,
  BucketParameterDocumentV5,
  BucketStateDocumentV5,
  CurrentDataDocumentV5,
  SourceTableDocumentV5
} from './models.js';

export class VersionedPowerSyncMongoV5 extends VersionedPowerSyncMongo {
  constructor(upstream: PowerSyncMongo, storageConfig: any) {
    super(upstream, storageConfig, 'V5');
  }

  sourceRecordsV5(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocumentV5> {
    return this.sourceRecords<CurrentDataDocumentV5>(replicationStreamId, sourceTableId);
  }

  listSourceRecordCollectionsV5(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV5>[]> {
    return this.listSourceRecordCollections<CurrentDataDocumentV5>(replicationStreamId);
  }

  bucketStateV5(replicationStreamId: number): mongo.Collection<BucketStateDocumentV5> {
    return this.bucketState<BucketStateDocumentV5>(replicationStreamId);
  }

  parameterIndexV5(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV5> {
    return this.parameterIndex<BucketParameterDocumentV5>(replicationStreamId, indexId);
  }

  sourceTablesV5(replicationStreamId: number): mongo.Collection<SourceTableDocumentV5> {
    return this.sourceTables<SourceTableDocumentV5>(replicationStreamId);
  }

  bucketDataV5(replicationStreamId: number, definitionId: BucketDefinitionId): mongo.Collection<BucketDataDocumentV5> {
    return this.bucketData<BucketDataDocumentV5>(replicationStreamId, definitionId);
  }

  listBucketDataCollectionsV5(replicationStreamId: number) {
    return this.listBucketDataCollections(replicationStreamId);
  }

  listParameterIndexCollectionsV5(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV5>; indexId: ParameterIndexId }[]> {
    return this.listParameterIndexCollections(replicationStreamId) as unknown as Promise<
      { collection: mongo.Collection<BucketParameterDocumentV5>; indexId: ParameterIndexId }[]
    >;
  }
}
