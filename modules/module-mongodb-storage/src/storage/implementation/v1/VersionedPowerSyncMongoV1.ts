import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import {
  BucketDataDocumentV1,
  BucketParameterDocument,
  BucketParameterDocumentV3,
  BucketStateDocumentV1,
  BucketStateDocumentV3,
  CommonSourceTableDocument,
  CurrentDataDocument,
  CurrentDataDocumentV3,
  SourceTableDocumentV3
} from '../models.js';

export class VersionedPowerSyncMongoV1 extends BaseVersionedPowerSyncMongo {
  get sourceRecordsV1(): mongo.Collection<CurrentDataDocument> {
    this.assertV1Enabled('current_data collection should not be used when incrementalReprocessing is enabled');
    return this.upstream.current_data;
  }

  get bucketStateV1(): mongo.Collection<BucketStateDocumentV1> {
    this.assertV1Enabled('bucket_state collection should not be used when incrementalReprocessing is enabled');
    return this.upstream.bucket_state;
  }

  sourceRecordsV3(
    _replicationStreamId: number,
    _sourceTableId: mongo.ObjectId
  ): mongo.Collection<CurrentDataDocumentV3> {
    this.assertV3Enabled('v3_current_data collection should not be used when incrementalReprocessing is disabled');
    return this.db.collection<CurrentDataDocumentV3>('__invalid__');
  }

  listSourceRecordCollectionsV3(_replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocumentV3>[]> {
    this.assertV3Enabled('v3_current_data collection should not be used when incrementalReprocessing is disabled');
    return Promise.resolve([]);
  }

  initializeSourceRecordsCollection(_replicationStreamId: number, _sourceTableId: mongo.ObjectId): Promise<void> {
    this.assertV3Enabled(
      'source_records collection initialization should not be used when incrementalReprocessing is disabled'
    );
    return Promise.resolve();
  }

  commonSourceTables(_replicationStreamId: number): mongo.Collection<CommonSourceTableDocument> {
    return this.upstream.source_tables as any as mongo.Collection<CommonSourceTableDocument>;
  }

  bucketStateV3(_replicationStreamId: number): mongo.Collection<BucketStateDocumentV3> {
    this.assertV3Enabled('v3 bucket_state collection should not be used when incrementalReprocessing is disabled');
    return this.db.collection<BucketStateDocumentV3>('__invalid__');
  }

  sourceTablesV3(_replicationStreamId: number): mongo.Collection<SourceTableDocumentV3> {
    this.assertV3Enabled('source_tables v3 collection should not be used when incrementalReprocessing is disabled');
    return this.db.collection<SourceTableDocumentV3>('__invalid__');
  }

  async initializeStreamStorage(_replicationStreamId: number): Promise<void> {}

  get v1_bucket_data(): mongo.Collection<BucketDataDocumentV1> {
    this.assertV1Enabled('bucket_data collection should not be used when incrementalReprocessing is enabled');
    return this.upstream.bucket_data;
  }

  bucket_data_v3(_groupId: number, _definitionId: BucketDefinitionId): mongo.Collection<any> {
    this.assertV3Enabled('v3 bucket_data collections should not be used when incrementalReprocessing is disabled');
    return this.db.collection('__invalid__');
  }

  listBucketDataCollectionsV3(_groupId: number): Promise<mongo.Collection<any>[]> {
    this.assertV3Enabled('v3 bucket_data collections should not be used when incrementalReprocessing is disabled');
    return Promise.resolve([]);
  }

  get parameterIndexV1(): mongo.Collection<BucketParameterDocument> {
    this.assertV1Enabled('bucket_parameters collection should not be used when incrementalReprocessing is enabled');
    return this.upstream.bucket_parameters;
  }

  parameterIndexV3(
    _replicationStreamId: number,
    _indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV3> {
    this.assertV3Enabled(
      'v3 bucket_parameters collections should not be used when incrementalReprocessing is disabled'
    );
    return this.db.collection<BucketParameterDocumentV3>('__invalid__');
  }

  listParameterIndexCollectionsV3(
    _replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]> {
    this.assertV3Enabled(
      'v3 bucket_parameters collections should not be used when incrementalReprocessing is disabled'
    );
    return Promise.resolve([]);
  }
}
