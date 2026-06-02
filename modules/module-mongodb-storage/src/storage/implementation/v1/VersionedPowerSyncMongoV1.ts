import { mongo } from '@powersync/lib-service-mongodb';
import { BaseVersionedPowerSyncMongo } from '../common/VersionedPowerSyncMongoBase.js';
import { CommonSourceTableDocument } from '../models.js';
import { BucketDataDocumentV1, BucketParameterDocument, BucketStateDocumentV1, CurrentDataDocument } from './models.js';

export class VersionedPowerSyncMongoV1 extends BaseVersionedPowerSyncMongo {
  get sourceRecordsV1(): mongo.Collection<CurrentDataDocument> {
    return this.upstream.current_data;
  }

  get bucketStateV1(): mongo.Collection<BucketStateDocumentV1> {
    return this.upstream.bucket_state;
  }

  commonSourceTables(_replicationStreamId: number): mongo.Collection<CommonSourceTableDocument> {
    return this.upstream.source_tables as any as mongo.Collection<CommonSourceTableDocument>;
  }

  async initializeStreamStorage(_replicationStreamId: number): Promise<void> {}

  get bucketDataV1(): mongo.Collection<BucketDataDocumentV1> {
    return this.upstream.bucket_data;
  }

  get parameterIndexV1(): mongo.Collection<BucketParameterDocument> {
    return this.upstream.bucket_parameters;
  }
}
