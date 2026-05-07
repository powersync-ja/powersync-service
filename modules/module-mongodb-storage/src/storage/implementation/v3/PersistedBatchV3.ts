import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import * as bson from 'bson';
import { flushBucketDataShared } from '../bucket-operations/batch-write.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { PersistedBatchShared } from '../common/PersistedBatchShared.js';
import { serializeParameterLookup } from '../document-formats/parameter-lookup.js';
import { V3FormatAdapter } from '../document-formats/v3-format.js';
import { BucketParameterDocument, taggedBucketParameterDocumentToTagged } from './models.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class PersistedBatchV3 extends PersistedBatchShared {
  declare protected readonly db: VersionedPowerSyncMongoV3;

  protected parameterIndex(indexId: string): mongo.Collection<BucketParameterDocument> {
    return this.db.parameterIndexV3(this.group_id, indexId);
  }

  protected sourceTables(): mongo.Collection<any> {
    return this.db.sourceTablesV3(this.group_id);
  }

  protected sourceRecords(sourceTableId: bson.ObjectId): mongo.Collection<any> {
    return this.db.sourceRecordsV3(this.group_id, sourceTableId);
  }

  protected bucketState(): mongo.Collection<any> {
    return this.db.bucketStateV3(this.group_id);
  }

  protected serializeParameterLookup(lookup: any): bson.Binary {
    return serializeParameterLookup(lookup);
  }

  protected taggedBucketParameterDocumentToTagged(doc: any): any {
    return taggedBucketParameterDocumentToTagged(doc);
  }

  protected checkDefinitionId(definitionId: BucketDefinitionId | null): BucketDefinitionId {
    if (definitionId == null) {
      throw new ReplicationAssertionError('Expected v3 bucket when incrementalReprocessing is enabled');
    }
    return definitionId;
  }

  protected async flushBucketData(session: mongo.ClientSession) {
    await flushBucketDataShared(
      {
        db: this.db,
        groupId: this.group_id,
        bucketData: this.bucketData,
        formatAdapter: new V3FormatAdapter(),
        getCollection: (groupId, definitionId) => this.db.bucketDataV3(groupId, definitionId)
      },
      session
    );
  }
}
