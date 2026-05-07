import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import * as bson from 'bson';
import { flushBucketDataShared } from '../bucket-operations/batch-write.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { PersistedBatchShared } from '../common/PersistedBatchShared.js';
import { serializeParameterLookup } from '../document-formats/parameter-lookup.js';
import { V5FormatAdapter } from '../document-formats/v5-format.js';
import { BucketParameterDocument, taggedBucketParameterDocumentToTagged } from './models.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class PersistedBatchV5 extends PersistedBatchShared {
  declare protected readonly db: VersionedPowerSyncMongoV5;

  protected parameterIndex(indexId: string): mongo.Collection<BucketParameterDocument> {
    return this.db.parameterIndexV5(this.group_id, indexId);
  }

  protected sourceTables(): mongo.Collection<any> {
    return this.db.sourceTablesV5(this.group_id);
  }

  protected sourceRecords(sourceTableId: bson.ObjectId): mongo.Collection<any> {
    return this.db.sourceRecordsV5(this.group_id, sourceTableId);
  }

  protected bucketState(): mongo.Collection<any> {
    return this.db.bucketStateV5(this.group_id);
  }

  protected serializeParameterLookup(lookup: any): bson.Binary {
    return serializeParameterLookup(lookup);
  }

  protected taggedBucketParameterDocumentToTagged(doc: any): any {
    return taggedBucketParameterDocumentToTagged(doc);
  }

  protected checkDefinitionId(definitionId: BucketDefinitionId | null): BucketDefinitionId {
    if (definitionId == null) {
      throw new ReplicationAssertionError('Expected v5 bucket when incrementalReprocessing is enabled');
    }
    return definitionId;
  }

  protected async flushBucketData(session: mongo.ClientSession) {
    await flushBucketDataShared(
      {
        db: this.db,
        groupId: this.group_id,
        bucketData: this.bucketData,
        formatAdapter: new V5FormatAdapter(),
        getCollection: (groupId, definitionId) => this.db.bucketDataV5(groupId, definitionId)
      },
      session
    );
  }
}
