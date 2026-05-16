import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import * as bson from 'bson';
import { flushBucketDataShared } from '../bucket-operations/batch-write.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { BucketParameterDocument, taggedBucketParameterDocumentToTagged } from '../common/models.js';
import { PersistedBatchShared } from '../common/PersistedBatchShared.js';
import { BucketDocumentFormatAdapter } from '../document-formats/bucket-document-format.js';
import { serializeParameterLookup } from '../document-formats/parameter-lookup.js';

export class PersistedBatchV3 extends PersistedBatchShared {
  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

  protected parameterIndex(indexId: string): mongo.Collection<BucketParameterDocument> {
    return this.db.parameterIndex(this.group_id, indexId);
  }

  protected sourceTables(): mongo.Collection<any> {
    return this.db.sourceTables(this.group_id);
  }

  protected sourceRecords(sourceTableId: bson.ObjectId): mongo.Collection<any> {
    return this.db.sourceRecords(this.group_id, sourceTableId);
  }

  protected bucketState(): mongo.Collection<any> {
    return this.db.bucketState(this.group_id);
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
        formatAdapter: new BucketDocumentFormatAdapter(),
        getCollection: (groupId, definitionId) => this.db.bucketData(groupId, definitionId)
      },
      session
    );
  }
}
