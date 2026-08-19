import { mongo } from '@powersync/lib-service-mongodb';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoParameterCompactorV1 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV1;

  protected async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    return [this.db.parameterIndexV1 as unknown as mongo.Collection<mongo.Document>];
  }

  protected override shouldCompactDocument(doc: { _id: bigint; key: mongo.Document }): boolean {
    return doc._id < this.checkpoint && doc.key.g === this.group_id;
  }

  protected deleteFilter(doc: mongo.Document): mongo.Document {
    return {
      'key.g': doc.key.g as number,
      lookup: doc.lookup,
      _id: { $lt: doc._id },
      key: doc.key
    };
  }

  protected deleteTombstoneFilter(doc: mongo.Document): mongo.Document {
    return {
      'key.g': doc.key.g as number,
      lookup: doc.lookup,
      _id: doc._id,
      key: doc.key
    };
  }
}
