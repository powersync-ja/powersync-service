import { mongo } from '@powersync/lib-service-mongodb';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoParameterCompactorV3 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;

  protected async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    const collections = await this.db.listParameterIndexCollectionsV3(this.group_id);
    return collections.map((collection) => collection.collection as unknown as mongo.Collection<mongo.Document>);
  }

  protected collectionFilter(): mongo.Document {
    return {};
  }

  protected deleteFilter(doc: mongo.Document): mongo.Document {
    return {
      lookup: doc.lookup,
      _id: { $lte: doc._id },
      key: doc.key
    };
  }
}
