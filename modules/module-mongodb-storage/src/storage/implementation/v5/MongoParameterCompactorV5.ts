import { mongo } from '@powersync/lib-service-mongodb';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoParameterCompactorV5 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV5;

  protected async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    const collections = await this.db.listParameterIndexCollectionsV5(this.group_id);
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
