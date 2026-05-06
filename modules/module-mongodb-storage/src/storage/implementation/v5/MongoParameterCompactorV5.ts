import { mongo } from '@powersync/lib-service-mongodb';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoParameterCompactorV5 extends MongoParameterCompactor {
  constructor(
    db: VersionedPowerSyncMongoV5,
    group_id: number,
    checkpoint: any,
    options: any
  ) {
    super(
      db,
      group_id,
      checkpoint,
      options,
      () =>
        db
          .listParameterIndexCollectionsV5(group_id)
          .then((collections) => collections.map((c) => c.collection as unknown as mongo.Collection<mongo.Document>))
    );
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
