import * as db from '../../db/db-index.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { BucketStorageProvider, ActiveStorage, GetStorageOptions } from '../StorageProvider.js';
import { PowerSyncMongo } from './db.js';

export class MongoStorageProvider implements BucketStorageProvider {
  get type() {
    return 'mongodb';
  }

  async getStorage(options: GetStorageOptions): Promise<ActiveStorage> {
    const { resolvedConfig } = options;

    const client = db.mongo.createMongoClient(resolvedConfig.storage);

    const database = new PowerSyncMongo(client, { database: resolvedConfig.storage.database });

    return {
      storage: new MongoBucketStorage(database, {
        // TODO currently need the entire resolved config for this
        slot_name_prefix: resolvedConfig.slot_name_prefix
      }),
      disposer: () => client.close()
    };
  }
}
