import { configFile } from '@powersync/service-types';

import * as db from '../../db/db-index.js';

import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { BaseStorageConfig, GeneratedStorage, StorageProvider } from '../StorageProvider.js';
import { PowerSyncMongo } from './db.js';

export type MongoStorageConfig = configFile.StorageConfig & BaseStorageConfig;
export class MongoStorageProvider implements StorageProvider<MongoStorageConfig> {
  get type() {
    return 'mongodb';
  }

  async generate(config: MongoStorageConfig): Promise<GeneratedStorage> {
    const client = db.mongo.createMongoClient(config);

    const database = new PowerSyncMongo(client, { database: config.database });

    return {
      storage: new MongoBucketStorage(database, {
        slot_name_prefix: config.slot_name_prefix
      }),
      disposer: () => client.close()
    };
  }
}
