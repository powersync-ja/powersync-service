import { configFile } from '@powersync/service-types';

import * as db from '../../db/db-index.js';

import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { GeneratedStorage, StorageGenerationParams, StorageProvider } from '../StorageProvider.js';
import { PowerSyncMongo } from './db.js';

export type MongoStorageConfig = configFile.StorageConfig;

export class MongoStorageProvider implements StorageProvider {
  get type() {
    return 'mongodb';
  }

  async generate(params: StorageGenerationParams): Promise<GeneratedStorage> {
    const { resolved_config } = params;
    const { storage } = resolved_config;
    const client = db.mongo.createMongoClient(resolved_config.storage);

    const database = new PowerSyncMongo(client, { database: storage.database });

    return {
      storage: new MongoBucketStorage(database, {
        // TODO currently need the entire resolved config for this
        slot_name_prefix: params.resolved_config.slot_name_prefix
      }),
      disposer: () => client.close()
    };
  }
}
