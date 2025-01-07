import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { configFile } from '@powersync/service-types';
import * as db from '../../db/db-index.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { PowerSyncMongo } from './db.js';

export const MONGO_STORAGE_TYPE = 'mongodb';
export class MongoStorageProvider implements storage.BucketStorageProvider {
  get type() {
    return MONGO_STORAGE_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { resolvedConfig } = options;

    const { storage } = resolvedConfig;
    if (storage.type != MONGO_STORAGE_TYPE) {
      // This should not be reached since the generation should be managed externally.
      throw new Error(
        `Cannot create MongoDB bucket storage with provided config ${storage.type} !== ${MONGO_STORAGE_TYPE}`
      );
    }

    const decodedConfig = configFile.MongoStorageConfig.decode(storage as any);
    const client = db.mongo.createMongoClient(decodedConfig);

    const database = new PowerSyncMongo(client, { database: resolvedConfig.storage.database });

    return {
      storage: new MongoBucketStorage(database, {
        // TODO currently need the entire resolved config due to this
        slot_name_prefix: resolvedConfig.slot_name_prefix
      }),
      shutDown: () => client.close(),
      tearDown: () => {
        logger.info(`Tearing down storage: ${database.db.namespace}...`);
        return database.db.dropDatabase();
      }
    } satisfies storage.ActiveStorage;
  }
}
