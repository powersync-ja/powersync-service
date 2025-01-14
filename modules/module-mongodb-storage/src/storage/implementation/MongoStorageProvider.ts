import * as lib_mongo from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { MongoStorageConfig } from '../../types/types.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { PowerSyncMongo } from './db.js';

export class MongoStorageProvider implements storage.BucketStorageProvider {
  get type() {
    return lib_mongo.MONGO_CONNECTION_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { resolvedConfig } = options;

    const { storage } = resolvedConfig;
    if (storage.type != this.type) {
      // This should not be reached since the generation should be managed externally.
      throw new Error(`Cannot create MongoDB bucket storage with provided config ${storage.type} !== ${this.type}`);
    }

    const decodedConfig = MongoStorageConfig.decode(storage as any);
    const client = lib_mongo.db.createMongoClient(decodedConfig);

    const database = new PowerSyncMongo(client, { database: resolvedConfig.storage.database });
    const factory = new MongoBucketStorage(database, {
      // TODO currently need the entire resolved config due to this
      slot_name_prefix: resolvedConfig.slot_name_prefix
    });
    return {
      storage: factory,
      shutDown: async () => {
        await factory[Symbol.asyncDispose]();
        await client.close();
      },
      tearDown: () => {
        logger.info(`Tearing down storage: ${database.db.namespace}...`);
        return database.db.dropDatabase();
      }
    } satisfies storage.ActiveStorage;
  }
}
