import * as db from '../../db/db-index.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { BucketStorageProvider, ActiveStorage, GetStorageOptions } from '../StorageProvider.js';
import { PowerSyncMongo } from './db.js';
import { logger } from '@powersync/lib-services-framework';

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
        // TODO currently need the entire resolved config due to this
        slot_name_prefix: resolvedConfig.slot_name_prefix
      }),
      shutDown: () => client.close(),
      tearDown: () => {
        logger.info(`Tearing down storage: ${database.db.namespace}...`);
        return database.db.dropDatabase();
      }
    } satisfies ActiveStorage;
  }
}
