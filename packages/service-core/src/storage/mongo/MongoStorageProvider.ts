import { logger } from '@powersync/lib-services-framework';
import * as db from '../../db/db-index.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { ActiveStorage, BucketStorageProvider, GetStorageOptions } from '../StorageProvider.js';
import { PowerSyncMongo } from './db.js';

export class MongoStorageProvider implements BucketStorageProvider {
  get type() {
    return 'mongodb';
  }

  async getStorage(options: GetStorageOptions): Promise<ActiveStorage> {
    const { eventManager, resolvedConfig } = options;

    const client = db.mongo.createMongoClient(resolvedConfig.storage);

    const database = new PowerSyncMongo(client, { database: resolvedConfig.storage.database });

    return {
      storage: new MongoBucketStorage(database, {
        // TODO currently need the entire resolved config due to this
        slot_name_prefix: resolvedConfig.slot_name_prefix,
        event_manager: eventManager,
        write_checkpoint_mode: options.writeCheckpointMode
      }),
      shutDown: () => client.close(),
      tearDown: () => {
        logger.info(`Tearing down storage: ${database.db.namespace}...`);
        return database.db.dropDatabase();
      }
    } satisfies ActiveStorage;
  }
}
