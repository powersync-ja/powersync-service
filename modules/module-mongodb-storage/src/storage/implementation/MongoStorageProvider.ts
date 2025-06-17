import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
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
      throw new ServiceAssertionError(
        `Cannot create MongoDB bucket storage with provided config ${storage.type} !== ${this.type}`
      );
    }

    const decodedConfig = MongoStorageConfig.decode(storage as any);
    const client = lib_mongo.db.createMongoClient(decodedConfig, {
      maxPoolSize: resolvedConfig.storage.max_pool_size ?? 8
    });

    let shuttingDown = false;

    // Explicitly connect on startup.
    // Connection errors during startup are typically not recoverable - we get topologyClosed.
    // This helps to catch the error early, along with the cause, and before the process starts
    // to serve API requests.
    // Errors here will cause the process to exit.
    await client.connect();

    const database = new PowerSyncMongo(client, { database: resolvedConfig.storage.database });
    const factory = new MongoBucketStorage(database, {
      // TODO currently need the entire resolved config due to this
      slot_name_prefix: resolvedConfig.slot_name_prefix
    });
    return {
      storage: factory,
      shutDown: async () => {
        shuttingDown = true;
        await factory[Symbol.asyncDispose]();
        await client.close();
      },
      tearDown: () => {
        logger.info(`Tearing down storage: ${database.db.namespace}...`);
        return database.db.dropDatabase();
      },
      onFatalError: (callback) => {
        client.addListener('topologyClosed', () => {
          // If we're shutting down, this is expected and we can ignore it.
          if (!shuttingDown) {
            // Unfortunately there is no simple way to catch the cause of this issue.
            // It most commonly happens when the process fails to _ever_ connect - connection issues after
            // the initial connection are usually recoverable.
            callback(
              new ServiceError({
                code: ErrorCode.PSYNC_S2402,
                description: 'MongoDB topology closed - failed to connect to MongoDB storage.'
              })
            );
          }
        });
      }
    } satisfies storage.ActiveStorage;
  }
}
