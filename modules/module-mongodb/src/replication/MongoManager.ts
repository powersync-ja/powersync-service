import { mongo } from '@powersync/lib-service-mongodb';

import { NormalizedMongoConnectionConfig } from '../types/types.js';
import { BSON_DESERIALIZE_DATA_OPTIONS, POWERSYNC_VERSION } from '@powersync/service-core';
import { BaseObserver } from '@powersync/lib-services-framework';

export interface MongoManagerListener {
  onEnded(): void;
}

/**
 * Manage a MongoDB source database connection.
 */
export class MongoManager extends BaseObserver<MongoManagerListener> {
  public readonly client: mongo.MongoClient;
  public readonly db: mongo.Db;

  constructor(
    public options: NormalizedMongoConnectionConfig,
    overrides?: mongo.MongoClientOptions
  ) {
    super();
    // The pool is lazy - no connections are opened until a query is performed.
    this.client = new mongo.MongoClient(options.uri, {
      auth: {
        username: options.username,
        password: options.password
      },

      lookup: options.lookup,
      // Time for connection to timeout
      connectTimeoutMS: 5_000,
      // Time for individual requests to timeout
      socketTimeoutMS: 60_000,
      // How long to wait for new primary selection
      serverSelectionTimeoutMS: 30_000,

      // Identify the client
      appName: `powersync ${POWERSYNC_VERSION}`,
      // Deprecated in the driver - in a future release we may have to rely on appName only.
      driverInfo: {
        // This is merged with the node driver info.
        name: 'powersync',
        version: POWERSYNC_VERSION
      },

      // Avoid too many connections:
      // 1. It can overwhelm the source database.
      // 2. Processing too many queries in parallel can cause the process to run out of memory.
      maxPoolSize: 8,

      maxConnecting: 3,
      maxIdleTimeMS: 60_000,

      ...BSON_DESERIALIZE_DATA_OPTIONS,

      ...overrides
    });
    this.db = this.client.db(options.database, {});
  }

  public get connectionTag() {
    return this.options.tag;
  }

  async end(): Promise<void> {
    await this.client.close();
    this.iterateListeners((listener) => {
      listener.onEnded?.();
    });
  }
}
