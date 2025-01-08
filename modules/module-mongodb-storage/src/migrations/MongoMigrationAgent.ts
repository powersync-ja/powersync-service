import * as framework from '@powersync/lib-services-framework';

import * as lib_mongo from '@powersync/lib-service-mongodb';
import { migrations } from '@powersync/service-core';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { createPowerSyncMongo, PowerSyncMongo } from '../storage/storage-index.js';
import { MongoStorageConfig } from '../types/types.js';
import { createMongoMigrationStore } from './mongo-migration-store.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MONGO_LOCK_PROCESS = 'migrations';
const MIGRATIONS_DIR = path.join(__dirname, '/db/migrations');

export class MongoMigrationAgent extends migrations.AbstractPowerSyncMigrationAgent {
  store: framework.MigrationStore;
  locks: framework.LockManager;

  protected client: PowerSyncMongo;

  constructor(mongoConfig: MongoStorageConfig) {
    super();

    this.client = createPowerSyncMongo(mongoConfig);

    this.store = createMongoMigrationStore(this.client.db);
    this.locks = new lib_mongo.locks.MongoLockManager({ collection: this.client.locks, name: MONGO_LOCK_PROCESS });
  }

  getInternalScriptsDir(): string {
    return MIGRATIONS_DIR;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.client.client.close();
  }
}
