import * as framework from '@powersync/lib-services-framework';

import { configFile } from '@powersync/service-types';
import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { createMongoLockManager } from '../../locks/MongoLocks.js';
import { createPowerSyncMongo, PowerSyncMongo } from '../../storage/storage-index.js';
import { createMongoMigrationStore } from './mongo-migration-store.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MONGO_LOCK_PROCESS = 'migrations';
const MIGRATIONS_DIR = path.join(__dirname, '../db/migrations');

export class MongoMigrationAgent extends framework.AbstractMigrationAgent {
  store: framework.MigrationStore;
  locks: framework.LockManager;

  protected client: PowerSyncMongo;

  constructor(mongoConfig: configFile.MongoStorageConfig) {
    super();

    this.client = createPowerSyncMongo(mongoConfig);

    this.store = createMongoMigrationStore(this.client.db);
    this.locks = createMongoLockManager(this.client.locks, { name: MONGO_LOCK_PROCESS });
  }

  async loadInternalMigrations(): Promise<framework.Migration[]> {
    const files = await fs.readdir(MIGRATIONS_DIR);
    const migrations = files.filter((file) => {
      return path.extname(file) === '.js';
    });

    return await Promise.all(
      migrations.map(async (migration) => {
        const module = await import(path.resolve(MIGRATIONS_DIR, migration));
        return {
          name: path.basename(migration).replace(path.extname(migration), ''),
          up: module.up,
          down: module.down
        };
      })
    );
  }

  async dispose(): Promise<void> {
    await this.client.client.close();
  }
}
