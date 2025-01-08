import * as framework from '@powersync/lib-services-framework';
import { migrations } from '@powersync/service-core';
import * as pg_types from '@powersync/service-module-postgres/types';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { PostgresLockManager } from '../locks/PostgresLockManager.js';
import { DatabaseClient } from '../utils/connection/DatabaseClient.js';
import { PostgresMigrationStore } from './PostgresMigrationStore.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MIGRATIONS_DIR = path.join(__dirname, 'scripts');

export class PostgresMigrationAgent extends migrations.AbstractPowerSyncMigrationAgent {
  store: framework.MigrationStore;
  locks: framework.LockManager;

  protected db: DatabaseClient;

  constructor(config: pg_types.PostgresConnectionConfig) {
    super();

    this.db = new DatabaseClient(pg_types.normalizeConnectionConfig(config));
    this.store = new PostgresMigrationStore({
      db: this.db
    });
    this.locks = new PostgresLockManager({
      name: 'migrations',
      db: this.db
    });
  }

  getInternalScriptsDir(): string {
    return MIGRATIONS_DIR;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.db[Symbol.asyncDispose]();
  }
}
