import * as lib_postgres from '@powersync/lib-service-postgres';
import * as framework from '@powersync/lib-services-framework';
import { migrations } from '@powersync/service-core';
import * as path from 'path';
import { fileURLToPath } from 'url';

import { normalizePostgresStorageConfig, PostgresStorageConfigDecoded } from '../types/types.js';

import { STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { PostgresMigrationStore } from './PostgresMigrationStore.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MIGRATIONS_DIR = path.join(__dirname, 'scripts');

export class PostgresMigrationAgent extends migrations.AbstractPowerSyncMigrationAgent {
  store: framework.MigrationStore;
  locks: framework.LockManager;

  protected db: lib_postgres.DatabaseClient;

  constructor(config: PostgresStorageConfigDecoded) {
    super();

    this.db = new lib_postgres.DatabaseClient({
      config: normalizePostgresStorageConfig(config),
      schema: STORAGE_SCHEMA_NAME
    });
    this.store = new PostgresMigrationStore({
      db: this.db
    });
    this.locks = new lib_postgres.PostgresLockManager({
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
