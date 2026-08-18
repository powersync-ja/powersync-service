import * as framework from '@powersync/lib-services-framework';
import { migrations } from '@powersync/service-core';
import { migrate } from 'drizzle-orm/better-sqlite3/migrator';
import { SqliteMigrationLockManager } from '../drivers/sqlite/SqliteMigrationLockManager.js';
import { createSqliteDrizzleRuntime, type SqliteDrizzleRuntime } from '../drivers/sqlite/sqlite-config.js';
import { normalizeDrizzleSqliteStorageConfig, type DrizzleStorageConfigDecoded } from '../types/types.js';
import { NoOpMigrationStore } from './NoOpMigrationStore.js';

export const SQLITE_DRIZZLE_MIGRATIONS_PATH = new URL('./sqlite', import.meta.url).pathname;

export function runSqliteDrizzleMigrations(runtime: SqliteDrizzleRuntime): void {
  migrate(runtime.db, { migrationsFolder: SQLITE_DRIZZLE_MIGRATIONS_PATH });
}

export class DrizzleMigrationAgent extends migrations.AbstractPowerSyncMigrationAgent {
  store: framework.MigrationStore = new NoOpMigrationStore();
  locks: framework.LockManager;
  private readonly runtime: SqliteDrizzleRuntime;

  constructor(config: DrizzleStorageConfigDecoded) {
    super();
    this.runtime = createSqliteDrizzleRuntime(normalizeDrizzleSqliteStorageConfig(config));
    this.locks = new SqliteMigrationLockManager({
      name: 'drizzle-migrations',
      runtime: this.runtime
    });
  }

  getInternalScriptsDir(): string {
    return new URL('./scripts', import.meta.url).pathname;
  }

  async run(params: framework.RunMigrationParams<migrations.PowerSyncMigrationGenerics>): Promise<void> {
    if (params.direction != framework.Direction.Up) {
      throw new Error('Drizzle storage migrations only support the up direction');
    }
    await this.locks.init?.();
    const lock = await this.locks.acquire({
      max_wait_ms: params.maxLockWaitMs ?? framework.DEFAULT_MAX_LOCK_WAIT_MS
    });
    if (lock == null) {
      throw new Error('Could not acquire Drizzle migration lock');
    }
    try {
      runSqliteDrizzleMigrations(this.runtime);
    } finally {
      await lock.release();
    }
  }

  async loadInternalMigrations(): Promise<framework.Migration<migrations.PowerSyncMigrationContext>[]> {
    return [];
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.runtime.close();
  }
}
