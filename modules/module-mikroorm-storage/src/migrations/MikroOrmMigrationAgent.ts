import { MikroORM } from '@mikro-orm/core';
import * as framework from '@powersync/lib-services-framework';
import { migrations } from '@powersync/service-core';
import { createSqliteMikroOrm } from '../drivers/sqlite/sqlite-config.js';
import { SqliteMigrationLockManager } from '../drivers/sqlite/SqliteMigrationLockManager.js';
import { MikroOrmSqliteStorageConfigDecoded, normalizeMikroOrmSqliteStorageConfig } from '../types/types.js';
import { NoOpMigrationStore } from './NoOpMigrationStore.js';

/**
 * Service migration agent that delegates schema changes to MikroORM migrations.
 *
 * PowerSync still owns orchestration and locking, but MikroORM owns migration discovery and migration state. The
 * lock is acquired first to avoid multiple service instances running the same MikroORM migration concurrently.
 */
export class MikroOrmMigrationAgent extends migrations.AbstractPowerSyncMigrationAgent {
  store: framework.MigrationStore;
  locks: framework.LockManager;

  private readonly ormPromise: Promise<MikroORM>;

  constructor(config: MikroOrmSqliteStorageConfigDecoded) {
    super();
    this.ormPromise = createSqliteMikroOrm(normalizeMikroOrmSqliteStorageConfig(config));
    this.store = new NoOpMigrationStore();
    this.locks = new SqliteMigrationLockManager({
      name: 'mikroorm-migrations',
      orm: this.ormPromise
    });
  }

  getInternalScriptsDir(): string {
    return new URL('./scripts', import.meta.url).pathname;
  }

  async run(params: framework.RunMigrationParams<migrations.PowerSyncMigrationGenerics>): Promise<void> {
    await this.locks.init?.();

    const logger = params.logger ?? framework.logger;
    logger.debug('Acquiring lock for MikroORM migrations');
    const lockHandle = await this.locks.acquire({
      max_wait_ms: params.maxLockWaitMs ?? framework.DEFAULT_MAX_LOCK_WAIT_MS
    });

    if (lockHandle == null) {
      throw new Error('Could not acquire MikroORM migration lock');
    }

    let isReleased = false;
    const releaseLock = async () => {
      if (isReleased) {
        return;
      }
      await lockHandle.release();
      isReleased = true;
    };

    process.addListener('beforeExit', releaseLock);

    try {
      if (params.migrations.length > 0) {
        logger.warn('Ignoring PowerSync migration list for MikroORM storage; MikroORM migrator owns migration state.');
      }

      const orm = await this.ormPromise;
      const migrator = orm.migrator;

      logger.info(`Running MikroORM migrations ${params.direction}`);
      if (migrator != null) {
        if (params.direction == framework.Direction.Up) {
          await migrator.up();
        } else {
          await migrator.down();
        }
      } else if (params.direction == framework.Direction.Up) {
        await orm.schema.update();
      }
    } finally {
      logger.debug('Releasing MikroORM migration lock');
      await releaseLock();
      process.removeListener('beforeExit', releaseLock);
      logger.debug('Done with MikroORM migrations');
    }
  }

  async loadInternalMigrations(): Promise<framework.Migration<migrations.PowerSyncMigrationContext>[]> {
    return [];
  }

  async [Symbol.asyncDispose](): Promise<void> {
    const orm = await this.ormPromise;
    await orm.close(true);
  }
}
