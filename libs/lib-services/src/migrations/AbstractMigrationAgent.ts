import { LockManager } from '../locks/LockManager.js';
import { logger } from '../logger/Logger.js';
import * as defs from './migration-definitions.js';

export type MigrationParams<Generics extends MigrationAgentGenerics = MigrationAgentGenerics> = {
  count?: number;
  direction: defs.Direction;
  migrationContext?: Generics['MIGRATION_CONTEXT'];
};

type WriteLogsParams = {
  state?: defs.MigrationState;
  log_stream: Iterable<defs.ExecutedMigration> | AsyncIterable<defs.ExecutedMigration>;
};

export type MigrationAgentGenerics = {
  MIGRATION_CONTEXT?: {};
};

export type RunMigrationParams<Generics extends MigrationAgentGenerics = MigrationAgentGenerics> = MigrationParams & {
  migrations: defs.Migration<Generics['MIGRATION_CONTEXT']>[];
};

type ExecuteParams = RunMigrationParams & {
  state?: defs.MigrationState;
};

export abstract class AbstractMigrationAgent<Generics extends MigrationAgentGenerics = MigrationAgentGenerics> {
  abstract get store(): defs.MigrationStore;
  abstract get locks(): LockManager;

  abstract loadInternalMigrations(): Promise<defs.Migration<Generics['MIGRATION_CONTEXT']>[]>;

  abstract dispose(): Promise<void>;

  async run(params: RunMigrationParams) {
    const { direction, migrations, migrationContext } = params;
    // Only one process should execute this at a time.
    logger.info('Acquiring lock');
    const lockId = await this.locks.acquire();

    if (!lockId) {
      throw new Error('Could not acquire lock_id');
    }

    let isReleased = false;
    const releaseLock = async () => {
      if (isReleased) {
        return;
      }
      await this.locks.release(lockId);
      isReleased = true;
    };

    // For the case where the migration is terminated
    process.addListener('beforeExit', releaseLock);

    try {
      const state = await this.store.load();

      logger.info('Running migrations');
      const logStream = this.execute({
        direction,
        migrations,
        state,
        migrationContext
      });

      await this.writeLogsToStore({
        log_stream: logStream,
        state
      });
    } finally {
      logger.info('Releasing lock');
      await releaseLock();
      process.removeListener('beforeExit', releaseLock);
      logger.info('Done with migrations');
    }
  }

  protected async *execute(params: ExecuteParams): AsyncGenerator<defs.ExecutedMigration> {
    const internalMigrations = await this.loadInternalMigrations();
    let migrations = [...internalMigrations, ...params.migrations];

    if (params.direction === defs.Direction.Down) {
      migrations.reverse();
    }

    let index = 0;

    if (params.state) {
      // Find the index of the last run
      index = migrations.findIndex((migration) => {
        return migration.name === params.state!.last_run;
      });

      if (index === -1) {
        throw new Error(
          `The last run migration ${params.state?.last_run} was not found in the given set of migrations`
        );
      }

      // If we are migrating down then we want to include the last run migration, otherwise we want to start at the next one
      if (params.direction === defs.Direction.Up) {
        index += 1;
      }
    }

    migrations = migrations.slice(index);

    let i = 0;
    const { migrationContext } = params;
    for (const migration of migrations) {
      if (params.count && params.count === i) {
        return;
      }

      logger.info(`Executing ${migration.name} (${params.direction})`);
      try {
        switch (params.direction) {
          case defs.Direction.Up: {
            await migration.up(migrationContext);
            break;
          }
          case defs.Direction.Down: {
            await migration.down(migrationContext);
            break;
          }
        }
        logger.debug(`Success`);
      } catch (err) {
        logger.error(`Failed`, err);
        process.exit(1);
      }

      yield {
        name: migration.name,
        direction: params.direction,
        timestamp: new Date()
      };

      i++;
    }
  }

  protected writeLogsToStore = async (params: WriteLogsParams): Promise<void> => {
    const log = [...(params.state?.log || [])];
    for await (const migration of params.log_stream) {
      log.push(migration);
      await this.store.save({
        last_run: migration.name,
        log: log
      });
    }
  };
}
