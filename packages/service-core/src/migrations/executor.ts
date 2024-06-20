import { logger } from '@powersync/lib-services-framework';
import * as defs from './definitions.js';
import { MigrationStore } from './store/migration-store.js';

type ExecuteParams = {
  migrations: defs.Migration[];
  state?: defs.MigrationState;

  direction: defs.Direction;
  count?: number;
};

export async function* execute(params: ExecuteParams): AsyncGenerator<defs.ExecutedMigration> {
  let migrations = [...params.migrations];
  if (params.direction === defs.Direction.Down) {
    migrations = migrations.reverse();
  }

  let index = 0;

  if (params.state) {
    // Find the index of the last run
    index = migrations.findIndex((migration) => {
      return migration.name === params.state!.last_run;
    });

    if (index === -1) {
      throw new Error(`The last run migration ${params.state?.last_run} was not found in the given set of migrations`);
    }

    // If we are migrating down then we want to include the last run migration, otherwise we want to start at the next one
    if (params.direction === defs.Direction.Up) {
      index += 1;
    }
  }

  migrations = migrations.slice(index);

  let i = 0;
  for (const migration of migrations) {
    if (params.count && params.count === i) {
      return;
    }

    logger.info(`Executing ${migration.name} (${params.direction})`);
    try {
      switch (params.direction) {
        case defs.Direction.Up: {
          await migration.up();
          break;
        }
        case defs.Direction.Down: {
          await migration.down();
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

type WriteLogsParams = {
  store: MigrationStore;
  state?: defs.MigrationState;
  log_stream: Iterable<defs.ExecutedMigration> | AsyncIterable<defs.ExecutedMigration>;
};
export const writeLogsToStore = async (params: WriteLogsParams): Promise<void> => {
  const log = [...(params.state?.log || [])];
  for await (const migration of params.log_stream) {
    log.push(migration);
    await params.store.save({
      last_run: migration.name,
      log: log
    });
  }
};
