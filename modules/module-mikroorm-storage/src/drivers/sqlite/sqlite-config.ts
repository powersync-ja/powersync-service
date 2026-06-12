import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, MikroORM, SqliteDriver } from '@mikro-orm/sqlite';
import { NormalizedMikroOrmSqliteStorageConfig } from '../../types/types.js';
import { sqliteMikroOrmStorageDialect } from './sqlite-dialect.js';

export const SQLITE_MIKRO_ORM_MIGRATIONS_PATH = new URL('../../migrations/sqlite', import.meta.url).pathname;

export function createSqliteMikroOrmOptions(config: NormalizedMikroOrmSqliteStorageConfig) {
  const fileBacked = config.filename != ':memory:';
  const readReplicaCount = fileBacked ? Math.max(0, config.max_pool_size - 1) : 0;

  return defineConfig({
    driver: SqliteDriver,
    dbName: config.filename,
    entities: sqliteMikroOrmStorageDialect.entityClasses,
    onCreateConnection: (connection) =>
      configureSqliteConnection(connection, {
        enableWal: fileBacked
      }),
    replicas: Array.from({ length: readReplicaCount }, (_, index) => ({
      name: `reader-${index + 1}`
    })),
    extensions: [Migrator],
    migrations: {
      path: SQLITE_MIKRO_ORM_MIGRATIONS_PATH,
      pathTs: SQLITE_MIKRO_ORM_MIGRATIONS_PATH,
      glob: '!(*.d).{js,ts}',
      emit: 'ts',
      snapshot: false
    }
  });
}

export async function createSqliteMikroOrm(config: NormalizedMikroOrmSqliteStorageConfig): Promise<MikroORM> {
  return MikroORM.init(createSqliteMikroOrmOptions(config));
}

interface SqliteKyselyConnection {
  executeQuery(query: { sql: string; parameters: unknown[] }): Promise<unknown>;
}

async function configureSqliteConnection(
  connection: unknown,
  options: {
    enableWal: boolean;
  }
): Promise<void> {
  const sqliteConnection = connection as SqliteKyselyConnection;

  if (options.enableWal) {
    await sqliteConnection.executeQuery({ sql: 'PRAGMA journal_mode = WAL', parameters: [] });
  }

  await sqliteConnection.executeQuery({ sql: 'PRAGMA busy_timeout = 5000', parameters: [] });
}
