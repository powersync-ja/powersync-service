import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, MikroORM, SqliteDriver } from '@mikro-orm/sqlite';
import { NormalizedMikroOrmSqliteStorageConfig } from '../../types/types.js';
import { sqliteMikroOrmStorageDialect } from './sqlite-dialect.js';

export const SQLITE_MIKRO_ORM_MIGRATIONS_PATH = new URL('../../migrations/sqlite', import.meta.url).pathname;

export function createSqliteMikroOrmOptions(config: NormalizedMikroOrmSqliteStorageConfig) {
  return defineConfig({
    driver: SqliteDriver,
    dbName: config.filename,
    entities: sqliteMikroOrmStorageDialect.entityClasses,
    extensions: [Migrator],
    migrations: {
      path: config.migrations_path ?? SQLITE_MIKRO_ORM_MIGRATIONS_PATH,
      pathTs: config.migrations_path ?? SQLITE_MIKRO_ORM_MIGRATIONS_PATH,
      glob: '!(*.d).{js,ts}',
      emit: 'ts',
      snapshot: false
    }
  });
}

export async function createSqliteMikroOrm(config: NormalizedMikroOrmSqliteStorageConfig): Promise<MikroORM> {
  return MikroORM.init(createSqliteMikroOrmOptions(config));
}
