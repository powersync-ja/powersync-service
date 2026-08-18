import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, MikroORM, MySqlDriver } from '@mikro-orm/mysql';
import { configureMySqlEntityColumnTypes } from '../../entities/entity-column-types.js';
import { NormalizedMikroOrmMySqlStorageConfig } from '../../types/types.js';
import { mysqlMikroOrmStorageDialect } from './mysql-dialect.js';

export const MYSQL_MIKRO_ORM_MIGRATIONS_PATH = new URL('../../migrations/mysql', import.meta.url).pathname;

export function createMySqlMikroOrmOptions(config: NormalizedMikroOrmMySqlStorageConfig) {
  configureMySqlEntityColumnTypes();

  return defineConfig({
    driver: MySqlDriver,
    clientUrl: config.uri,
    entities: mysqlMikroOrmStorageDialect.entityClasses,
    pool: {
      min: 0,
      max: config.max_pool_size
    },
    extensions: [Migrator],
    migrations: {
      path: MYSQL_MIKRO_ORM_MIGRATIONS_PATH,
      pathTs: MYSQL_MIKRO_ORM_MIGRATIONS_PATH,
      glob: '!(*.d).{js,ts}',
      emit: 'ts',
      snapshot: false,
      dropTables: false,
      // MySQL DDL performs implicit commits, so wrapping generated migrations in
      // MikroORM transactions/savepoints can leave the driver trying to roll
      // back a savepoint that MySQL has already discarded.
      transactional: false,
      allOrNothing: false
    }
  });
}

export async function createMySqlMikroOrm(config: NormalizedMikroOrmMySqlStorageConfig): Promise<MikroORM> {
  return MikroORM.init(createMySqlMikroOrmOptions(config));
}
