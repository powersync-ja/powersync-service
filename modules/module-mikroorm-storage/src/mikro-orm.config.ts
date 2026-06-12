import { resolve } from 'node:path';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createMySqlMikroOrmOptions } from './drivers/mysql/mysql-config.js';
import { createSqliteMikroOrmOptions } from './drivers/sqlite/sqlite-config.js';
import {
  MIKRO_ORM_MYSQL_STORAGE_TYPE,
  MIKRO_ORM_SQLITE_STORAGE_TYPE,
  normalizeMikroOrmMySqlStorageConfig,
  normalizeMikroOrmSqliteStorageConfig
} from './types/types.js';

type MikroOrmStorageDialectName = 'sqlite' | 'mysql';

const moduleRoot = fileURLToPath(new URL('..', import.meta.url));

function getDialect(): MikroOrmStorageDialectName {
  const dialect = process.env.MIKRO_ORM_STORAGE_DIALECT ?? 'sqlite';
  if (dialect == 'sqlite') {
    return dialect;
  }
  if (dialect == 'mysql') {
    return dialect;
  }

  throw new Error(`Unsupported MikroORM storage dialect for migration generation: ${dialect}`);
}

function sqliteConfig() {
  const options = createSqliteMikroOrmOptions(
    normalizeMikroOrmSqliteStorageConfig({
      type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
      filename: process.env.MIKRO_ORM_SQLITE_DB ?? ':memory:'
    })
  );
  const path = resolve(moduleRoot, 'dist/migrations/sqlite');
  const pathTs = resolve(moduleRoot, 'src/migrations/sqlite');
  ensureMigrationDirectories(path, pathTs);

  return {
    ...options,
    migrations: {
      ...options.migrations,
      path,
      pathTs,
      emit: 'ts'
    }
  };
}

function mysqlConfig() {
  const options = createMySqlMikroOrmOptions(
    normalizeMikroOrmMySqlStorageConfig({
      type: MIKRO_ORM_MYSQL_STORAGE_TYPE,
      uri: process.env.MIKRO_ORM_MYSQL_URI ?? 'mysql://repl_user:good_password@localhost:3306/powersync'
    })
  );
  const path = resolve(moduleRoot, 'dist/migrations/mysql');
  const pathTs = resolve(moduleRoot, 'src/migrations/mysql');
  ensureMigrationDirectories(path, pathTs);

  return {
    ...options,
    migrations: {
      ...options.migrations,
      path,
      pathTs,
      emit: 'ts'
    }
  };
}

function ensureMigrationDirectories(...paths: string[]): void {
  for (const path of paths) {
    mkdirSync(path, { recursive: true });
  }
}

const configs = {
  sqlite: sqliteConfig,
  mysql: mysqlConfig
} satisfies Record<MikroOrmStorageDialectName, () => unknown>;

const selectedConfig: unknown = configs[getDialect()]();

export default selectedConfig;
