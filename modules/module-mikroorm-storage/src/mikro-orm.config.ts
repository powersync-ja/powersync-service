import type { Options } from '@mikro-orm/sqlite';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createSqliteMikroOrmOptions } from './drivers/sqlite/sqlite-config.js';
import { MIKRO_ORM_SQLITE_STORAGE_TYPE, normalizeMikroOrmSqliteStorageConfig } from './types/types.js';

type MikroOrmStorageDialectName = 'sqlite';

const moduleRoot = fileURLToPath(new URL('..', import.meta.url));

function getDialect(): MikroOrmStorageDialectName {
  const dialect = process.env.MIKRO_ORM_STORAGE_DIALECT ?? 'sqlite';
  if (dialect == 'sqlite') {
    return dialect;
  }

  throw new Error(`Unsupported MikroORM storage dialect for migration generation: ${dialect}`);
}

function sqliteConfig(): Options {
  const options = createSqliteMikroOrmOptions(
    normalizeMikroOrmSqliteStorageConfig({
      type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
      filename: process.env.MIKRO_ORM_SQLITE_DB ?? ':memory:'
    })
  );

  return {
    ...options,
    migrations: {
      ...options.migrations,
      path: resolve(moduleRoot, 'dist/migrations/sqlite'),
      pathTs: resolve(moduleRoot, 'src/migrations/sqlite'),
      emit: 'ts'
    }
  };
}

const configs = {
  sqlite: sqliteConfig
} satisfies Record<MikroOrmStorageDialectName, () => Options>;

export default configs[getDialect()]();
