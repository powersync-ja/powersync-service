import { storage, SUPPORTED_STORAGE_VERSIONS } from '@powersync/service-core';
import {
  createMySqlMikroOrmStorageFactory,
  createSqliteMikroOrmStorageFactory,
  MIKRO_ORM_MYSQL_STORAGE_TYPE,
  MIKRO_ORM_SQLITE_STORAGE_TYPE,
  normalizeMikroOrmMySqlStorageConfig,
  normalizeMikroOrmSqliteStorageConfig
} from '../../src/index.js';
import { env } from './env.js';

const BASE_CONFIG = {
  type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
  filename: ':memory:'
} as const;

export const MIKRO_ORM_SQLITE_STORAGE_FACTORY: storage.TestStorageConfig = {
  tableIdStrings: true,
  factory: async () => {
    const config = normalizeMikroOrmSqliteStorageConfig(BASE_CONFIG);
    const factory = await createSqliteMikroOrmStorageFactory({
      config,
      slotNamePrefix: 'test_'
    });
    await factory.orm.schema.update();
    return factory;
  }
};

export const MIKRO_ORM_MYSQL_STORAGE_FACTORY: storage.TestStorageConfig = {
  tableIdStrings: true,
  factory: async () => {
    const config = normalizeMikroOrmMySqlStorageConfig({
      type: MIKRO_ORM_MYSQL_STORAGE_TYPE,
      uri: env.MIKROORM_MYSQL_STORAGE_TEST_URI
    });
    const factory = await createMySqlMikroOrmStorageFactory({
      config,
      slotNamePrefix: 'test_'
    });
    await dropMySqlStorageTables(factory.orm);
    await factory.orm.schema.create();
    return factory;
  }
};

export const TEST_STORAGE_VERSIONS = SUPPORTED_STORAGE_VERSIONS;

async function dropMySqlStorageTables(orm: Awaited<ReturnType<typeof createMySqlMikroOrmStorageFactory>>['orm']) {
  const connection = orm.em.getConnection();
  for (const table of [
    'write_checkpoints',
    'bucket_parameters',
    'current_data',
    'bucket_data',
    'source_tables',
    'sync_rules',
    'instance'
  ]) {
    await connection.execute(`drop table if exists \`${table}\``, [], 'run');
  }
}
