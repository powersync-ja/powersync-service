import { storage, SUPPORTED_STORAGE_VERSIONS } from '@powersync/service-core';
import {
  createSqliteMikroOrmStorageFactory,
  MIKRO_ORM_SQLITE_STORAGE_TYPE,
  normalizeMikroOrmSqliteStorageConfig
} from '../../src/index.js';

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

export const TEST_STORAGE_VERSIONS = SUPPORTED_STORAGE_VERSIONS;
