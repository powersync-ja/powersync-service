import { storage } from '@powersync/service-core';
import { migrate } from 'drizzle-orm/better-sqlite3/migrator';
import {
  createSqliteDrizzleStorageFactory,
  DRIZZLE_SQLITE_STORAGE_TYPE,
  normalizeDrizzleSqliteStorageConfig,
  SQLITE_DRIZZLE_MIGRATIONS_PATH
} from '../../src/index.js';

const BASE_CONFIG = {
  type: DRIZZLE_SQLITE_STORAGE_TYPE,
  filename: ':memory:'
} as const;

export const DRIZZLE_SQLITE_STORAGE_FACTORY: storage.TestStorageConfig = {
  tableIdStrings: true,
  factory: async () => {
    const factory = createSqliteDrizzleStorageFactory({
      config: normalizeDrizzleSqliteStorageConfig(BASE_CONFIG),
      slotNamePrefix: 'test_'
    });
    migrate(factory.runtime.db, { migrationsFolder: SQLITE_DRIZZLE_MIGRATIONS_PATH });
    return factory;
  }
};

export const TEST_STORAGE_VERSIONS = [storage.LEGACY_STORAGE_VERSION];
