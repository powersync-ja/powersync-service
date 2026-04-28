import * as lib_postgres from '@powersync/lib-service-postgres';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { configFile } from '@powersync/service-types';
import { isPostgresStorageConfig, normalizePostgresStorageConfig, PostgresStorageConfig } from '../types/types.js';
import { getStorageApplicationName } from '../utils/application-name.js';
import { STORAGE_SCHEMA_NAME } from '../utils/db.js';

export const openMigrationDB = (config: configFile.BaseStorageConfig) => {
  if (!isPostgresStorageConfig(config)) {
    throw new ServiceAssertionError(`Input storage configuration is not for Postgres`);
  }
  return new lib_postgres.DatabaseClient({
    config: normalizePostgresStorageConfig(PostgresStorageConfig.decode(config)),
    schema: STORAGE_SCHEMA_NAME,
    applicationName: getStorageApplicationName()
  });
};
