import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';

export const MIKRO_ORM_SQLITE_STORAGE_TYPE = 'mikroorm:sqlite';
export const MIKRO_ORM_MYSQL_STORAGE_TYPE = 'mikroorm:mysql';

export const MikroOrmSqliteStorageConfig = configFile.BaseStorageConfig.and(
  t.object({
    type: t.literal(MIKRO_ORM_SQLITE_STORAGE_TYPE),
    /**
     * SQLite database file. Use ":memory:" for in-memory storage.
     */
    filename: t.string
  })
);

export type MikroOrmSqliteStorageConfig = t.Encoded<typeof MikroOrmSqliteStorageConfig>;
export type MikroOrmSqliteStorageConfigDecoded = t.Decoded<typeof MikroOrmSqliteStorageConfig>;

export const MikroOrmMySqlStorageConfig = configFile.BaseStorageConfig.and(
  t.object({
    type: t.literal(MIKRO_ORM_MYSQL_STORAGE_TYPE),
    /**
     * MySQL connection URI, for example mysql://root:password@localhost:3306/powersync_storage.
     */
    uri: t.string
  })
);

export type MikroOrmMySqlStorageConfig = t.Encoded<typeof MikroOrmMySqlStorageConfig>;
export type MikroOrmMySqlStorageConfigDecoded = t.Decoded<typeof MikroOrmMySqlStorageConfig>;

export const MikroOrmStorageConfig = MikroOrmSqliteStorageConfig.or(MikroOrmMySqlStorageConfig);
export type MikroOrmStorageConfig = t.Encoded<typeof MikroOrmStorageConfig>;
export type MikroOrmStorageConfigDecoded = t.Decoded<typeof MikroOrmStorageConfig>;

export const isMikroOrmSqliteStorageConfig = (
  config: configFile.GenericStorageConfig
): config is MikroOrmSqliteStorageConfig => {
  return config.type == MIKRO_ORM_SQLITE_STORAGE_TYPE;
};

export const isMikroOrmMySqlStorageConfig = (
  config: configFile.GenericStorageConfig
): config is MikroOrmMySqlStorageConfig => {
  return config.type == MIKRO_ORM_MYSQL_STORAGE_TYPE;
};

export const isMikroOrmStorageConfig = (config: configFile.GenericStorageConfig): config is MikroOrmStorageConfig => {
  return isMikroOrmSqliteStorageConfig(config) || isMikroOrmMySqlStorageConfig(config);
};

export interface NormalizedMikroOrmSqliteStorageConfig {
  type: typeof MIKRO_ORM_SQLITE_STORAGE_TYPE;
  filename: string;
  max_pool_size: number;
}

export interface NormalizedMikroOrmMySqlStorageConfig {
  type: typeof MIKRO_ORM_MYSQL_STORAGE_TYPE;
  uri: string;
  max_pool_size: number;
}

export function normalizeMikroOrmSqliteStorageConfig(
  config: MikroOrmSqliteStorageConfigDecoded
): NormalizedMikroOrmSqliteStorageConfig {
  return {
    type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
    filename: config.filename,
    max_pool_size: config.max_pool_size ?? 10
  };
}

export function normalizeMikroOrmMySqlStorageConfig(
  config: MikroOrmMySqlStorageConfigDecoded
): NormalizedMikroOrmMySqlStorageConfig {
  return {
    type: MIKRO_ORM_MYSQL_STORAGE_TYPE,
    uri: config.uri,
    max_pool_size: config.max_pool_size ?? 10
  };
}
