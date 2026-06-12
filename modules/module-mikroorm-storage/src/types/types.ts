import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';

export const MIKRO_ORM_SQLITE_STORAGE_TYPE = 'mikroorm:sqlite';

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

export const isMikroOrmSqliteStorageConfig = (
  config: configFile.GenericStorageConfig
): config is MikroOrmSqliteStorageConfig => {
  return config.type == MIKRO_ORM_SQLITE_STORAGE_TYPE;
};

export interface NormalizedMikroOrmSqliteStorageConfig {
  type: typeof MIKRO_ORM_SQLITE_STORAGE_TYPE;
  filename: string;
  max_pool_size: number;
}

export function normalizeMikroOrmSqliteStorageConfig(
  config: MikroOrmSqliteStorageConfigDecoded
): NormalizedMikroOrmSqliteStorageConfig {
  return {
    type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
    filename: config.filename,
    max_pool_size: config.max_pool_size ?? 1
  };
}
