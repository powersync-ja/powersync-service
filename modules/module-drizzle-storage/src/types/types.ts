import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';

export const DRIZZLE_SQLITE_STORAGE_TYPE = 'drizzle:sqlite';

export const DrizzleSqliteStorageConfig = configFile.BaseStorageConfig.and(
  t.object({
    type: t.literal(DRIZZLE_SQLITE_STORAGE_TYPE),
    filename: t.string
  })
);

export type DrizzleSqliteStorageConfig = t.Encoded<typeof DrizzleSqliteStorageConfig>;
export type DrizzleSqliteStorageConfigDecoded = t.Decoded<typeof DrizzleSqliteStorageConfig>;
export const DrizzleStorageConfig = DrizzleSqliteStorageConfig;
export type DrizzleStorageConfig = DrizzleSqliteStorageConfig;
export type DrizzleStorageConfigDecoded = DrizzleSqliteStorageConfigDecoded;

export interface NormalizedDrizzleSqliteStorageConfig {
  type: typeof DRIZZLE_SQLITE_STORAGE_TYPE;
  filename: string;
  max_pool_size: number;
}

export function isDrizzleStorageConfig(config: configFile.GenericStorageConfig): config is DrizzleSqliteStorageConfig {
  return config.type == DRIZZLE_SQLITE_STORAGE_TYPE;
}

export function normalizeDrizzleSqliteStorageConfig(
  config: DrizzleSqliteStorageConfigDecoded
): NormalizedDrizzleSqliteStorageConfig {
  return {
    type: DRIZZLE_SQLITE_STORAGE_TYPE,
    filename: config.filename,
    max_pool_size: config.max_pool_size ?? 10
  };
}
