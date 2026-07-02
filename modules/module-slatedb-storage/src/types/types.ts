import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';

export const SLATEDB_STORAGE_TYPE = 'slatedb';

export const SlateDBStorageConfig = configFile.BaseStorageConfig.and(
  t.object({
    type: t.literal(SLATEDB_STORAGE_TYPE),
    path: t.string.meta({
      description: 'Filesystem path used by the SlateDB storage backend.'
    })
  })
);

export type SlateDBStorageConfig = t.Encoded<typeof SlateDBStorageConfig>;
export type SlateDBStorageConfigDecoded = t.Decoded<typeof SlateDBStorageConfig>;

export const isSlateDBStorageConfig = (config: configFile.GenericStorageConfig): config is SlateDBStorageConfig => {
  return config.type == SLATEDB_STORAGE_TYPE;
};
