import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';

export const SLATEDB_STORAGE_TYPE = 'slatedb';

export const SlateDBObjectStoreConfig = t.union(
  t.union(
    t.object({
      type: t.literal('memory')
    }),
    t.object({
      type: t.literal('file'),
      path: t.string.meta({
        description: 'Filesystem path used as the root for SlateDB object storage.'
      })
    })
  ),
  t.union(
    t.object({
      type: t.literal('s3'),
      bucket: t.string.meta({
        description: 'S3 bucket used for SlateDB object storage.'
      }),
      prefix: t.string
        .meta({
          description: 'Optional key prefix for SlateDB data in the S3 bucket.'
        })
        .optional()
    }),
    t.object({
      url: t.string.meta({
        description:
          'Object store URL passed directly to SlateDB ObjectStore.resolve(), for example memory:/// or s3://bucket.'
      })
    })
  )
);

export const SlateDBStorageConfig = configFile.BaseStorageConfig.and(
  t.object({
    type: t.literal(SLATEDB_STORAGE_TYPE),
    path: t.string
      .meta({
        description: 'Deprecated local filesystem shortcut. Prefer object_store: { type: "file", path: "..." }.'
      })
      .optional(),
    db_path: t.string
      .meta({
        description: 'Database path inside the configured SlateDB object store. Defaults to "powersync".'
      })
      .optional(),
    object_store: SlateDBObjectStoreConfig.optional()
  })
);

export type SlateDBStorageConfig = t.Encoded<typeof SlateDBStorageConfig>;
export type SlateDBStorageConfigDecoded = t.Decoded<typeof SlateDBStorageConfig>;

export const isSlateDBStorageConfig = (config: configFile.GenericStorageConfig): config is SlateDBStorageConfig => {
  return config.type == SLATEDB_STORAGE_TYPE;
};
