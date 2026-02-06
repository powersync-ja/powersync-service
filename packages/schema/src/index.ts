import { MongoStorageConfig } from '@powersync/service-module-mongodb-storage/types';
import { MongoConnectionConfig } from '@powersync/service-module-mongodb/types';
import { MySQLConnectionConfig } from '@powersync/service-module-mysql/types';
import { PostgresStorageConfig } from '@powersync/service-module-postgres-storage';
import { PostgresConnectionConfig } from '@powersync/service-module-postgres/types';
import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';

// Merge configs for modules
const baseShape = configFile.powerSyncConfig.props.shape;

const mergedDataSourceConfig = configFile.genericDataSourceConfig
  .or(PostgresConnectionConfig)
  .or(MongoConnectionConfig)
  .or(MySQLConnectionConfig);

const mergedStorageConfig = configFile.GenericStorageConfig.or(PostgresStorageConfig).or(MongoStorageConfig);

export const MergedServiceConfig = t.object({
  ...baseShape,
  replication: t
    .object({
      ...baseShape.replication.props.shape,
      connections: t.array(mergedDataSourceConfig).optional()
    })
    .optional(),
  storage: mergedStorageConfig.optional()
});
export type MergedServiceConfig = t.Encoded<typeof MergedServiceConfig>;
export type MergedServiceConfigDecoded = t.Decoded<typeof MergedServiceConfig>;
