import { MongoStorageConfig } from '@powersync/service-module-mongodb-storage/types';
import { MongoConnectionConfig } from '@powersync/service-module-mongodb/types';
import { ConvexConnectionConfig } from '@powersync/service-module-convex/types';
import { MySQLConnectionConfig } from '@powersync/service-module-mysql/types';
import { PostgresStorageConfig } from '@powersync/service-module-postgres-storage/types';
import { PostgresConnectionConfig } from '@powersync/service-module-postgres/types';
import { configFile } from '@powersync/service-types';
import fs from 'fs';
import path from 'path';
import * as t from 'ts-codec';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(__dirname, '../../json-schema');

fs.mkdirSync(schemaDir, { recursive: true });

// Merge configs for modules
const baseShape = configFile.powerSyncConfig.props.shape;

const mergedDataSourceConfig = configFile.genericDataSourceConfig
  .or(PostgresConnectionConfig)
  .or(MongoConnectionConfig)
  .or(MySQLConnectionConfig)
  .or(ConvexConnectionConfig);

const mergedStorageConfig = configFile.GenericStorageConfig.or(PostgresStorageConfig).or(MongoStorageConfig);

const mergedConfig = t.object({
  ...baseShape,
  replication: t
    .object({
      ...baseShape.replication.props.shape,
      connections: t.array(mergedDataSourceConfig).optional()
    })
    .optional(),
  storage: mergedStorageConfig.optional()
});

const mergedConfigSchema = t.generateJSONSchema(mergedConfig, {
  allowAdditional: true,
  parsers: [configFile.portParser]
});

fs.writeFileSync(path.join(schemaDir, 'powersync-config.json'), JSON.stringify(mergedConfigSchema, null, '\t'));
