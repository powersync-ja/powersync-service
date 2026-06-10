import * as lib_mongo from '@powersync/lib-service-mongodb';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

const S3ObjectStorageConfig = t.object({
  type: t.literal('s3'),
  bucket: t.string,
  region: t.string,
  prefix: t.string.optional(),
  endpoint: t.string.optional()
});

export const MongoStorageConfig = lib_mongo.BaseMongoConfig.and(
  t.object({
    // Add any mongo specific storage settings here in future
    object_storage: S3ObjectStorageConfig.optional()
  })
);

export type MongoStorageConfig = t.Encoded<typeof MongoStorageConfig>;
export type MongoStorageConfigDecoded = t.Decoded<typeof MongoStorageConfig>;

export function isMongoStorageConfig(
  config: service_types.configFile.GenericStorageConfig
): config is MongoStorageConfig {
  return config.type == lib_mongo.MONGO_CONNECTION_TYPE;
}
