import * as lib_mongo from '@powersync/lib-service-mongodb';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

const S3ObjectStorageConfig = t.object({
  type: t.literal('s3'),
  bucket: t.string,
  region: t.string,
  prefix: t.string.optional(),
  endpoint: t.string.optional(),
  // Chunks whose BSON-serialized size falls below this byte threshold
  // stay inline in MongoDB instead of being offloaded to S3. Default 256.
  inline_threshold_bytes: t.number.optional()
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
