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

export const MongoStorageReadPreference = t
  .literal('primary')
  .or(t.literal('primaryPreferred'))
  .or(t.literal('secondary'))
  .or(t.literal('secondaryPreferred'))
  .or(t.literal('nearest'));

export type MongoStorageReadPreference = t.Encoded<typeof MongoStorageReadPreference>;

export const MongoStorageConfig = lib_mongo.BaseMongoConfig.and(
  t.object({
    /**
     * Read preference for bulk checksum and bucket data reads.
     *
     * If unset, MongoDB driver defaults are used for backwards compatibility.
     *
     * This is an experimental option, and may be removed in a future release.
     */
    bulk_read_preference: MongoStorageReadPreference.optional(),

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
