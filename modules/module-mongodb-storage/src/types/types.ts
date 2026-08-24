import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

/**
 * AWS defaults mode, as used by the AWS SDK.
 *
 * This is the baseline for the object storage request timeouts, so that they do not each need to be
 * configured individually.
 *
 * The SDK's `legacy` and `auto` modes are not selectable here: `legacy` defines no timeouts at all,
 * and `auto` makes the timeouts depend on where the process happens to be running. Both are still
 * handled when they come from the AWS environment, and are then treated as `standard`.
 */
export const S3DefaultsMode = t
  .literal('standard')
  .or(t.literal('in-region'))
  .or(t.literal('cross-region'))
  .or(t.literal('mobile'));

export type S3DefaultsMode = t.Encoded<typeof S3DefaultsMode>;

const S3ObjectStorageConfig = t.object({
  type: t.literal('s3'),
  bucket: t.string,
  region: t.string.optional(),
  prefix: t.string.optional(),
  endpoint: t.string.optional(),
  force_path_style: t.boolean.optional(),
  access_key_id: t.string.optional(),
  secret_access_key: t.string.optional(),
  concurrency_limit: t.number.optional(),
  // Defaults to the AWS_DEFAULTS_MODE environment variable, or defaults_mode in the AWS shared
  // config file.
  defaults_mode: S3DefaultsMode.optional(),
  // Chunks whose BSON-serialized size falls below this byte threshold
  // stay inline in MongoDB instead of being offloaded to S3. Default 1024.
  inline_threshold_bytes: t.number.optional()
});

export const MongoStorageReadPreference = t
  .literal('primary')
  .or(t.literal('primaryPreferred'))
  .or(t.literal('secondary'))
  .or(t.literal('secondaryPreferred'))
  .or(t.literal('nearest'));

export type MongoStorageReadPreference = t.Encoded<typeof MongoStorageReadPreference>;

export const MongoStorageConfig = service_types.configFile.BaseStorageConfig.and(lib_mongo.BaseMongoConfig).and(
  t.object({
    /**
     * Read preference for bulk checksum and bucket data reads.
     *
     * If unset, MongoDB driver defaults are used for backwards compatibility.
     *
     * This is an experimental option, and may be removed in a future release.
     */
    bulk_read_preference: MongoStorageReadPreference.optional(),
    /**
     * Throttle clear operations after sync config deploys, by pausing between batches.
     * The rate produces a pause proportional to the previous batch duration.
     *
     * Increase this to reduce impact of clear operations on the storage cluster.
     * Use 0 to clear as fast as possible.
     *
     * Defaults to 0.2.
     */
    clear_batch_throttle_rate: t.number.optional(),

    object_storage: S3ObjectStorageConfig.optional()
  })
);

export type MongoStorageConfig = t.Encoded<typeof MongoStorageConfig>;
export type MongoStorageConfigDecoded = t.Decoded<typeof MongoStorageConfig>;

export const DEFAULT_CLEAR_BATCH_THROTTLE_RATE = 0.2;

export function normalizeClearBatchThrottleRate(value: number | undefined): number {
  const rate = value ?? DEFAULT_CLEAR_BATCH_THROTTLE_RATE;
  if (!Number.isFinite(rate) || rate < 0 || rate > 20) {
    throw new ServiceError(
      ErrorCode.PSYNC_S3201,
      'storage.clear_batch_throttle_rate must be a finite number between 0 and 20'
    );
  }
  return rate;
}

export function isMongoStorageConfig(
  config: service_types.configFile.GenericStorageConfig
): config is MongoStorageConfig {
  return config.type == lib_mongo.MONGO_CONNECTION_TYPE;
}
