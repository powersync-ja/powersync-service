import * as lib_mongo from '@powersync/lib-service-mongodb/types';
import * as service_types from '@powersync/service-types';
import { LookupFunction } from 'node:net';
import * as t from 'ts-codec';

export enum PostImagesOption {
  /**
   * Use fullDocument: updateLookup on the changeStream.
   *
   * This does not guarantee consistency - operations may
   * arrive out of order, especially when there is replication lag.
   *
   * This is the default option for backwards-compatability.
   */
  OFF = 'off',

  /**
   * Use fullDocument: required on the changeStream.
   *
   * Collections are automatically configured with:
   * `changeStreamPreAndPostImages: { enabled: true }`
   *
   * This is the recommended behavior for new instances.
   */
  AUTO_CONFIGURE = 'auto_configure',

  /**
   *
   * Use fullDocument: required on the changeStream.
   *
   * Collections are not automatically configured. Each
   * collection must be configured configured manually before
   * replicating with:
   *
   * `changeStreamPreAndPostImages: { enabled: true }`
   *
   * Use when the collMod permission is not available.
   */
  READ_ONLY = 'read_only'
}

export interface NormalizedMongoConnectionConfig {
  id: string;
  tag: string;

  uri: string;
  database: string;

  username?: string;
  password?: string;

  lookup?: LookupFunction;

  postImages: PostImagesOption;
}

export const MongoConnectionConfig = service_types.configFile.DataSourceConfig.and(lib_mongo.BaseMongoConfig).and(
  t.object({
    // Replication specific settings
    post_images: t.literal('off').or(t.literal('auto_configure')).or(t.literal('read_only')).optional()
  })
);

/**
 * Config input specified when starting services
 */
export type MongoConnectionConfig = t.Encoded<typeof MongoConnectionConfig>;
export type MongoConnectionConfigDecoded = t.Decoded<typeof MongoConnectionConfig>;

/**
 * Resolved version of {@link MongoConnectionConfig}
 */
export type ResolvedConnectionConfig = MongoConnectionConfigDecoded & NormalizedMongoConnectionConfig;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: MongoConnectionConfigDecoded): NormalizedMongoConnectionConfig {
  const base = lib_mongo.normalizeMongoConfig(options);

  return {
    ...base,
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',
    postImages: (options.post_images as PostImagesOption | undefined) ?? PostImagesOption.OFF
  };
}
