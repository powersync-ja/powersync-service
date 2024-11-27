import { normalizeMongoConfig } from '@powersync/service-core';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

export const MONGO_CONNECTION_TYPE = 'mongodb' as const;

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

  postImages: PostImagesOption;
}

export const MongoConnectionConfig = service_types.configFile.DataSourceConfig.and(
  t.object({
    type: t.literal(MONGO_CONNECTION_TYPE),
    /** Unique identifier for the connection - optional when a single connection is present. */
    id: t.string.optional(),
    /** Tag used as reference in sync rules. Defaults to "default". Does not have to be unique. */
    tag: t.string.optional(),
    uri: t.string,
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional(),

    post_images: t.literal('off').or(t.literal('auto_configure')).or(t.literal('read_only')).optional()
  })
);

/**
 * Config input specified when starting services
 */
export type MongoConnectionConfig = t.Decoded<typeof MongoConnectionConfig>;

/**
 * Resolved version of {@link MongoConnectionConfig}
 */
export type ResolvedConnectionConfig = MongoConnectionConfig & NormalizedMongoConnectionConfig;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: MongoConnectionConfig): NormalizedMongoConnectionConfig {
  const base = normalizeMongoConfig(options);

  return {
    ...base,
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',
    postImages: (options.post_images as PostImagesOption | undefined) ?? PostImagesOption.OFF
  };
}

/**
 * Construct a mongodb URI, without username, password or ssl options.
 *
 * Only contains hostname, port, database.
 */
export function baseUri(options: NormalizedMongoConnectionConfig) {
  return options.uri;
}
