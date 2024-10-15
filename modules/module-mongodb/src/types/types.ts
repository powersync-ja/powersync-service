import { normalizeMongoConfig } from '@powersync/service-core';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

export const MONGO_CONNECTION_TYPE = 'mongodb' as const;

export interface NormalizedMongoConnectionConfig {
  id: string;
  tag: string;

  uri: string;
  database: string;

  username?: string;
  password?: string;
}

export const MongoConnectionConfig = service_types.configFile.dataSourceConfig.and(
  t.object({
    type: t.literal(MONGO_CONNECTION_TYPE),
    /** Unique identifier for the connection - optional when a single connection is present. */
    id: t.string.optional(),
    /** Tag used as reference in sync rules. Defaults to "default". Does not have to be unique. */
    tag: t.string.optional(),
    uri: t.string,
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional()
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
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    ...base
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
