import {
  ErrorCode,
  LookupOptions,
  makeMultiHostnameLookupFunction,
  ServiceError
} from '@powersync/lib-services-framework';
import * as t from 'ts-codec';
import ConnectionURI from 'mongodb-connection-string-url';
import { LookupFunction } from 'node:net';

export const MONGO_CONNECTION_TYPE = 'mongodb' as const;

export const BaseMongoConfig = t.object({
  type: t.literal(MONGO_CONNECTION_TYPE),
  uri: t.string,
  database: t.string.optional(),
  username: t.string.optional(),
  password: t.string.optional(),

  reject_ip_ranges: t.array(t.string).optional(),

  connectTimeoutMS: t.number.optional(),
  socketTimeoutMS: t.number.optional(),
  serverSelectionTimeoutMS: t.number.optional(),
  maxPoolSize: t.number.optional(),
  maxIdleTimeMS: t.number.optional()
});

export type BaseMongoConfig = t.Encoded<typeof BaseMongoConfig>;
export type BaseMongoConfigDecoded = t.Decoded<typeof BaseMongoConfig>;

export type NormalizedMongoConfig = {
  uri: string;
  database: string;
  username: string;
  password: string;
  lookup: LookupFunction | undefined;
  connectTimeoutMS?: number;
  socketTimeoutMS?: number;
  serverSelectionTimeoutMS?: number;
  maxPoolSize?: number;
  maxIdleTimeMS?: number;
};

/**
 * Construct a mongodb URI, without username, password or ssl options.
 *
 * Only contains hostname, port, database.
 */
export function baseUri(options: BaseMongoConfig) {
  return options.uri;
}

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 *
 * For use by both storage and mongo module.
 */
export function normalizeMongoConfig(options: BaseMongoConfigDecoded): NormalizedMongoConfig {
  let uri: ConnectionURI;

  try {
    uri = new ConnectionURI(options.uri);
  } catch (error) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1109,
      `MongoDB connection: invalid URI ${error instanceof Error ? `- ${error.message}` : ''}`
    );
  }

  const database = options.database ?? uri.pathname.split('/')[1] ?? '';
  const username = options.username ?? uri.username;
  const password = options.password ?? uri.password;

  uri.password = '';
  uri.username = '';

  if (database == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1105, `MongoDB connection: database required`);
  }

  const parseQueryParam = (key: string): number | undefined => {
    const value = uri.searchParams.get(key);
    if (value == null) return undefined;
    const num = Number(value);
    if (isNaN(num) || num < 0) return undefined;
    return num;
  };
  const connectTimeoutMS = options.connectTimeoutMS ?? parseQueryParam('connectTimeoutMS');
  const socketTimeoutMS = options.socketTimeoutMS ?? parseQueryParam('socketTimeoutMS');
  const serverSelectionTimeoutMS = options.serverSelectionTimeoutMS ?? parseQueryParam('serverSelectionTimeoutMS');
  const maxPoolSize = options.maxPoolSize ?? parseQueryParam('maxPoolSize');
  const maxIdleTimeMS = options.maxIdleTimeMS ?? parseQueryParam('maxIdleTimeMS');

  const lookupOptions: LookupOptions = {
    reject_ip_ranges: options.reject_ip_ranges ?? []
  };
  const lookup = makeMultiHostnameLookupFunction(uri.hosts, lookupOptions);

  return {
    uri: uri.toString(),
    database,

    username,
    password,

    lookup,

    connectTimeoutMS,
    socketTimeoutMS,
    serverSelectionTimeoutMS,
    maxPoolSize,
    maxIdleTimeMS
  };
}
