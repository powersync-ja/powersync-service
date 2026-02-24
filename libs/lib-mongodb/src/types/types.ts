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

  reject_ip_ranges: t.array(t.string).optional()
});

export type BaseMongoConfig = t.Encoded<typeof BaseMongoConfig>;
export type BaseMongoConfigDecoded = t.Decoded<typeof BaseMongoConfig>;

/**
 * Connection parameters that can be parsed from the MongoDB URI query string.
 *
 * All timeout values are in milliseconds (MongoDB convention).
 * maxPoolSize is a count, not a time value.
 */
export interface MongoConnectionParams {
  connectTimeoutMS?: number;
  socketTimeoutMS?: number;
  serverSelectionTimeoutMS?: number;
  maxPoolSize?: number;
  maxIdleTimeMS?: number;
}

export type NormalizedMongoConfig = {
  uri: string;
  database: string;
  username: string;
  password: string;
  lookup: LookupFunction | undefined;
  connectionParams: MongoConnectionParams;
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
 * Parse a single numeric connection parameter from a URI query string value.
 *
 * Returns undefined if the value is missing, not a valid number, NaN, negative, zero, or Infinity.
 * Values are already in the correct unit (ms for timeouts, count for maxPoolSize).
 */
export function parseMongoConnectionParam(value: string | null | undefined): number | undefined {
  if (value == null) {
    return undefined;
  }
  const parsed = Number(value);
  if (isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return undefined;
}

/**
 * Parse connection parameters from a MongoDB URI's query string.
 *
 * All values are in milliseconds (MongoDB convention), except maxPoolSize which is a count.
 * Invalid values (NaN, negative, non-numeric) are silently ignored.
 */
export function parseMongoConnectionParams(searchParams: URLSearchParams): MongoConnectionParams {
  const params: MongoConnectionParams = {};

  const connectTimeoutMS = parseMongoConnectionParam(searchParams.get('connectTimeoutMS'));
  if (connectTimeoutMS != null) {
    params.connectTimeoutMS = connectTimeoutMS;
  }

  const socketTimeoutMS = parseMongoConnectionParam(searchParams.get('socketTimeoutMS'));
  if (socketTimeoutMS != null) {
    params.socketTimeoutMS = socketTimeoutMS;
  }

  const serverSelectionTimeoutMS = parseMongoConnectionParam(searchParams.get('serverSelectionTimeoutMS'));
  if (serverSelectionTimeoutMS != null) {
    params.serverSelectionTimeoutMS = serverSelectionTimeoutMS;
  }

  const maxPoolSize = parseMongoConnectionParam(searchParams.get('maxPoolSize'));
  if (maxPoolSize != null) {
    params.maxPoolSize = maxPoolSize;
  }

  const maxIdleTimeMS = parseMongoConnectionParam(searchParams.get('maxIdleTimeMS'));
  if (maxIdleTimeMS != null) {
    params.maxIdleTimeMS = maxIdleTimeMS;
  }

  return params;
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

  const lookupOptions: LookupOptions = {
    reject_ip_ranges: options.reject_ip_ranges ?? []
  };
  const lookup = makeMultiHostnameLookupFunction(uri.hosts, lookupOptions);

  // Parse connection parameters from URL query string
  const connectionParams = parseMongoConnectionParams(uri.searchParams);

  return {
    uri: uri.toString(),
    database,

    username,
    password,

    lookup,
    connectionParams
  };
}
