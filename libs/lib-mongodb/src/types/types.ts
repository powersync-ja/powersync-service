import { ErrorCode, LookupOptions, makeHostnameLookupFunction, ServiceError } from '@powersync/lib-services-framework';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

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
export function normalizeMongoConfig(options: BaseMongoConfigDecoded) {
  let uri = urijs.parse(options.uri);

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const userInfo = uri.userinfo?.split(':');

  const username = options.username ?? userInfo?.[0];
  const password = options.password ?? userInfo?.[1];

  if (database == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1105, `MongoDB connection: database required`);
  }

  delete uri.userinfo;

  const lookupOptions: LookupOptions = {
    reject_ip_ranges: options.reject_ip_ranges ?? []
  };
  const lookup = makeHostnameLookupFunction(uri.host ?? '', lookupOptions);

  return {
    uri: urijs.serialize(uri),
    database,

    username,
    password,

    lookup
  };
}
