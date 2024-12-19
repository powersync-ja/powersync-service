import * as urijs from 'uri-js';

export interface MongoConnectionConfig {
  uri: string;
  username?: string;
  password?: string;
  database?: string;
}

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 *
 * For use by both storage and mongo module.
 */
export function normalizeMongoConfig(options: MongoConnectionConfig) {
  let uri = urijs.parse(options.uri);

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const userInfo = uri.userinfo?.split(':');

  const username = options.username ?? userInfo?.[0];
  const password = options.password ?? userInfo?.[1];

  if (database == '') {
    throw new Error(`database required`);
  }

  delete uri.userinfo;

  return {
    uri: urijs.serialize(uri),
    database,

    username,
    password
  };
}
