import { logger } from '@powersync/lib-services-framework';
import mysql from 'mysql2';
import mysqlPromise from 'mysql2/promise';
import * as types from '../types/types.js';

export const MySQLTypesMap: { [key: number]: string } = {};
for (const [name, code] of Object.entries(mysql.Types)) {
  MySQLTypesMap[code as number] = name;
}

export type RetriedQueryOptions = {
  connection: mysqlPromise.Connection;
  query: string;
  params?: any[];
  retries?: number;
};

/**
 * Retry a simple query - up to 2 attempts total.
 */
export async function retriedQuery(options: RetriedQueryOptions) {
  const { connection, query, params = [], retries = 2 } = options;
  for (let tries = retries; ; tries--) {
    try {
      logger.debug(`Executing query: ${query}`);
      return connection.query<mysqlPromise.RowDataPacket[]>(query, params);
    } catch (e) {
      if (tries == 1) {
        throw e;
      }
      logger.warn('Query error, retrying', e);
    }
  }
}

export function createPool(config: types.NormalizedMySQLConnectionConfig, options?: mysql.PoolOptions): mysql.Pool {
  const sslOptions = {
    ca: config.cacert,
    key: config.client_private_key,
    cert: config.client_certificate
  };
  const hasSSLOptions = Object.values(sslOptions).some((v) => !!v);
  return mysql.createPool({
    host: config.hostname,
    user: config.username,
    password: config.password,
    database: config.database,
    ssl: hasSSLOptions ? sslOptions : undefined,
    supportBigNumbers: true,
    timezone: 'Z', // Ensure no auto timezone manipulation of the dates occur
    ...(options || {})
  });
}
